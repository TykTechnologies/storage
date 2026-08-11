package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/TykTechnologies/storage/persistent/model"
	"gorm.io/gorm"
)

// Query retrieves records from the database matching the given filter into result.
// Returns an error if the query fails or the result cannot be populated.
func (d *driver) Query(ctx context.Context, object model.DBObject, result interface{}, filter model.DBM) error {
	tableName, err := d.validateDBAndTable(object)
	if err != nil {
		return err
	}

	// Validate result parameter
	resultVal := reflect.ValueOf(result)
	if resultVal.Kind() != reflect.Ptr {
		return errors.New("result must be a pointer")
	}

	db := d.db.WithContext(ctx).Table(tableName)

	db, err = d.translateQuery(db, filter, object)
	if err != nil {
		return err
	}

	resultElem := resultVal.Elem()
	isSingle := resultElem.Kind() != reflect.Slice

	if isSingle {
		// For a single object, use First
		err := db.First(result).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return sql.ErrNoRows
			}

			return err
		}
	} else {
		// For a slice, use Find
		err := db.Find(result).Error
		if err != nil {
			return err
		}

		// Check if any records were found
		if resultElem.Len() == 0 {
			return sql.ErrNoRows
		}
	}

	return nil
}

// Count returns the number of records in the table matching the provided filters.
// Returns an error if the count operation fails.
func (d *driver) Count(ctx context.Context, row model.DBObject, filters ...model.DBM) (count int, error error) {
	tableName, err := d.validateDBAndTable(row)
	if err != nil {
		return 0, err
	}

	tableExist, err := d.HasTable(ctx, row.TableName())
	if !tableExist || err != nil {
		return 0, ErrorCollectionNotFound
	}

	db := d.db.WithContext(ctx).Table(tableName)

	// If we have a filter, use our translator function
	if len(filters) == 1 {
		countFilter := make(model.DBM)
		countFilter["_count"] = true

		for k, v := range filters[0] {
			countFilter[k] = v
		}

		db, err = d.translateQuery(db, countFilter, row)
		if err != nil {
			return 0, err
		}
	}

	var result int64

	err = db.Count(&result).Error
	if err != nil {
		return 0, err
	}

	return int(result), nil
}

// Aggregate executes an aggregation pipeline on the specified table.
// Returns the resulting documents or an error if the operation fails.
func (d *driver) Aggregate(ctx context.Context, row model.DBObject, pipeline []model.DBM) ([]model.DBM, error) {
	tableName, err := d.validateDBAndTable(row)
	if err != nil {
		return []model.DBM{}, err
	}

	// Check if pipeline is empty
	if len(pipeline) == 0 {
		return nil, errors.New("empty aggregation pipeline")
	}

	sqlQuery, args, err := translateAggregationPipeline(tableName, pipeline)
	if err != nil {
		return nil, fmt.Errorf("failed to translate aggregation pipeline: %w", err)
	}

	// Execute the query using GORM
	rows, err := d.db.WithContext(ctx).Raw(sqlQuery, args...).Rows()
	if err != nil {
		return nil, fmt.Errorf("failed to execute aggregation query: %w", err)
	}

	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	results := []model.DBM{}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}

		// Scan the row into the values
		err := rows.Scan(values...)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		rowMap := model.DBM{}

		// Set values in the map
		for i, col := range columns {
			val := *(values[i].(*interface{}))
			rowMap[col] = val
		}

		results = append(results, rowMap)
	}

	// Check for errors during iteration
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return results, nil
}

// applyMongoUpdateOperators applies MongoDB-style update operators to a GORM DB instance
func (d *driver) applyMongoUpdateOperators(db *gorm.DB, update model.DBM) (*gorm.DB, map[string]interface{}, error) {
	if db == nil {
		return nil, nil, ErrorSessionClosed
	}

	result := db
	updateMap := map[string]interface{}{}

	// Process MongoDB update operators
	for operator, fields := range update {
		switch operator {
		case "$set":
			// $set operator: directly set field values
			if setMap, ok := fields.(model.DBM); ok && len(setMap) > 0 {
				// Handle model.DBM type which is common in the codebase
				for field, value := range setMap {
					updateMap[field] = value
				}
			}

		case "$inc":
			// $inc operator: increment field values
			if incMap, ok := fields.(model.DBM); ok {
				for field, value := range incMap {
					updateMap[field] = gorm.Expr(fmt.Sprintf("%s + ?", field), value)
				}
			}

		case "$mul":
			// $mul operator: multiply field values
			if mulMap, ok := fields.(model.DBM); ok {
				for field, value := range mulMap {
					updateMap[field] = gorm.Expr(fmt.Sprintf("%s * ?", field), value)
				}
			}

		case "$unset":
			// $unset operator: set fields to NULL
			if unsetMap, ok := fields.(model.DBM); ok {
				for field := range unsetMap {
					updateMap[field] = nil
				}
			}

		case "$min":
			// $min operator: update field if new value is less than current value
			if minMap, ok := fields.(model.DBM); ok {
				for field, value := range minMap {
					updateMap[field] = gorm.Expr(fmt.Sprintf("CASE WHEN ? < %s THEN ? ELSE %s END", field, field), value, value)
				}
			}

		case "$max":
			// $max operator: update field if new value is greater than current value
			if maxMap, ok := fields.(model.DBM); ok {
				for field, value := range maxMap {
					updateMap[field] = gorm.Expr(fmt.Sprintf("CASE WHEN ? > %s THEN ? ELSE %s END", field, field), value, value)
				}
			}

		case "$currentDate":
			// $currentDate operator: set fields to current date/time
			if dateMap, ok := fields.(model.DBM); ok {
				for field := range dateMap {
					updateMap[field] = gorm.Expr("CURRENT_TIMESTAMP")
				}
			}

		default:
			// If not an operator, treat as a direct field update
			if !strings.HasPrefix(operator, "$") {
				updateMap[operator] = fields
			} else {
				return nil, nil, errors.New("unsupported operator: " + operator)
			}
		}
	}

	return result, updateMap, nil
}

// translateQuery converts MongoDB-style queries to GORM queries with sharding support
func (d *driver) translateQuery(db *gorm.DB, q model.DBM, result interface{}) (*gorm.DB, error) {
	if db == nil {
		return nil, ErrorSessionClosed
	}

	where := map[string]interface{}{}
	order := ""

	shardField, useSharding := q["_date_sharding"].(string)
	var minShardDate, maxShardDate time.Time

	tableSharding := d.options != nil && d.TableSharding

	if !tableSharding {
		useSharding = false
	}

	for k, v := range q {
		if k == "_collection" || k == "_date_sharding" || k == "_limit" || k == "_offset" || k == "_count" || k == "_sort" {
			continue
		}

		if s, ok := v.(string); ok && s == "" {
			continue
		}

		if o, ok := v.(model.ObjectID); ok {
			where[k] = o.Hex()
			continue
		}

		if k == "$or" {
			if nested, ok := v.([]model.DBM); ok {
				for i, n := range nested {
					sub := db.Session(&gorm.Session{NewDB: true})

					for nk, nv := range n {
						// Apply the same dot-to-underscore conversion as the non-$or path
						// (line below the $or block) so nested field names like "user.name"
						// remain valid before sanitization.
						col, colErr := sanitizeIdentifier(strings.ReplaceAll(nk, ".", "_"))
						if colErr != nil {
							return nil, fmt.Errorf("invalid field in $or: %w", colErr)
						}

						val := ""
						if o, ok := nv.(model.ObjectID); ok {
							val = o.Hex()
						} else {
							val = fmt.Sprint(nv)
						}

						sub = sub.Where(col+" = ?", val)
					}

					if i == 0 {
						db = db.Where(sub)
					} else {
						db = db.Or(sub)
					}
				}
			}

			continue
		}

		// Handle nested operators
		if nested, ok := v.(model.DBM); ok {
			for nk, nv := range nested {
				switch nk {
				case "$ne":
					// MongoDB $ne also matches documents where the field is
					// absent/null; mirror that so NULL rows are included, unlike a
					// bare NOT (col = ?) which excludes them.
					db = db.Where(fmt.Sprintf("(%v IS NULL OR %v <> ?)", k, k), nv)
				case "$regex":
					if pattern, ok := nv.(string); ok && pattern != "" {
						matchOp := "~"
						if opts, ok := nested["$options"].(string); ok && strings.Contains(opts, "i") {
							matchOp = "~*"
						}

						db = db.Where(fmt.Sprintf("%v %s ?", k, matchOp), pattern)
					}
				case "$options":
					// Consumed together with $regex above; no standalone clause.
				case "$exists":
					if exists, ok := nv.(bool); ok {
						if exists {
							db = db.Where(fmt.Sprintf("%v IS NOT NULL", k))
						} else {
							db = db.Where(fmt.Sprintf("%v IS NULL", k))
						}
					}
				case "$gt":
					db = db.Where(fmt.Sprintf("%v > ?", k), nv)

					if useSharding && k == shardField {
						minShardDate = nv.(time.Time)
					}

				case "$gte":
					db = db.Where(fmt.Sprintf("%v >= ?", k), nv)

					if useSharding && k == shardField {
						minShardDate = nv.(time.Time)
					}

				case "$lt":
					db = db.Where(fmt.Sprintf("%v < ?", k), nv)

					if useSharding && k == shardField {
						maxShardDate = nv.(time.Time)
					}

				case "$lte":
					db = db.Where(fmt.Sprintf("%v <= ?", k), nv)

					if useSharding && k == shardField {
						maxShardDate = nv.(time.Time)
					}

				case "$in":
					inArr := []string{}
					s := reflect.ValueOf(nv)

					for i := 0; i < s.Len(); i++ {
						current := s.Index(i).Interface()
						if def, ok := current.(model.ObjectID); ok {
							inArr = append(inArr, def.Hex())
						} else {
							inArr = append(inArr, fmt.Sprint(current))
						}
					}

					db = db.Where(fmt.Sprintf("%v IN ?", k), inArr)

				case "$i":
					if nv.(string) != "" {
						db = db.Where(fmt.Sprintf("LOWER(%v) = ?", k), strings.ToLower(nv.(string)))
					}

				case "$text":
					if nv.(string) != "" {
						db = db.Where(fmt.Sprintf("LOWER(%s) like ?", k), "%"+strings.ToLower(nv.(string))+"%")
					}
				}
			}

			continue
		}

		// Convert nested keys from mongo notation
		where[strings.ReplaceAll(k, ".", "_")] = v
	}

	db = db.Where(where)

	_, counter := q["_count"].(bool)
	if counter {
		db = db.Select("count(1) as cnt")
	}

	if useSharding {
		if minShardDate.IsZero() || maxShardDate.IsZero() {
			// Sharding requires both gte and lte date dimensions
			return nil, errors.New("date sharding requires both gte and lte date dimensions")
		}

		baseTable := ""
		if obj, ok := result.(model.DBObject); ok {
			baseTable = obj.TableName()
		} else {
			// Try to get collection name from the result type
			baseTable, _ = getCollectionName(result)
		}

		if baseTable != "" {
			tablePattern := baseTable + "_%"

			// Query to get all tables matching the pattern
			query := `
				SELECT tablename 
				FROM pg_tables 
				WHERE schemaname = 'public' 
				AND tablename LIKE ?
        	`

			rows, err := d.db.Raw(query, tablePattern).Rows()
			if err != nil {
				return nil, fmt.Errorf("failed to get sharded tables: %w", err)
			}

			defer rows.Close()

			var matchingTables []string

			for rows.Next() {
				var tableName string
				if err := rows.Scan(&tableName); err != nil {
					return nil, fmt.Errorf("failed to scan table name: %w", err)
				}

				matchingTables = append(matchingTables, tableName)
			}

			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("error iterating table names: %w", err)
			}

			allTablesSQL := []string{}
			dateFormat := "20060102"
			minDateStr := minShardDate.Format(dateFormat)
			maxDateStr := maxShardDate.Format(dateFormat)

			for _, tableName := range matchingTables {
				// Extract date suffix from table name
				if len(tableName) <= len(baseTable)+1 {
					continue // Skip if table name is too short
				}

				dateSuffix := tableName[len(baseTable)+1:] // +1 for the underscore

				// Validate that the suffix is a date in the expected format
				if len(dateSuffix) != 8 {
					continue // Not a date suffix
				}

				// Check if the date is within our range
				if dateSuffix >= minDateStr && dateSuffix <= maxDateStr {
					allTablesSQL = append(allTablesSQL, "SELECT * FROM "+tableName)
				}
			}

			if len(allTablesSQL) > 0 {
				fromSQL := strings.Join(allTablesSQL, " UNION ALL ")
				fromSQL = "(" + fromSQL + ") AS base"

				db = db.Table(fromSQL)
			}
		}
	}

	// Handle pagination
	if limit, limitFound := q["_limit"].(int); limitFound && limit > 0 {
		db = db.Limit(limit)
	}

	if offset, offsetFound := q["_offset"].(int); offsetFound && offset > 0 {
		db = db.Offset(offset)
	}

	// Handle sorting
	if sort, sortFound := q["_sort"].(string); sortFound && sort != "" {
		if strings.HasPrefix(sort, "-") {
			order = strings.TrimPrefix(sort, "-") + " desc"
		} else {
			order = sort
		}

		db = db.Order(order)
	}

	return db, nil
}

func translateAggregationPipeline(tableName string, pipeline []model.DBM) (string, []interface{}, error) {
	// Initialize SQL parts
	selectClause := "*"
	fromClause := tableName
	var whereClause string
	var groupByClause string
	var havingClause string
	var orderByClause string
	var limitClause string
	var offsetClause string
	var args []interface{}
	argIndex := 1

	for _, stage := range pipeline {
		if len(stage) != 1 {
			return "", nil, errors.New("each pipeline stage must have exactly one operator")
		}

		var operator string
		var value interface{}

		for k, v := range stage {
			operator = k
			value = v
		}

		switch operator {
		case "$match":
			if matchExpr, ok := value.(model.DBM); ok {
				matchWhere, matchArgs := buildWhereClause(matchExpr)
				if matchWhere != "" {
					if whereClause == "" {
						whereClause = matchWhere
					} else {
						whereClause = whereClause + " AND (" + matchWhere + ")"
					}

					args = append(args, matchArgs...)
					argIndex += len(matchArgs)
				}
			} else {
				return "", nil, errors.New("$match value must be a DBM")
			}

		case "$group":
			if groupExpr, ok := value.(model.DBM); ok {
				if idExpr, ok := groupExpr["_id"]; ok {
					if idMap, ok := idExpr.(model.DBM); ok {
						groupFields := []string{}

						for _, expr := range idMap {
							if fieldName, ok := expr.(string); ok {
								fieldName = strings.TrimPrefix(fieldName, "$")
								groupFields = append(groupFields, fieldName)
							} else {
								return "", nil, errors.New("complex group expressions not supported")
							}
						}

						if len(groupFields) > 0 {
							groupByClause = strings.Join(groupFields, ", ")
						}
					} else if idExpr == nil {
						groupByClause = ""
					} else if fieldName, ok := idExpr.(string); ok {
						fieldName = strings.TrimPrefix(fieldName, "$")
						groupByClause = fieldName
					} else {
						return "", nil, errors.New("complex group expressions not supported")
					}
				}

				selectParts := []string{}

				// Add group by fields to select clause
				if groupByClause != "" {
					groupFields := strings.Split(groupByClause, ", ")
					selectParts = append(selectParts, groupFields...)
				}

				// Add aggregation functions to select clause
				for field, expr := range groupExpr {
					if field == "_id" {
						continue // Already processed
					}

					if exprMap, ok := expr.(model.DBM); ok {
						for funcName, funcArg := range exprMap {
							// Map MongoDB aggregation functions to SQL
							var sqlFunc string

							switch funcName {
							case "$sum":
								sqlFunc = "SUM"
							case "$avg":
								sqlFunc = "AVG"
							case "$min":
								sqlFunc = "MIN"
							case "$max":
								sqlFunc = "MAX"
							case "$count":
								sqlFunc = "COUNT"
							default:
								return "", nil, fmt.Errorf("unsupported aggregation function: %s", funcName)
							}

							// The accumulator argument may be the literal 1 (COUNT(*)), a
							// field reference ("$field"), a nested aggregation expression
							// such as {$cond: ...} (compiled to a CASE expression), or a
							// plain literal (bound as a parameter).
							argStr, exprArgs, err := aggAccumulatorArg(funcName, funcArg)
							if err != nil {
								return "", nil, err
							}

							args = append(args, exprArgs...)
							argIndex += len(exprArgs)

							selectParts = append(selectParts, fmt.Sprintf("%s(%s) AS %s", sqlFunc, argStr, field))
						}
					} else {
						return "", nil, fmt.Errorf("invalid aggregation expression for field %s", field)
					}
				}

				if len(selectParts) > 0 {
					selectClause = strings.Join(selectParts, ", ")
				}
			} else {
				return "", nil, errors.New("$group value must be a DBM")
			}

		case "$project":
			if projectExpr, ok := value.(model.DBM); ok {
				projectParts := []string{}

				for field, include := range projectExpr {
					if include == 1 || include == true {
						// Include the field as is
						projectParts = append(projectParts, field)
					} else if include == 0 || include == false {
						// Exclude the field (do nothing)
					} else if exprMap, ok := include.(model.DBM); ok {
						// Field has an expression
						for exprOp, exprVal := range exprMap {
							switch exprOp {
							case "$concat":
								// Concatenate strings
								if concatArray, ok := exprVal.([]interface{}); ok {
									concatParts := []string{}

									for _, part := range concatArray {
										if partStr, ok := part.(string); ok {
											if strings.HasPrefix(partStr, "$") {
												// Field reference
												concatParts = append(concatParts, strings.TrimPrefix(partStr, "$"))
											} else {
												// String literal
												concatParts = append(concatParts, fmt.Sprintf("'%s'", partStr))
											}
										} else {
											return "", nil, errors.New("$concat arguments must be strings")
										}
									}

									concatStmt := fmt.Sprintf("CONCAT(%s)", strings.Join(concatParts, ", "))
									projectParts = append(projectParts, concatStmt, field)
								} else {
									return "", nil, errors.New("$concat value must be an array")
								}
							default:
								return "", nil, fmt.Errorf("unsupported projection operator: %s", exprOp)
							}
						}
					} else {
						return "", nil, fmt.Errorf("invalid projection expression for field %s", field)
					}
				}

				if len(projectParts) > 0 {
					selectClause = strings.Join(projectParts, ", ")
				}
			} else {
				return "", nil, errors.New("$project value must be a DBM")
			}

		case "$sort":
			if sortExpr, ok := value.(model.DBM); ok {
				sortParts := []string{}

				for field, direction := range sortExpr {
					var dirStr string

					if dir, ok := direction.(int); ok {
						switch dir {
						case 1:
							dirStr = "ASC"
						case -1:
							dirStr = "DESC"
						default:
							return "", nil, fmt.Errorf("invalid sort direction for field %s: %d", field, dir)
						}

						sortParts = append(sortParts, fmt.Sprintf("%s %s", field, dirStr))
					} else {
						return "", nil, fmt.Errorf("sort direction for field %s must be an integer", field)
					}
				}

				if len(sortParts) > 0 {
					orderByClause = strings.Join(sortParts, ", ")
				}
			} else {
				return "", nil, errors.New("$sort value must be a DBM")
			}

		case "$limit":
			if limit, ok := value.(int); ok {
				limitClause = fmt.Sprintf("%d", limit)
			} else {
				return "", nil, errors.New("$limit value must be an integer")
			}

		case "$skip":
			if skip, ok := value.(int); ok {
				offsetClause = fmt.Sprintf("%d", skip)
			} else {
				return "", nil, errors.New("$skip value must be an integer")
			}

		case "$unwind":
			// $unwind flattens an array field into one row per element. In a
			// document store the counters live in arrays/sub-documents; the
			// equivalent SQL schema stores them as one row per element keyed by a
			// dimension column, so there is no single-table SQL rewrite for
			// $unwind without aligning the two schemas. Reject it explicitly
			// rather than silently producing an incorrect query.
			return "", nil, errors.New("$unwind is not supported by the Postgres aggregation translator: " +
				"it maps to a dimension/row-per-element schema that cannot be expressed as a single-table rewrite " +
				"(see docs/postgres-analytics-aggregation.md)")

		default:
			return "", nil, fmt.Errorf("unsupported aggregation operator: %s", operator)
		}
	}

	query := fmt.Sprintf("SELECT %s FROM %s", selectClause, fromClause)

	if whereClause != "" {
		query += fmt.Sprintf(" WHERE %s", whereClause)
	}

	if groupByClause != "" {
		query += fmt.Sprintf(" GROUP BY %s", groupByClause)
	}

	if havingClause != "" {
		query += fmt.Sprintf(" HAVING %s", havingClause)
	}

	if orderByClause != "" {
		query += fmt.Sprintf(" ORDER BY %s", orderByClause)
	}

	if limitClause != "" {
		query += fmt.Sprintf(" LIMIT %s", limitClause)
	}

	if offsetClause != "" {
		query += fmt.Sprintf(" OFFSET %s", offsetClause)
	}

	return query, args, nil
}

// aggFieldPattern validates a bare SQL identifier used inside an aggregation
// expression. Dots are converted to underscores before validation.
var aggFieldPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// sanitizeAggField validates a field identifier referenced from an aggregation
// expression, rejecting anything that is not a plain identifier so that field
// references can be interpolated into SQL without opening an injection vector.
func sanitizeAggField(name string) (string, error) {
	name = strings.ReplaceAll(name, ".", "_")
	if !aggFieldPattern.MatchString(name) {
		return "", fmt.Errorf("invalid field identifier in aggregation expression: %q", name)
	}

	return name, nil
}

// aggAccumulatorArg renders the argument of a $group accumulator ($sum, $avg,
// …) to a SQL expression. It supports the COUNT(*) shorthand ($sum: 1), field
// references, nested aggregation expressions (e.g. {$cond: …}), and plain
// literals (returned as a bound parameter).
func aggAccumulatorArg(funcName string, funcArg interface{}) (string, []interface{}, error) {
	if funcName == "$sum" {
		if n, ok := funcArg.(int); ok && n == 1 {
			return "*", nil, nil
		}
	}

	switch fa := funcArg.(type) {
	case string:
		col, err := sanitizeAggField(strings.TrimPrefix(fa, "$"))
		if err != nil {
			return "", nil, err
		}

		return col, nil, nil
	case model.DBM:
		expr, err := translateAggValueExpr(fa)
		if err != nil {
			return "", nil, err
		}

		return expr, nil, nil
	default:
		return "?", []interface{}{funcArg}, nil
	}
}

// translateAggValueExpr renders a MongoDB aggregation *value* expression to SQL.
// Field references ("$col") become column identifiers, operator maps recurse
// (currently {$cond: …}), and scalar literals are inlined (numbers/booleans) or
// single-quoted (strings). Numeric literals are inlined rather than bound so
// that expressions embedded in the SELECT list do not disturb positional
// parameter ordering relative to the WHERE clause.
func translateAggValueExpr(expr interface{}) (string, error) {
	switch e := expr.(type) {
	case string:
		if strings.HasPrefix(e, "$") {
			return sanitizeAggField(strings.TrimPrefix(e, "$"))
		}

		return "'" + strings.ReplaceAll(e, "'", "''") + "'", nil
	case bool:
		if e {
			return "TRUE", nil
		}

		return "FALSE", nil
	case int:
		return strconv.Itoa(e), nil
	case int32:
		return strconv.FormatInt(int64(e), 10), nil
	case int64:
		return strconv.FormatInt(e, 10), nil
	case float64:
		return strconv.FormatFloat(e, 'f', -1, 64), nil
	case model.DBM:
		if len(e) != 1 {
			return "", errors.New("aggregation value expression must have exactly one operator")
		}

		for op, v := range e {
			if op == "$cond" {
				return translateCondExpr(v)
			}

			return "", fmt.Errorf("unsupported aggregation value operator: %s", op)
		}
	}

	return "", fmt.Errorf("unsupported aggregation value expression: %T", expr)
}

// translateCondExpr compiles a MongoDB $cond into a SQL CASE expression. Both
// the object form ({if, then, else}) and the array form ([if, then, else]) are
// supported.
func translateCondExpr(v interface{}) (string, error) {
	var ifExpr, thenExpr, elseExpr interface{}

	switch c := v.(type) {
	case model.DBM:
		ifExpr, thenExpr, elseExpr = c["if"], c["then"], c["else"]
	case []interface{}:
		if len(c) != 3 {
			return "", errors.New("$cond array form must have exactly 3 elements")
		}

		ifExpr, thenExpr, elseExpr = c[0], c[1], c[2]
	default:
		return "", errors.New("$cond must be an object {if,then,else} or a 3-element array")
	}

	cond, err := translateAggBoolExpr(ifExpr)
	if err != nil {
		return "", err
	}

	thenSQL, err := translateAggValueExpr(thenExpr)
	if err != nil {
		return "", err
	}

	elseSQL, err := translateAggValueExpr(elseExpr)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("CASE WHEN %s THEN %s ELSE %s END", cond, thenSQL, elseSQL), nil
}

// translateAggBoolExpr compiles a MongoDB boolean/comparison aggregation
// expression ($and, $or, $eq, $ne, $gt, $gte, $lt, $lte) to a SQL predicate.
func translateAggBoolExpr(expr interface{}) (string, error) {
	m, ok := expr.(model.DBM)
	if !ok {
		return "", fmt.Errorf("boolean expression must be a DBM, got %T", expr)
	}

	if len(m) != 1 {
		return "", errors.New("boolean expression must have exactly one operator")
	}

	for op, v := range m {
		switch op {
		case "$and", "$or":
			arr, ok := v.([]interface{})
			if !ok {
				return "", fmt.Errorf("%s expects an array of expressions", op)
			}

			parts := make([]string, 0, len(arr))

			for _, sub := range arr {
				p, err := translateAggBoolExpr(sub)
				if err != nil {
					return "", err
				}

				parts = append(parts, p)
			}

			joiner := " OR "
			if op == "$and" {
				joiner = " AND "
			}

			return "(" + strings.Join(parts, joiner) + ")", nil
		case "$eq", "$ne", "$gt", "$gte", "$lt", "$lte":
			arr, ok := v.([]interface{})
			if !ok || len(arr) != 2 {
				return "", fmt.Errorf("%s expects a 2-element array", op)
			}

			left, err := translateAggValueExpr(arr[0])
			if err != nil {
				return "", err
			}

			right, err := translateAggValueExpr(arr[1])
			if err != nil {
				return "", err
			}

			return fmt.Sprintf("%s %s %s", left, aggComparator(op), right), nil
		default:
			return "", fmt.Errorf("unsupported boolean operator in aggregation: %s", op)
		}
	}

	return "", errors.New("empty boolean expression")
}

// aggComparator maps a MongoDB comparison operator to its SQL operator.
func aggComparator(op string) string {
	switch op {
	case "$eq":
		return "="
	case "$ne":
		return "<>"
	case "$gt":
		return ">"
	case "$gte":
		return ">="
	case "$lt":
		return "<"
	case "$lte":
		return "<="
	}

	return ""
}

func buildWhereClause(filter model.DBM) (string, []interface{}) {
	if len(filter) == 0 {
		return "", nil
	}

	var conditions []string
	var values []interface{}

	i := 1

	for k, v := range filter {
		// Skip special operators like $sort, $skip, $limit
		if k == "$sort" || k == "$skip" || k == "$limit" {
			continue
		}

		// Handle logical operators
		if k == "$or" || k == "$and" {
			// This would need specific implementation based on your needs
			continue
		}

		switch val := v.(type) {
		case model.DBM:
			for op, opVal := range val {
				switch op {
				case "$gt":
					conditions = append(conditions, fmt.Sprintf("%s > ?", k))
					values = append(values, opVal)
					i++

				case "$gte":
					conditions = append(conditions, fmt.Sprintf("%s >= ?", k))
					values = append(values, opVal)
					i++

				case "$lt":
					conditions = append(conditions, fmt.Sprintf("%s < ?", k))
					values = append(values, opVal)
					i++

				case "$lte":
					conditions = append(conditions, fmt.Sprintf("%s <= ?", k))
					values = append(values, opVal)
					i++

				case "$ne":
					conditions = append(conditions, fmt.Sprintf("%s <> ?", k))
					values = append(values, opVal)
					i++

				case "$in":
					inValues, ok := opVal.([]interface{})
					if !ok {
						// Handle error or try to convert
						continue
					}

					placeholders := make([]string, len(inValues))

					for j := range inValues {
						values = append(values, inValues[j])
						placeholders[j] = "?"
						i++
					}

					conditions = append(conditions, fmt.Sprintf("%s IN (%s)", k, strings.Join(placeholders, ",")))
				}
			}

		default:
			conditions = append(conditions, fmt.Sprintf("%s = ?", k))
			values = append(values, v)
			i++
		}
	}

	return strings.Join(conditions, " AND "), values
}
