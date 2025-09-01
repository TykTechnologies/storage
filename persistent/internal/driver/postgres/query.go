package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/TykTechnologies/storage/persistent/internal/types"
	"github.com/TykTechnologies/storage/persistent/model"
	"gorm.io/gorm"
	"math"
	"reflect"
	"strings"
	"time"
)

func (d *driver) Query(ctx context.Context, object model.DBObject, result interface{}, filter model.DBM) error {
	// Check if the database connection is valid
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
	db = d.translateQuery(db, filter, object)

	// Determine if result is a slice or a single object
	resultElem := resultVal.Elem()
	isSingle := resultElem.Kind() != reflect.Slice

	// Execute the query based on the result type
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

func (d *driver) Count(ctx context.Context, row model.DBObject, filters ...model.DBM) (count int, error error) {
	tableName, err := d.validateDBAndTable(row)
	if err != nil {
		return 0, err
	}

	tableExist, _ := d.HasTable(ctx, row.TableName())
	if !tableExist {
		return 0, errors.New(types.ErrorCollectionNotFound)
	}

	db := d.db.WithContext(ctx).Table(tableName)
	// If we have a filter, use our translator function
	if len(filters) == 1 {
		// Add _count flag to the filter to ensure proper handling in translateQuery
		countFilter := make(model.DBM)
		for k, v := range filters[0] {
			countFilter[k] = v
		}
		countFilter["_count"] = true

		db = d.translateQuery(db, countFilter, row)
	}
	var result int64
	err = db.Count(&result).Error
	if err != nil {
		return 0, error
	}
	return int(result), nil
}

func (d *driver) Aggregate(ctx context.Context, row model.DBObject, pipeline []model.DBM) ([]model.DBM, error) {
	tableName, err := d.validateDBAndTable(row)
	if err != nil {
		return []model.DBM{}, err
	}

	// Check if pipeline is empty
	if len(pipeline) == 0 {
		return nil, errors.New("empty aggregation pipeline")
	}

	// Translate MongoDB aggregation pipeline to SQL
	// Assuming translateAggregationPipeline is a helper function that converts
	// MongoDB-style aggregation pipelines to SQL queries and parameters
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

	// Process the results
	results := []model.DBM{}

	for rows.Next() {
		// Create a slice of interface{} to hold the values
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}

		// Scan the row into the values
		err := rows.Scan(values...)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Create a DBM for the row
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
		return nil, nil, errors.New("nil database connection")
	}

	// Make a copy of the DB to avoid modifying the original
	result := db
	updateMap := map[string]interface{}{}

	// Process MongoDB update operators
	for operator, fields := range update {
		switch operator {
		case "$set":
			// $set operator: directly set field values
			if setMap, ok := fields.(map[string]interface{}); ok && len(setMap) > 0 {
				for field, value := range setMap {
					updateMap[field] = value
				}
			} else if setMap, ok := fields.(model.DBM); ok && len(setMap) > 0 {
				// Handle model.DBM type which is common in the codebase
				for field, value := range setMap {
					updateMap[field] = value
				}
			}

		case "$inc":
			// $inc operator: increment field values
			if incMap, ok := fields.(map[string]interface{}); ok {
				for field, value := range incMap {
					// Store the expression in the update map
					updateMap[field] = gorm.Expr(fmt.Sprintf("%s + ?", field), value)
				}
			} else if incMap, ok := fields.(model.DBM); ok {
				for field, value := range incMap {
					updateMap[field] = gorm.Expr(fmt.Sprintf("%s + ?", field), value)
				}
			}

		case "$mul":
			// $mul operator: multiply field values
			if mulMap, ok := fields.(map[string]interface{}); ok {
				for field, value := range mulMap {
					// Store the expression in the update map
					updateMap[field] = gorm.Expr(fmt.Sprintf("%s * ?", field), value)
				}
			} else if mulMap, ok := fields.(model.DBM); ok {
				for field, value := range mulMap {
					updateMap[field] = gorm.Expr(fmt.Sprintf("%s * ?", field), value)
				}
			}

		case "$unset":
			// $unset operator: set fields to NULL
			if unsetMap, ok := fields.(map[string]interface{}); ok {
				for field := range unsetMap {
					updateMap[field] = nil
				}
			} else if unsetMap, ok := fields.(model.DBM); ok {
				for field := range unsetMap {
					updateMap[field] = nil
				}
			}

		case "$min":
			// $min operator: update field if new value is less than current value
			if minMap, ok := fields.(map[string]interface{}); ok {
				for field, value := range minMap {
					updateMap[field] = gorm.Expr(fmt.Sprintf("CASE WHEN ? < %s THEN ? ELSE %s END", field, field), value, value)
				}
			} else if minMap, ok := fields.(model.DBM); ok {
				for field, value := range minMap {
					updateMap[field] = gorm.Expr(fmt.Sprintf("CASE WHEN ? < %s THEN ? ELSE %s END", field, field), value, value)
				}
			}

		case "$max":
			// $max operator: update field if new value is greater than current value
			if maxMap, ok := fields.(map[string]interface{}); ok {
				for field, value := range maxMap {
					updateMap[field] = gorm.Expr(fmt.Sprintf("CASE WHEN ? > %s THEN ? ELSE %s END", field, field), value, value)
				}
			} else if maxMap, ok := fields.(model.DBM); ok {
				for field, value := range maxMap {
					updateMap[field] = gorm.Expr(fmt.Sprintf("CASE WHEN ? > %s THEN ? ELSE %s END", field, field), value, value)
				}
			}

		case "$currentDate":
			// $currentDate operator: set fields to current date/time
			if dateMap, ok := fields.(map[string]interface{}); ok {
				for field := range dateMap {
					updateMap[field] = gorm.Expr("CURRENT_TIMESTAMP")
				}
			} else if dateMap, ok := fields.(model.DBM); ok {
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
func (d *driver) translateQuery(db *gorm.DB, q model.DBM, result interface{}) *gorm.DB {
	if db == nil {
		return nil
	}

	where := map[string]interface{}{}
	order := ""

	// Sharding support
	shardField, useSharding := q["_date_sharding"].(string)
	var minShardDate, maxShardDate time.Time

	// Check if table sharding is enabled (you might need to add this as a configuration option)
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

		// Handle $or operator
		if k == "$or" {
			if nested, ok := v.([]model.DBM); ok {
				for ni, n := range nested {
					for nk, nv := range n {
						val := ""
						if o, ok := nv.(model.ObjectID); ok {
							val = o.Hex()
						} else {
							val = fmt.Sprint(nv)
						}

						if ni == 0 {
							db = db.Where(nk+" = ?", val)
						} else {
							db = db.Or(nk+" = ?", val)
						}
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
					db = db.Not(fmt.Sprintf("%v = ?", k), nv)
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
		where[strings.Replace(k, ".", "_", -1)] = v
	}

	db = db.Where(where)

	// Handle count
	_, counter := q["_count"].(bool)
	if counter {
		db = db.Select("count(1) as cnt")
	}

	// Handle sharding
	if useSharding {
		if minShardDate.IsZero() || maxShardDate.IsZero() {
			panic("Sharding requires both gte and lte date dimensions")
		}

		baseTable := ""
		if obj, ok := result.(model.DBObject); ok {
			baseTable = obj.TableName()
		} else {
			// Try to get collection name from the result type
			baseTable, _ = getCollectionName(result)
		}

		if baseTable != "" {
			allTablesSQL := []string{}
			days := int(math.Ceil(maxShardDate.Sub(minShardDate).Hours() / 24))

			for i := 0; i <= days; i++ {
				table := baseTable + "_" + minShardDate.Add(time.Duration(i*24)*time.Hour).Format("20060102")

				// Check if table exists
				exists, _ := d.HasTable(context.Background(), table)
				if !exists {
					continue
				}

				allTablesSQL = append(allTablesSQL, "SELECT * FROM "+table)
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

	return db
}

func translateAggregationPipeline(tableName string, pipeline []model.DBM) (string, []interface{}, error) {
	// Initialize SQL parts
	var selectClause string = "*"
	var fromClause string = tableName
	var whereClause string
	var groupByClause string
	var havingClause string
	var orderByClause string
	var limitClause string
	var offsetClause string

	// Initialize args for parameterized query
	var args []interface{}
	var argIndex int = 1

	// Process each stage in the pipeline
	for _, stage := range pipeline {
		// Each stage should have exactly one key (the operator)
		if len(stage) != 1 {
			return "", nil, errors.New("each pipeline stage must have exactly one operator")
		}

		// Get the operator and its value
		var operator string
		var value interface{}
		for k, v := range stage {
			operator = k
			value = v
		}

		// Process the operator
		switch operator {
		case "$match":
			// $match: Filter documents (WHERE clause)
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
			// $group: Group documents (GROUP BY clause)
			if groupExpr, ok := value.(model.DBM); ok {
				// Process _id field which defines the grouping keys
				if idExpr, ok := groupExpr["_id"]; ok {
					if idMap, ok := idExpr.(model.DBM); ok {
						// _id is a document with field:expression pairs
						groupFields := []string{}
						for _, expr := range idMap {
							// For simplicity, we assume the expression is just a field name
							if fieldName, ok := expr.(string); ok {
								if strings.HasPrefix(fieldName, "$") {
									// Remove the $ prefix for field references
									fieldName = strings.TrimPrefix(fieldName, "$")
								}
								groupFields = append(groupFields, fieldName)
							} else {
								return "", nil, errors.New("complex group expressions not supported")
							}
						}
						if len(groupFields) > 0 {
							groupByClause = strings.Join(groupFields, ", ")
						}
					} else if idExpr == nil {
						// _id: null means group all documents
						groupByClause = ""
					} else if fieldName, ok := idExpr.(string); ok {
						// _id is a simple field reference
						if strings.HasPrefix(fieldName, "$") {
							// Remove the $ prefix for field references
							fieldName = strings.TrimPrefix(fieldName, "$")
						}
						groupByClause = fieldName
					} else {
						return "", nil, errors.New("complex group expressions not supported")
					}
				}

				// Process aggregation functions
				selectParts := []string{}

				// Add group by fields to select clause
				if groupByClause != "" {
					groupFields := strings.Split(groupByClause, ", ")
					for _, field := range groupFields {
						selectParts = append(selectParts, field)
					}
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

							// For simplicity, we assume the function argument is just a field name or a literal
							var argStr string
							if funcArg == 1 && funcName == "$sum" {
								// Special case for $sum: 1 which is COUNT(*)
								argStr = "*"
							} else if fieldName, ok := funcArg.(string); ok {
								if strings.HasPrefix(fieldName, "$") {
									// Remove the $ prefix for field references
									fieldName = strings.TrimPrefix(fieldName, "$")
								}
								argStr = fieldName
							} else {
								// For literals, add as a parameter
								argStr = fmt.Sprintf("?")
								args = append(args, funcArg)
								argIndex++
							}

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
			// $project: Reshape documents (SELECT clause)
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
									projectParts = append(projectParts, fmt.Sprintf("CONCAT(%s) AS %s", strings.Join(concatParts, ", "), field))
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
			// $sort: Sort documents (ORDER BY clause)
			if sortExpr, ok := value.(model.DBM); ok {
				sortParts := []string{}
				for field, direction := range sortExpr {
					var dirStr string
					if dir, ok := direction.(int); ok {
						if dir == 1 {
							dirStr = "ASC"
						} else if dir == -1 {
							dirStr = "DESC"
						} else {
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
			// $limit: Limit the number of documents (LIMIT clause)
			if limit, ok := value.(int); ok {
				limitClause = fmt.Sprintf("%d", limit)
			} else {
				return "", nil, errors.New("$limit value must be an integer")
			}

		case "$skip":
			// $skip: Skip documents (OFFSET clause)
			if skip, ok := value.(int); ok {
				offsetClause = fmt.Sprintf("%d", skip)
			} else {
				return "", nil, errors.New("$skip value must be an integer")
			}

		default:
			return "", nil, fmt.Errorf("unsupported aggregation operator: %s", operator)
		}
	}
	// Build the SQL query
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

		// Handle field conditions
		switch val := v.(type) {
		case model.DBM:
			// Handle operators like $gt, $gte, etc.
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
					// Handle IN operator (simplified)
					inValues, ok := opVal.([]interface{})
					if !ok {
						// Handle error or try to convert
						continue
					}
					placeholders := make([]string, len(inValues))
					for j := range inValues {
						placeholders[j] = fmt.Sprintf("?")
						values = append(values, inValues[j])
						i++
					}
					conditions = append(conditions, fmt.Sprintf("%s IN (%s)", k, strings.Join(placeholders, ",")))
				case "$nin":
					// Handle NOT IN operator (similar to $in)
					// ...
				}
			}
		default:
			// Handle direct equality
			conditions = append(conditions, fmt.Sprintf("%s = ?", k))
			values = append(values, v)
			i++
		}
	}

	return strings.Join(conditions, " AND "), values
}
