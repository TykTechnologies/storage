//go:build postgres || postgres16.1 || postgres15 || postgres14.11 || postgres13.3 || postgres12.22
// +build postgres postgres16.1 postgres15 postgres14.11 postgres13.3 postgres12.22

package postgres

import (
	"database/sql"
	"fmt"
	"github.com/TykTechnologies/storage/persistent/internal/types"
	"github.com/TykTechnologies/storage/persistent/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestQuery(t *testing.T) {
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	// Helper function to clean up test data
	cleanupTestData := func(tableName string) {
		err := driver.db.WithContext(ctx).Exec(fmt.Sprintf("DELETE FROM %s", tableName)).Error
		if err != nil {
			t.Logf("Error cleaning up test data: %v", err)
		}
	}

	// Test case 1: Query with simple filter
	t.Run("QueryWithSimpleFilter", func(t *testing.T) {
		// Create a test table and insert test data
		testObj := &TestObject{TableNameValue: "test_query_simple"}
		defer cleanupTestData(testObj.TableName())

		// Migrate to create the table
		err := driver.Migrate(ctx, []model.DBObject{testObj})
		require.NoError(t, err, "Failed to create test table")

		// Insert test data
		for i := 1; i <= 5; i++ {
			obj := &TestObject{
				TableNameValue: "test_query_simple",
				ID:             model.NewObjectID(),
				Name:           fmt.Sprintf("Test %d", i),
				Value:          i * 10,
				CreatedAt:      time.Now(),
			}
			err := driver.Insert(ctx, obj)
			require.NoError(t, err, "Failed to insert test data")
		}

		// Query for objects with Value > 20
		var results []*TestObject
		filter := model.DBM{"value": model.DBM{"$gt": 20}}
		err = driver.Query(ctx, testObj, &results, filter)
		require.NoError(t, err, "Query should not return an error")

		// Verify results
		assert.Equal(t, 3, len(results), "Should find 3 objects with Value > 20")
		for _, result := range results {
			assert.Greater(t, result.Value, 20, "All results should have Value > 20")
		}
	})

	// Test case 2: Query with empty result
	t.Run("QueryWithEmptyResult", func(t *testing.T) {
		// Create a test table and insert test data
		testObj := &TestObject{TableNameValue: "test_query_empty"}
		defer cleanupTestData(testObj.TableName())

		// Migrate to create the table
		err := driver.Migrate(ctx, []model.DBObject{testObj})
		require.NoError(t, err, "Failed to create test table")

		// Insert test data
		for i := 1; i <= 5; i++ {
			obj := &TestObject{
				TableNameValue: "test_query_empty",
				ID:             model.NewObjectID(),
				Name:           fmt.Sprintf("Test %d", i),
				Value:          i * 10,
				CreatedAt:      time.Now(),
			}
			err := driver.Insert(ctx, obj)
			require.NoError(t, err, "Failed to insert test data")
		}

		// Query with filter that matches no objects
		var results []*TestObject
		filter := model.DBM{"value": model.DBM{"$gt": 100}}
		err = driver.Query(ctx, testObj, &results, filter)

		assert.ErrorIs(t, err, sql.ErrNoRows, "Query should return an error with empty result")
		assert.Empty(t, results, "Result slice should be empty")
	})

	// Test case 3: Query with multiple conditions
	t.Run("QueryWithMultipleConditions", func(t *testing.T) {
		// Create a test table and insert test data
		testObj := &TestObject{TableNameValue: "test_query_multiple"}
		defer cleanupTestData(testObj.TableName())

		// Migrate to create the table
		err := driver.Migrate(ctx, []model.DBObject{testObj})
		require.NoError(t, err, "Failed to create test table")

		// Insert test data
		categories := []string{"A", "B", "C"}
		for i := 1; i <= 9; i++ {
			obj := &TestObject{
				TableNameValue: "test_query_multiple",
				ID:             model.NewObjectID(),
				Name:           fmt.Sprintf("Test %d", i),
				Value:          i * 10,
				Category:       categories[i%3],
				CreatedAt:      time.Now(),
			}
			err := driver.Insert(ctx, obj)
			require.NoError(t, err, "Failed to insert test data")
		}

		// Query for objects with Value > 30 AND Category = "A"
		var results []*TestObject
		filter := model.DBM{
			"value":    model.DBM{"$gt": 30},
			"category": "A",
		}
		err = driver.Query(ctx, testObj, &results, filter)
		require.NoError(t, err, "Query should not return an error")

		// Verify results
		assert.NotEmpty(t, results, "Should find at least one object")
		for _, result := range results {
			assert.Greater(t, result.Value, 30, "All results should have Value > 30")
			assert.Equal(t, "A", result.Category, "All results should have Category = 'A'")
		}
	})

	// Test case 4: Query with invalid result parameter
	t.Run("QueryWithInvalidResultParameter", func(t *testing.T) {
		testObj := &TestObject{TableNameValue: "test_query_invalid"}

		// Try to query with a non-pointer result
		var results []*TestObject
		filter := model.DBM{}
		err := driver.Query(ctx, testObj, results, filter) // Note: passing results, not &results
		assert.Error(t, err, "Query should return an error with non-pointer result parameter")
	})

	// Test case 5: Query with nil object
	t.Run("QueryWithNilObject", func(t *testing.T) {
		var results []*TestObject
		filter := model.DBM{}
		err := driver.Query(ctx, nil, &results, filter)
		assert.Error(t, err, "Query should return an error with nil object")
	})

	// Test case 6: Query with nil result
	t.Run("QueryWithNilResult", func(t *testing.T) {
		testObj := &TestObject{TableNameValue: "test_query_nil_result"}
		filter := model.DBM{}
		err := driver.Query(ctx, testObj, nil, filter)
		assert.Error(t, err, "Query should return an error with nil result")
	})
}

func TestCount(t *testing.T) {
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	// Helper function to clean up test data
	cleanupTestData := func(tableName string) {
		err := driver.db.WithContext(ctx).Exec(fmt.Sprintf("DELETE FROM %s", tableName)).Error
		if err != nil {
			t.Logf("Error cleaning up test data: %v", err)
		}
	}

	// Test case 1: Count all objects in a table
	t.Run("CountAllObjects", func(t *testing.T) {
		// Create a test table and insert test data
		testObj := &TestObject{TableNameValue: "test_count_all"}
		defer cleanupTestData(testObj.TableName())

		// Migrate to create the table
		err := driver.Migrate(ctx, []model.DBObject{testObj})
		require.NoError(t, err, "Failed to create test table")

		// Insert test data
		for i := 1; i <= 5; i++ {
			obj := &TestObject{
				TableNameValue: "test_count_all",
				ID:             model.NewObjectID(),
				Name:           fmt.Sprintf("Test %d", i),
				Value:          i * 10,
				CreatedAt:      time.Now(),
			}
			err := driver.Insert(ctx, obj)
			require.NoError(t, err, "Failed to insert test data")
		}

		// Count all objects
		count, err := driver.Count(ctx, testObj)
		require.NoError(t, err, "Count should not return an error")
		assert.Equal(t, 5, count, "Should count 5 objects in the table")
	})

	// Test case 2: Count objects with a filter
	t.Run("CountWithFilter", func(t *testing.T) {
		// Create a test table and insert test data
		testObj := &TestObject{TableNameValue: "test_count_filter"}
		defer cleanupTestData(testObj.TableName())

		// Migrate to create the table
		err := driver.Migrate(ctx, []model.DBObject{testObj})
		require.NoError(t, err, "Failed to create test table")

		recordsCat1 := 0
		// Insert test data
		for i := 1; i <= 10; i++ {
			obj := &TestObject{
				TableNameValue: "test_count_filter",
				ID:             model.NewObjectID(),
				Name:           fmt.Sprintf("Test %d", i),
				Value:          i * 10,
				Category:       fmt.Sprintf("Category %d", (i%3)+1),
				CreatedAt:      time.Now(),
			}
			if obj.Category == "Category 1" {
				recordsCat1++
			}
			err := driver.Insert(ctx, obj)
			require.NoError(t, err, "Failed to insert test data")
		}

		// Count objects with Value > 50
		filter := model.DBM{"value": model.DBM{"$gt": 50}}
		count, err := driver.Count(ctx, testObj, filter)
		require.NoError(t, err, "Count with filter should not return an error")
		assert.Equal(t, 5, count, "Should count 5 objects with Value > 50")

		// Count objects in Category 1
		filter = model.DBM{"category": "Category 1"}
		count, err = driver.Count(ctx, testObj, filter)
		require.NoError(t, err, "Count with category filter should not return an error")
		assert.Equalf(t, recordsCat1, count, "expected %d objects in Category 1", recordsCat1)
	})

	// Test case 3: Count objects with multiple filters
	t.Run("CountWithMultipleFilters", func(t *testing.T) {
		// Create a test table and insert test data
		testObj := &TestObject{TableNameValue: "test_count_multiple"}
		defer cleanupTestData(testObj.TableName())

		// Migrate to create the table
		err := driver.Migrate(ctx, []model.DBObject{testObj})
		require.NoError(t, err, "Failed to create test table")

		// Insert test data
		for i := 1; i <= 15; i++ {
			obj := &TestObject{
				TableNameValue: "test_count_multiple",
				ID:             model.NewObjectID(),
				Name:           fmt.Sprintf("Test %d", i),
				Value:          i * 10,
				Category:       fmt.Sprintf("Category %d", i%2+1),
				Active:         i%2 == 0, // Even numbers are active
				CreatedAt:      time.Now(),
			}
			err := driver.Insert(ctx, obj)
			require.NoError(t, err, "Failed to insert test data")
		}

		// Count objects with Value > 70 AND Category = "Category 2"
		filter := model.DBM{
			"value":    model.DBM{"$gt": 70},
			"category": "Category 2",
		}
		count, err := driver.Count(ctx, testObj, filter)
		require.NoError(t, err, "Count with multiple filters should not return an error")

		// We expect objects with indices 8, 11, 14 to match (Value > 70 AND Category 2)
		expectedCount := 4
		assert.Equal(t, expectedCount, count, "Should count %d objects matching multiple filters", expectedCount)
	})

	// Test case 4: Count with empty table
	t.Run("CountWithEmptyTable", func(t *testing.T) {
		// Create a test table but don't insert any data
		testObj := &TestObject{TableNameValue: "test_count_empty"}
		defer cleanupTestData(testObj.TableName())

		// Migrate to create the table
		err := driver.Migrate(ctx, []model.DBObject{testObj})
		require.NoError(t, err, "Failed to create test table")

		// Count all objects in empty table
		count, err := driver.Count(ctx, testObj)
		require.NoError(t, err, "Count on empty table should not return an error")
		assert.Equal(t, 0, count, "Should count 0 objects in empty table")
	})

	// Test case 5: Count with nil object
	t.Run("CountWithNilObject", func(t *testing.T) {
		// Try to count with nil object
		_, err := driver.Count(ctx, nil)
		assert.Error(t, err, "Count with nil object should return an error")
	})

	// Test case 6: Count with non-existent table
	t.Run("CountWithNonExistentTable", func(t *testing.T) {
		// Create an object for a table that doesn't exist
		nonExistentObj := &TestObject{TableNameValue: "non_existent_table"}

		// Try to count
		count, err := driver.Count(ctx, nonExistentObj)
		assert.Equal(t, 0, count, "Count on non-existent table should return 0")
		assert.Error(t, err, "Count on non-existent table should return an error")
	})
}

func TestAggregate(t *testing.T) {
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	// Helper function to clean up test data
	cleanupTestData := func(tableName string) {
		err := driver.db.WithContext(ctx).Exec(fmt.Sprintf("DELETE FROM %s", tableName)).Error
		if err != nil {
			t.Logf("Error cleaning up test data: %v", err)
		}
	}

	// Helper function to set up test data
	setupTestData := func(tableName string) {
		testObj := &TestObject{TableNameValue: tableName}

		// Migrate to create the table
		err := driver.Migrate(ctx, []model.DBObject{testObj})
		require.NoError(t, err, "Failed to create test table")

		// Insert test data
		categories := []string{"A", "B", "C"}
		for i := 1; i <= 10; i++ {
			obj := &TestObject{
				TableNameValue: tableName,
				ID:             model.NewObjectID(),
				Name:           fmt.Sprintf("Test %d", i),
				Value:          i * 10,
				Category:       categories[i%3],
				Active:         i%2 == 0, // Even numbers are active
				CreatedAt:      time.Now(),
			}
			err := driver.Insert(ctx, obj)
			require.NoError(t, err, "Failed to insert test data")
		}
	}

	// Test case 1: Simple $match aggregation
	t.Run("SimpleMatchAggregation", func(t *testing.T) {
		tableName := "test_agg_match"
		setupTestData(tableName)
		defer cleanupTestData(tableName)

		// Create a simple $match pipeline
		pipeline := []model.DBM{
			{
				"$match": model.DBM{
					"value": model.DBM{"$gt": 50},
				},
			},
		}

		// Execute the aggregation
		results, err := driver.Aggregate(ctx, &TestObject{TableNameValue: tableName}, pipeline)
		require.NoError(t, err, "Aggregate should not return an error")

		// Verify the results
		assert.Equal(t, 5, len(results), "Should find 5 objects with Value > 50")
		for _, result := range results {
			value, ok := result["value"].(int64)
			assert.True(t, ok, "Value should be an int")
			assert.Greater(t, value, int64(50), "All results should have Value > 50")
			assert.True(t, ok, "Value should be a float64")
			assert.Greater(t, value, int64(50), "All results should have Value > 50")
		}
	})

	// Test case 2: $match and $sort aggregation
	t.Run("MatchAndSortAggregation", func(t *testing.T) {
		tableName := "test_agg_match_sort"
		setupTestData(tableName)
		defer cleanupTestData(tableName)

		// Create a pipeline with $match and $sort
		pipeline := []model.DBM{
			{
				"$match": model.DBM{
					"category": "A",
				},
			},
			{
				"$sort": model.DBM{
					"value": -1, // Descending order
				},
			},
		}

		// Execute the aggregation
		results, err := driver.Aggregate(ctx, &TestObject{TableNameValue: tableName}, pipeline)
		require.NoError(t, err, "Aggregate should not return an error")

		// Verify the results
		assert.NotEmpty(t, results, "Should find objects in Category A")
		for _, result := range results {
			assert.Equal(t, "A", result["category"], "All results should have Category A")
		}

		// Verify sorting
		for i := 0; i < len(results)-1; i++ {
			value1, _ := results[i]["value"].(float64)
			value2, _ := results[i+1]["value"].(float64)
			assert.GreaterOrEqual(t, value1, value2, "Results should be sorted by Value in descending order")
		}
	})

	// Test case 3: $match, $sort, and $limit aggregation
	t.Run("MatchSortLimitAggregation", func(t *testing.T) {
		tableName := "test_agg_match_sort_limit"
		setupTestData(tableName)
		defer cleanupTestData(tableName)

		// Create a pipeline with $match, $sort, and $limit
		pipeline := []model.DBM{
			{
				"$match": model.DBM{
					"active": true,
				},
			},
			{
				"$sort": model.DBM{
					"value": 1, // Ascending order
				},
			},
			{
				"$limit": 3,
			},
		}

		// Execute the aggregation
		results, err := driver.Aggregate(ctx, &TestObject{TableNameValue: tableName}, pipeline)
		require.NoError(t, err, "Aggregate should not return an error")

		// Verify the results
		assert.Equal(t, 3, len(results), "Should find exactly 3 objects due to $limit")
		for _, result := range results {
			assert.Equal(t, true, result["active"], "All results should be active")
		}

		// Verify sorting
		for i := 0; i < len(results)-1; i++ {
			value1, _ := results[i]["value"].(float64)
			value2, _ := results[i+1]["value"].(float64)
			assert.LessOrEqual(t, value1, value2, "Results should be sorted by Value in ascending order")
		}
	})

	// Test case 4: Nil object
	t.Run("NilObject", func(t *testing.T) {
		// Create a simple pipeline
		pipeline := []model.DBM{
			{
				"$match": model.DBM{
					"value": model.DBM{"$gt": 50},
				},
			},
		}

		// Execute the aggregation with nil object
		_, err := driver.Aggregate(ctx, nil, pipeline)
		assert.Error(t, err, "Aggregate should return an error with nil object")
	})

	// Test case 5: Invalid pipeline
	t.Run("InvalidPipeline", func(t *testing.T) {
		tableName := "test_agg_invalid"
		setupTestData(tableName)
		defer cleanupTestData(tableName)

		// Create an invalid pipeline (unsupported stage)
		pipeline := []model.DBM{
			{
				"$unsupported": model.DBM{},
			},
		}

		// Execute the aggregation
		_, err := driver.Aggregate(ctx, &TestObject{TableNameValue: tableName}, pipeline)
		assert.Error(t, err, "Aggregate should return an error with invalid pipeline")
	})
}

func TestTranslateAggregationPipeline(t *testing.T) {
	// Test case 1: Simple $match stage
	t.Run("SimpleMatchStage", func(t *testing.T) {
		tableName := "test_table"

		// Create a simple $match pipeline
		pipeline := []model.DBM{
			{
				"$match": model.DBM{
					"value": model.DBM{"$gt": 50},
				},
			},
		}

		// Translate the pipeline
		query, values, err := translateAggregationPipeline(tableName, pipeline)
		require.NoError(t, err, "translateAggregationPipeline should not return an error")

		// Verify the query string contains expected SQL fragments
		assert.Contains(t, query, "SELECT * FROM test_table", "Query should select from the correct table")
		assert.Contains(t, query, "WHERE", "Query should contain a WHERE clause")
		assert.Contains(t, query, "value > ", "Query should filter on value")

		// Verify the values
		assert.Equal(t, 1, len(values), "Should have 1 parameter value")
		assert.Equal(t, 50, values[0], "Parameter value should be 50")
	})

	// Test case 2: $match and $sort stages
	t.Run("MatchAndSortStages", func(t *testing.T) {
		tableName := "test_table"

		// Create a pipeline with $match and $sort
		pipeline := []model.DBM{
			{
				"$match": model.DBM{
					"category": "A",
				},
			},
			{
				"$sort": model.DBM{
					"value": -1, // Descending order
				},
			},
		}

		// Translate the pipeline
		query, values, err := translateAggregationPipeline(tableName, pipeline)
		require.NoError(t, err, "translateAggregationPipeline should not return an error")

		// Verify the query string contains expected SQL fragments
		assert.Contains(t, query, "SELECT * FROM test_table", "Query should select from the correct table")
		assert.Contains(t, query, "WHERE", "Query should contain a WHERE clause")
		assert.Contains(t, query, "category = ", "Query should filter on category")
		assert.Contains(t, query, "ORDER BY", "Query should contain an ORDER BY clause")
		assert.Contains(t, query, "value DESC", "Query should order by value in descending order")

		// Verify the values
		assert.Equal(t, 1, len(values), "Should have 1 parameter value")
		assert.Equal(t, "A", values[0], "Parameter value should be 'A'")
	})

	// Test case 3: $match, $sort, and $limit stages
	t.Run("MatchSortLimitStages", func(t *testing.T) {
		tableName := "test_table"

		// Create a pipeline with $match, $sort, and $limit
		pipeline := []model.DBM{
			{
				"$match": model.DBM{
					"active": true,
				},
			},
			{
				"$sort": model.DBM{
					"value": 1, // Ascending order
				},
			},
			{
				"$limit": 3,
			},
		}

		// Translate the pipeline
		query, values, err := translateAggregationPipeline(tableName, pipeline)
		require.NoError(t, err, "translateAggregationPipeline should not return an error")

		// Verify the query string contains expected SQL fragments
		assert.Contains(t, query, "SELECT * FROM test_table", "Query should select from the correct table")
		assert.Contains(t, query, "WHERE", "Query should contain a WHERE clause")
		assert.Contains(t, query, "active = ", "Query should filter on active")
		assert.Contains(t, query, "ORDER BY", "Query should contain an ORDER BY clause")
		assert.Contains(t, query, "value ASC", "Query should order by value in ascending order")
		assert.Contains(t, query, "LIMIT 3", "Query should limit to 3 results")

		// Verify the values
		assert.Equal(t, 1, len(values), "Should have 1 parameter value")
		assert.Equal(t, true, values[0], "Parameter value should be true")
	})

	// Test case 4: Empty pipeline
	t.Run("EmptyPipeline", func(t *testing.T) {
		tableName := "test_table"

		// Create an empty pipeline
		pipeline := []model.DBM{}

		// Translate the pipeline
		query, values, err := translateAggregationPipeline(tableName, pipeline)
		require.NoError(t, err, "translateAggregationPipeline should not return an error with empty pipeline")

		// Verify the query string
		assert.Equal(t, "SELECT * FROM test_table", query, "Query should be a simple SELECT with empty pipeline")

		// Verify the values
		assert.Empty(t, values, "Should have no parameter values with empty pipeline")
	})

	// Test case 5: Unsupported stage
	t.Run("UnsupportedStage", func(t *testing.T) {
		tableName := "test_table"

		// Create a pipeline with an unsupported stage
		pipeline := []model.DBM{
			{
				"$unsupported": model.DBM{},
			},
		}

		// Translate the pipeline
		_, _, err := translateAggregationPipeline(tableName, pipeline)

		// Check if the function returns an error for unsupported stage
		assert.Error(t, err, "translateAggregationPipeline should return an error for unsupported stage")
		assert.Contains(t, err.Error(), "unsupported", "Error message should mention unsupported stage")
	})
}

func TestTranslateAggregationPipelineGroup(t *testing.T) {
	// Define test cases
	testCases := []struct {
		name             string
		tableName        string
		pipeline         []model.DBM
		expectedParts    []string // Parts that must be in the query
		expectedNotParts []string // Parts that must not be in the query
		expectedArgCount int      // Number of expected arguments
		expectedArgs     []interface{}
		expectedError    bool
	}{
		{
			name:      "Group by Single Field",
			tableName: "test_table",
			pipeline: []model.DBM{
				{
					"$group": model.DBM{
						"_id": "$category",
						"count": model.DBM{
							"$sum": 1,
						},
					},
				},
			},
			expectedParts: []string{
				"SELECT", "category", "SUM(*) AS count", "FROM test_table", "GROUP BY category",
			},
			expectedArgCount: 0,
			expectedError:    false,
		},
		{
			name:      "Group by Multiple Fields",
			tableName: "test_table",
			pipeline: []model.DBM{
				{
					"$group": model.DBM{
						"_id": model.DBM{
							"category": "$category",
							"status":   "$status",
						},
						"total": model.DBM{
							"$sum": "$amount",
						},
						"avg_value": model.DBM{
							"$avg": "$value",
						},
					},
				},
			},
			expectedParts: []string{
				"SELECT", "category", "status", "SUM(amount) AS total", "AVG(value) AS avg_value",
				"FROM test_table", "GROUP BY",
			},
			expectedArgCount: 0,
			expectedError:    false,
		},
		{
			name:      "Group with Multiple Aggregation Functions",
			tableName: "test_table",
			pipeline: []model.DBM{
				{
					"$group": model.DBM{
						"_id": "$category",
						"count": model.DBM{
							"$sum": 1,
						},
						"total": model.DBM{
							"$sum": "$amount",
						},
						"min_value": model.DBM{
							"$min": "$value",
						},
						"max_value": model.DBM{
							"$max": "$value",
						},
						"avg_value": model.DBM{
							"$avg": "$value",
						},
					},
				},
			},
			expectedParts: []string{
				"SELECT", "category", "SUM(*) AS count", "SUM(amount) AS total",
				"MIN(value) AS min_value", "MAX(value) AS max_value", "AVG(value) AS avg_value",
				"FROM test_table", "GROUP BY category",
			},
			expectedArgCount: 0,
			expectedError:    false,
		},
		{
			name:      "Group All Documents (null _id)",
			tableName: "test_table",
			pipeline: []model.DBM{
				{
					"$group": model.DBM{
						"_id": nil,
						"total_count": model.DBM{
							"$sum": 1,
						},
						"grand_total": model.DBM{
							"$sum": "$amount",
						},
					},
				},
			},
			expectedParts: []string{
				"SELECT", "SUM(*) AS total_count", "SUM(amount) AS grand_total", "FROM test_table",
			},
			expectedNotParts: []string{
				"GROUP BY", // Should not have GROUP BY for null _id
			},
			expectedArgCount: 0,
			expectedError:    false,
		},
		{
			name:      "Complex Pipeline with Match, Group, and Sort",
			tableName: "test_table",
			pipeline: []model.DBM{
				{
					"$match": model.DBM{
						"status": "active",
					},
				},
				{
					"$group": model.DBM{
						"_id": "$category",
						"count": model.DBM{
							"$sum": 1,
						},
						"total": model.DBM{
							"$sum": "$amount",
						},
					},
				},
				{
					"$sort": model.DBM{
						"total": -1,
					},
				},
			},
			expectedParts: []string{
				"SELECT", "category", "SUM(*) AS count", "SUM(amount) AS total",
				"FROM test_table", "WHERE status = ?", "GROUP BY category", "ORDER BY total DESC",
			},
			expectedArgCount: 1,
			expectedArgs:     []interface{}{"active"},
			expectedError:    false,
		},
	}

	// Run test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			query, args, err := translateAggregationPipeline(tc.tableName, tc.pipeline)

			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)

				// Check that all expected parts are in the query
				for _, part := range tc.expectedParts {
					assert.Contains(t, query, part, "Query should contain: %s", part)
				}

				// Check that all parts that should not be in the query are indeed not there
				for _, part := range tc.expectedNotParts {
					assert.NotContains(t, query, part, "Query should not contain: %s", part)
				}

				// Check argument count
				assert.Equal(t, tc.expectedArgCount, len(args), "Argument count should match")

				// Check specific argument values if provided
				for i, expectedArg := range tc.expectedArgs {
					assert.Equal(t, expectedArg, args[i], "Argument at index %d should match", i)
				}
			}
		})
	}
}

func TestBuildWhereClause(t *testing.T) {
	// Test case 1: Simple equality filter
	t.Run("SimpleEqualityFilter", func(t *testing.T) {
		filter := model.DBM{"name": "test", "value": 123}

		whereClause, values := buildWhereClause(filter)

		// The order of conditions in the WHERE clause might vary, so we need to check both possibilities
		possibleClauses := []string{
			"name = ? AND value = ?",
			"value = ? AND name = ?",
		}

		// Check if the generated clause matches any of the possible clauses
		clauseMatches := false
		for _, clause := range possibleClauses {
			if whereClause == clause {
				clauseMatches = true
				break
			}
		}

		assert.True(t, clauseMatches, "WHERE clause should match one of the expected formats")

		// Check that values contains both "test" and 123, in some order
		assert.Equal(t, 2, len(values), "Should have 2 values")
		assert.Contains(t, values, "test", "Values should contain 'test'")
		assert.Contains(t, values, 123, "Values should contain 123")
	})

	// Test case 2: Filter with comparison operators
	t.Run("ComparisonOperators", func(t *testing.T) {
		filter := model.DBM{
			"age":   model.DBM{"$gt": 30},
			"score": model.DBM{"$lte": 100},
		}

		whereClause, values := buildWhereClause(filter)

		// The order of conditions might vary
		possibleClauses := []string{
			"age > ? AND score <= ?",
			"score <= ? AND age > ?",
		}

		clauseMatches := false
		for _, clause := range possibleClauses {
			if whereClause == clause {
				clauseMatches = true
				break
			}
		}

		assert.True(t, clauseMatches, "WHERE clause should match one of the expected formats")
		assert.Equal(t, 2, len(values), "Should have 2 values")
		assert.Contains(t, values, 30, "Values should contain 30")
		assert.Contains(t, values, 100, "Values should contain 100")
	})

	// Test case 3: Filter with multiple operators on the same field
	t.Run("MultipleOperatorsOnSameField", func(t *testing.T) {
		filter := model.DBM{
			"age": model.DBM{
				"$gt": 20,
				"$lt": 50,
			},
		}

		whereClause, values := buildWhereClause(filter)

		// The order of conditions might vary
		possibleClauses := []string{
			"age > ? AND age < ?",
			"age < ? AND age > ?",
		}

		clauseMatches := false
		for _, clause := range possibleClauses {
			if whereClause == clause {
				clauseMatches = true
				break
			}
		}

		assert.True(t, clauseMatches, "WHERE clause should match one of the expected formats")
		assert.Equal(t, 2, len(values), "Should have 2 values")
		assert.Contains(t, values, 20, "Values should contain 20")
		assert.Contains(t, values, 50, "Values should contain 50")
	})

	// Test case 4: Empty filter
	t.Run("EmptyFilter", func(t *testing.T) {
		filter := model.DBM{}

		whereClause, values := buildWhereClause(filter)

		assert.Equal(t, "", whereClause, "WHERE clause should be empty for empty filter")
		assert.Empty(t, values, "Values should be empty for empty filter")
	})
}

func TestApplyMongoUpdateOperators(t *testing.T) {
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)
	tableName := "test_update"

	// Helper function to clean up test data
	cleanupTestData := func(tableName string) {
		err := driver.db.WithContext(ctx).Exec(fmt.Sprintf("DELETE FROM %s", tableName)).Error
		if err != nil {
			t.Logf("Error cleaning up test data: %v", err)
		}
	}

	// Helper function to create a test object and insert it
	createTestObject := func(tableName string, id model.ObjectID, name string, value int, active bool) *TestObject {
		obj := &TestObject{
			TableNameValue: tableName,
			ID:             id,
			Name:           name,
			Value:          value,
			Active:         active,
			CreatedAt:      time.Now(),
		}
		err := driver.Migrate(ctx, []model.DBObject{obj})
		require.NoError(t, err, "Failed to create test table")
		err = driver.Insert(ctx, obj)
		require.NoError(t, err, "Failed to insert test object")
		return obj
	}

	t.Run("nil database", func(t *testing.T) {
		update := model.DBM{
			"$set": model.DBM{
				"field": "value",
			},
		}

		_, _, err := driver.applyMongoUpdateOperators(nil, update)
		assert.Error(t, err, "applyMongoUpdateOperators should return an error with nil database")
	})
	t.Run("SetOperator", func(t *testing.T) {
		// Create a test object

		id := model.NewObjectID()
		obj := createTestObject(tableName, id, "Original Name", 10, true)
		defer cleanupTestData(obj.TableName())

		// Create an update with $set operator
		update := model.DBM{
			"$set": model.DBM{
				"name":  "Updated Name",
				"value": 20,
			},
		}

		// Apply the update operators
		db := driver.db.WithContext(ctx).Table(tableName)
		updatedDB, updates, err := driver.applyMongoUpdateOperators(db, update)
		require.NoError(t, err, "applyMongoUpdateOperators should not return an error")

		// Verify the updates map
		assert.Equal(t, "Updated Name", updates["name"], "updates map should contain the new name")
		assert.Equal(t, 20, updates["value"], "updates map should contain the new value")

		// Execute the update
		err = updatedDB.Where("id = ?", id).Updates(updates).Error
		require.NoError(t, err, "Update operation should not return an error")

		// Verify the object was updated
		var updatedObj TestObject
		err = driver.db.WithContext(ctx).Table(tableName).Where("id = ?", id).First(&updatedObj).Error
		require.NoError(t, err, "Failed to retrieve updated object")

		assert.Equal(t, "Updated Name", updatedObj.Name, "Name should be updated")
		assert.Equal(t, 20, updatedObj.Value, "Value should be updated")
		assert.True(t, updatedObj.Active, "Active should remain unchanged")
	})

	t.Run("IncOperator", func(t *testing.T) {
		// Create a test object
		id := model.NewObjectID()
		obj := createTestObject(tableName, id, "Test Inc", 10, true)
		defer cleanupTestData(obj.TableName())

		// Create an update with $inc operator
		update := model.DBM{
			"$inc": model.DBM{
				"value": 5, // Increment by 5
			},
		}

		// Apply the update operators
		db := driver.db.WithContext(ctx).Table(tableName)
		updatedDB, updates, err := driver.applyMongoUpdateOperators(db, update)
		require.NoError(t, err, "applyMongoUpdateOperators should not return an error")

		// Execute the update
		err = updatedDB.Where("id = ?", id).Updates(updates).Error
		require.NoError(t, err, "Update operation should not return an error")

		// Verify the object was updated
		var updatedObj TestObject
		err = driver.db.WithContext(ctx).Table(tableName).Where("id = ?", id).First(&updatedObj).Error
		require.NoError(t, err, "Failed to retrieve updated object")

		assert.Equal(t, 15, updatedObj.Value, "Value should be incremented by 5")
	})

	t.Run("MultipleOperators", func(t *testing.T) {
		// Create a test object
		id := model.NewObjectID()
		obj := createTestObject(tableName, id, "Original Multiple", 10, true)
		defer cleanupTestData(obj.TableName())

		// Create an update with multiple operators
		update := model.DBM{
			"$set": model.DBM{
				"name": "Updated Multiple",
			},
			"$inc": model.DBM{
				"value": 15, // Increment by 15
			},
		}

		// Apply the update operators
		db := driver.db.WithContext(ctx).Table(tableName)
		updatedDB, updates, err := driver.applyMongoUpdateOperators(db, update)
		require.NoError(t, err, "applyMongoUpdateOperators should not return an error")

		// Execute the update
		err = updatedDB.Where("id = ?", id).Updates(updates).Error
		require.NoError(t, err, "Update operation should not return an error")

		// Verify the object was updated
		var updatedObj TestObject
		err = driver.db.WithContext(ctx).Table(tableName).Where("id = ?", id).First(&updatedObj).Error
		require.NoError(t, err, "Failed to retrieve updated object")

		assert.Equal(t, "Updated Multiple", updatedObj.Name, "Name should be updated")
		assert.Equal(t, 25, updatedObj.Value, "Value should be incremented by 15")
	})

	t.Run("EmptyUpdate", func(t *testing.T) {
		// Create an empty update
		update := model.DBM{}

		// Apply the update operators
		db := driver.db.WithContext(ctx).Table("test_table")
		updatedDB, updates, err := driver.applyMongoUpdateOperators(db, update)
		require.NoError(t, err, "applyMongoUpdateOperators should not return an error with empty update")

		// Verify the updates map is empty
		assert.Empty(t, updates, "updates map should be empty for empty update")

		// Verify the DB object is unchanged
		assert.Equal(t, db, updatedDB, "DB object should be unchanged for empty update")
	})

	t.Run("MultOperator", func(t *testing.T) {
		// Create a test object
		id := model.NewObjectID()
		obj := createTestObject(tableName, id, "Test Inc", 10, true)
		defer cleanupTestData(obj.TableName())

		// Create an update with $mult operator
		update := model.DBM{
			"$mul": model.DBM{
				"value": 3, // multiply by 3
			},
		}

		// Apply the update operators
		db := driver.db.WithContext(ctx).Table(tableName)
		updatedDB, updates, err := driver.applyMongoUpdateOperators(db, update)
		require.NoError(t, err, "applyMongoUpdateOperators should not return an error")

		// Execute the update
		err = updatedDB.Where("id = ?", id).Updates(updates).Error
		require.NoError(t, err, "Update operation should not return an error")

		// Verify the object was updated
		var updatedObj TestObject
		err = driver.db.WithContext(ctx).Table(tableName).Where("id = ?", id).First(&updatedObj).Error
		require.NoError(t, err, "Failed to retrieve updated object")

		assert.Equal(t, 30, updatedObj.Value, "Value should be multiplied by 3")
	})

	t.Run("MinOperator", func(t *testing.T) {
		// Create a test object
		id := model.NewObjectID()
		obj := createTestObject(tableName, id, "Test Inc", 10, true)
		defer cleanupTestData(obj.TableName())

		// Create an update with $min operator
		update := model.DBM{
			"$min": model.DBM{
				"value": 3, // check min val
			},
		}

		// Apply the update operators
		db := driver.db.WithContext(ctx).Table(tableName)
		updatedDB, updates, err := driver.applyMongoUpdateOperators(db, update)
		require.NoError(t, err, "applyMongoUpdateOperators should not return an error")

		// Execute the update
		err = updatedDB.Where("id = ?", id).Updates(updates).Error
		require.NoError(t, err, "Update operation should not return an error")

		// Verify the object was updated
		var updatedObj TestObject
		err = driver.db.WithContext(ctx).Table(tableName).Where("id = ?", id).First(&updatedObj).Error
		require.NoError(t, err, "Failed to retrieve updated object")

		assert.Equal(t, 3, updatedObj.Value, "Value should be 3")
	})

	t.Run("MaxOperator", func(t *testing.T) {
		// Create a test object
		id := model.NewObjectID()
		obj := createTestObject(tableName, id, "Test Inc", 10, true)
		defer cleanupTestData(obj.TableName())

		// Create an update with $min operator
		update := model.DBM{
			"$max": model.DBM{
				"value": 300, // set max value
			},
		}

		// Apply the update operators
		db := driver.db.WithContext(ctx).Table(tableName)
		updatedDB, updates, err := driver.applyMongoUpdateOperators(db, update)
		require.NoError(t, err, "applyMongoUpdateOperators should not return an error")

		// Execute the update
		err = updatedDB.Where("id = ?", id).Updates(updates).Error
		require.NoError(t, err, "Update operation should not return an error")

		// Verify the object was updated
		var updatedObj TestObject
		err = driver.db.WithContext(ctx).Table(tableName).Where("id = ?", id).First(&updatedObj).Error
		require.NoError(t, err, "Failed to retrieve updated object")

		assert.Equal(t, 300, updatedObj.Value, "Value should be 300")
	})

	t.Run("UnsetOperator", func(t *testing.T) {
		// Create and insert a test object
		id := model.NewObjectID()
		obj := createTestObject(tableName, id, "Test Inc", 10, true)
		defer cleanupTestData(obj.TableName())

		// Create an update with $unset operator using model.DBM
		dbmMap := model.DBM{
			"name": 1, // The value doesn't matter for $unset, only the field name is used
		}

		update := model.DBM{
			"$unset": dbmMap, // Using model.DBM directly
		}

		// Apply the update operators
		db := driver.db.WithContext(ctx).Table(tableName)
		updatedDB, updates, err := driver.applyMongoUpdateOperators(db, update)
		require.NoError(t, err, "applyMongoUpdateOperators should not return an error")

		// Execute the update
		err = updatedDB.Where("id = ?", id).Updates(updates).Error
		require.NoError(t, err, "Update operation should not return an error")

		// Verify the object was updated
		var updatedObj TestObject
		err = driver.db.WithContext(ctx).Table(tableName).Where("id = ?", id).First(&updatedObj).Error
		require.NoError(t, err, "Failed to retrieve updated object")

		assert.Equal(t, "", updatedObj.Name, "Name should be empty")
	})

	t.Run("CurrentDate", func(t *testing.T) {
		// Create and insert a test object
		pastTime := time.Now().Add(-24 * time.Hour)
		id := model.NewObjectID()
		obj := createTestObject(tableName, id, "Test Inc", 10, true)
		defer cleanupTestData(obj.TableName())

		dbmMap := model.DBM{
			"created_at": true,
		}

		update := model.DBM{
			"$currentDate": dbmMap, // Using model.DBM directly
		}

		// Apply the update operators
		db := driver.db.WithContext(ctx).Table(tableName)
		updatedDB, updates, err := driver.applyMongoUpdateOperators(db, update)
		require.NoError(t, err, "applyMongoUpdateOperators should not return an error")

		// Execute the update
		err = updatedDB.Where("id = ?", id).Updates(updates).Error
		require.NoError(t, err, "Update operation should not return an error")

		// Verify the object was updated
		var updatedObj TestObject
		err = driver.db.WithContext(ctx).Table(tableName).Where("id = ?", id).First(&updatedObj).Error
		require.NoError(t, err, "Failed to retrieve updated object")
		assert.True(t, updatedObj.CreatedAt.After(pastTime), "CreatedAt should be updated to a newer time")
	})

	t.Run("UnsupportedOperator", func(t *testing.T) {
		// Create an update with an unsupported operator
		update := model.DBM{
			"$unsupported": model.DBM{
				"field": "value",
			},
		}

		// Apply the update operators
		db := driver.db.WithContext(ctx).Table("test_table")
		_, _, err := driver.applyMongoUpdateOperators(db, update)

		// Check if the function returns an error for unsupported operator
		assert.Error(t, err, "applyMongoUpdateOperators should return an error for unsupported operator")
		assert.Contains(t, err.Error(), "unsupported", "Error message should mention unsupported operator")
	})
}

// TestTranslateQuery tests the translateQuery function with various MongoDB-style query operators
func TestTranslateQuery(t *testing.T) {
	// Setup test database connection
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	// Ensure the test table exists
	tableName := "test_objects"
	err := driver.db.WithContext(ctx).AutoMigrate(&TestObject{})
	require.NoError(t, err, "Failed to create test table")

	// Clean up any existing data
	err = driver.db.WithContext(ctx).Exec("DELETE FROM " + tableName).Error
	require.NoError(t, err, "Failed to clean up test table")

	// Create test data
	id1 := model.NewObjectID()
	id2 := model.NewObjectID()
	id3 := model.NewObjectID()

	testData := []TestObject{
		{
			ID:        id1,
			Name:      "Test 1",
			Value:     10,
			Category:  "A",
			CreatedAt: time.Now().Add(-48 * time.Hour),
		},
		{
			ID:        id2,
			Name:      "Test 2",
			Value:     20,
			Category:  "B",
			CreatedAt: time.Now().Add(-24 * time.Hour),
		},
		{
			ID:        id3,
			Name:      "Test 3",
			Value:     30,
			Category:  "A",
			CreatedAt: time.Now(),
		},
	}

	// Insert test data
	for _, obj := range testData {
		err := driver.db.WithContext(ctx).Table(tableName).Create(&obj).Error
		require.NoError(t, err, "Failed to insert test data")
	}

	// Define test cases
	type testCase struct {
		name          string
		query         model.DBM
		expectedCount int
	}

	testCases := []testCase{
		{
			name: "Simple Equality",
			query: model.DBM{
				"category": "A",
			},
			expectedCount: 2,
		},
		{
			name: "Multiple Equality Conditions",
			query: model.DBM{
				"category": "A",
				"value":    10,
			},
			expectedCount: 1,
		},
		{
			name: "ObjectID Equality",
			query: model.DBM{
				"id": id1,
			},
			expectedCount: 1,
		},
		{
			name: "OR Operator",
			query: model.DBM{
				"$or": []model.DBM{
					{"category": "A"},
					{"value": 20},
				},
			},
			expectedCount: 3,
		},
		{
			name: "Not Equal Operator",
			query: model.DBM{
				"category": model.DBM{
					"$ne": "A",
				},
			},
			expectedCount: 1,
		},
		{
			name: "Greater Than Operator",
			query: model.DBM{
				"value": model.DBM{
					"$gt": 10,
				},
			},
			expectedCount: 2,
		},
		{
			name: "Greater Than or Equal Operator",
			query: model.DBM{
				"value": model.DBM{
					"$gte": 20,
				},
			},
			expectedCount: 2,
		},
		{
			name: "Less Than Operator",
			query: model.DBM{
				"value": model.DBM{
					"$lt": 30,
				},
			},
			expectedCount: 2,
		},
		{
			name: "Less Than or Equal Operator",
			query: model.DBM{
				"value": model.DBM{
					"$lte": 20,
				},
			},
			expectedCount: 2,
		},
		{
			name: "IN Operator",
			query: model.DBM{
				"value": model.DBM{
					"$in": []int{10, 30},
				},
			},
			expectedCount: 2,
		},
		{
			name: "Case Insensitive Equality",
			query: model.DBM{
				"name": model.DBM{
					"$i": "test 1",
				},
			},
			expectedCount: 1,
		},
		{
			name: "Text Search",
			query: model.DBM{
				"name": model.DBM{
					"$text": "test",
				},
			},
			expectedCount: 3,
		},
		{
			name: "Limit",
			query: model.DBM{
				"category": "A",
				"_limit":   1,
			},
			expectedCount: 1,
		},
		{
			name: "Offset",
			query: model.DBM{
				"category": "A",
				"_offset":  1,
			},
			expectedCount: 1,
		},
		{
			name: "Sort Ascending",
			query: model.DBM{
				"category": "A",
				"_sort":    "value",
			},
			expectedCount: 2,
		},
		{
			name: "Sort Descending",
			query: model.DBM{
				"category": "A",
				"_sort":    "-value",
			},
			expectedCount: 2,
		},
		{
			name: "Complex Query",
			query: model.DBM{
				"category": "A",
				"value": model.DBM{
					"$gte": 10,
					"$lte": 30,
				},
				"_sort":  "-value",
				"_limit": 1,
			},
			expectedCount: 1,
		},
	}

	// Run test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a base DB query
			db := driver.db.WithContext(ctx).Table(tableName)

			// Apply the translateQuery function
			translatedDB := driver.translateQuery(db, tc.query, &TestObject{})

			// Execute the query and check the result count
			var results []TestObject
			err := translatedDB.Find(&results).Error
			require.NoError(t, err, "Query execution should not fail")

			assert.Equal(t, tc.expectedCount, len(results), "Query should return expected number of results")
		})
	}

	// Test count flag separately
	t.Run("Count", func(t *testing.T) {
		// Create a base DB query
		db := driver.db.WithContext(ctx).Table(tableName)

		// Apply the translateQuery function with count flag
		query := model.DBM{
			"category": "A",
			"_count":   true,
		}
		translatedDB := driver.translateQuery(db, query, &TestObject{})

		// Execute the query and check the count
		var count int64
		err := translatedDB.Count(&count).Error
		require.NoError(t, err, "Count query should not fail")

		assert.Equal(t, int64(2), count, "Count should return expected number")
	})
}

func TestTranslateQueryWithShardingEnabled(t *testing.T) {
	// Setup test database connection
	driver, ctx := setupTest(t)
	defer teardownTest(t, driver)

	// Enable sharding
	driver.TableSharding = true
	driver.options = &types.ClientOpts{}

	// Base table name
	baseTableName := "test_objects"

	// Create date range for sharding
	now := time.Now()
	startDate := now.Add(-3 * 24 * time.Hour) // 3 days ago
	endDate := now                            // Today

	// Create sharded tables for each day in the range
	for i := 0; i <= 3; i++ {
		date := startDate.Add(time.Duration(i*24) * time.Hour)
		shardTableName := baseTableName + "_" + date.Format("20060102")

		// Create the sharded table
		err := driver.db.WithContext(ctx).Exec(fmt.Sprintf(`
            CREATE TABLE IF NOT EXISTS %s (
                id TEXT PRIMARY KEY,
                name TEXT,
                value INTEGER,
                category TEXT,
                created_at TIMESTAMP
            )
        `, shardTableName)).Error
		require.NoError(t, err, "Failed to create sharded table")

		// Insert test data into the sharded table
		for j := 1; j <= 3; j++ {
			id := model.NewObjectID()
			err := driver.db.WithContext(ctx).Exec(fmt.Sprintf(`
                INSERT INTO %s (id, name, value, category, created_at)
                VALUES (?, ?, ?, ?, ?)
            `, shardTableName), id.Hex(), fmt.Sprintf("Shard %d-%d", i, j), j*10,
				fmt.Sprintf("Category %d", j), date).Error
			require.NoError(t, err, "Failed to insert test data")
		}
	}

	// Create a query with date sharding
	query := model.DBM{
		"_date_sharding": "created_at",
		"created_at": model.DBM{
			"$gte": startDate,
			"$lte": endDate,
		},
		"category": "Category 1",
	}

	// Create a base DB query
	db := driver.db.WithContext(ctx).Table(baseTableName)

	// Apply the translateQuery function
	testObj := &TestObject{}
	translatedDB := driver.translateQuery(db, query, testObj)

	// Execute the query and check the result count
	var results []TestObject
	err := translatedDB.Find(&results).Error
	require.NoError(t, err, "Query execution should not fail")

	// We should have 4 results (one from each day's shard for Category 1)
	assert.Equal(t, 4, len(results), "Query should return data from all shards")

	// Verify the results are from different shards
	dateMap := make(map[string]bool)
	for _, result := range results {
		dateStr := result.CreatedAt.Format("2006-01-02")
		dateMap[dateStr] = true
	}
	assert.Equal(t, 4, len(dateMap), "Results should come from 4 different dates/shards")

	// Clean up the sharded tables
	for i := 0; i <= 3; i++ {
		date := startDate.Add(time.Duration(i*24) * time.Hour)
		shardTableName := baseTableName + "_" + date.Format("20060102")
		err := driver.db.WithContext(ctx).Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", shardTableName)).Error
		require.NoError(t, err, "Failed to drop sharded table")
	}
}
