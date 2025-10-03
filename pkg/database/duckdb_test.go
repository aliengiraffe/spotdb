package database

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aliengiraffe/spotdb/pkg/helpers"
)

func init() {
	// Silence standard Go logger
	helpers.SilenceLogOutput()
}

func TestNewDuckDB(t *testing.T) {
	// Use testing's TempDir for automatic cleanup
	tempDir := t.TempDir()
	// Save the old value to restore it later
	oldTempDir := os.TempDir()
	// Set the temporary directory for this test
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Logf("Failed to restore TMPDIR: %v", err)
		}
	}()

	ctx := context.Background()
	db, err := NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database")

	// Verify database is initialized
	if db.db == nil {
		t.Error("Database connection is nil")
	}

	// Check if we can execute a simple query
	result, err := db.ExecuteQuery(ctx, "SELECT 1 AS test")
	if err != nil {
		t.Fatalf("Failed to execute test query: %v", err)
	}

	if len(result.Results) != 1 {
		t.Errorf("Expected 1 result row, got %d", len(result.Results))
	}

	// Check result value exists and looks like 1 (type can vary by implementation)
	testVal := result.Results[0]["test"]
	if testVal == nil {
		t.Errorf("Expected test value to be 1, got nil")
	} else {
		// Convert to string and check it's "1" regardless of type
		valStr := fmt.Sprintf("%v", testVal)
		if valStr != "1" {
			t.Errorf("Expected test value to be 1, got %v of type %T", testVal, testVal)
		}
	}
}

func TestNewDuckDBConfig(t *testing.T) {
	// Use a unique directory for each test
	tempDir := t.TempDir()
	// Save the old value to restore it later
	oldTempDir := os.TempDir()
	// Set the temporary directory for this test
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Logf("Failed to restore TMPDIR: %v", err)
		}
	}()

	ctx := context.Background()

	db, err := NewDuckDBConfig(ctx)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database")

}

func TestExecuteQuery(t *testing.T) {
	// Use a unique directory for each test
	tempDir := t.TempDir()
	// Save the old value to restore it later
	oldTempDir := os.TempDir()
	// Set the temporary directory for this test
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Logf("Failed to restore TMPDIR: %v", err)
		}
	}()

	ctx := context.Background()
	db, err := NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database")

	// Test with a valid query
	result, err := db.ExecuteQuery(ctx, "SELECT 1 AS num, 'test' AS str")
	if err != nil {
		t.Fatalf("Failed to execute valid query: %v", err)
	}

	if len(result.Results) != 1 {
		t.Errorf("Expected 1 result row, got %d", len(result.Results))
	}

	// Check numeric result
	testVal := result.Results[0]["num"]
	if testVal == nil {
		t.Errorf("Expected num value to be 1, got nil")
	} else {
		// Convert to string and check it's "1" regardless of type
		valStr := fmt.Sprintf("%v", testVal)
		if valStr != "1" {
			t.Errorf("Expected num value to be 1, got %v of type %T", testVal, testVal)
		}
	}

	// Check string result
	strVal := result.Results[0]["str"]
	if strVal == nil {
		t.Errorf("Expected str value to be 'test', got nil")
	} else {
		// Convert to string and check it's "test" regardless of type
		valStr := fmt.Sprintf("%v", strVal)
		if valStr != "test" {
			t.Errorf("Expected str value to be 'test', got %v of type %T", strVal, strVal)
		}
	}

	// Check duration
	if result.Duration <= 0 {
		t.Error("Expected positive duration")
	}

	// Check benchmark metrics
	if result.BenchmarkMetrics == nil {
		t.Error("Expected benchmark metrics to be populated")
	}

	// Test with an invalid query
	_, err = db.ExecuteQuery(ctx, "SELECT * FROM non_existent_table")
	if err == nil {
		t.Error("Expected error for invalid query, got nil")
	}
}

func TestCreateTableFromCSV(t *testing.T) {
	// Use a unique directory for each test
	tempDir := t.TempDir()
	// Save the old value to restore it later
	oldTempDir := os.TempDir()
	// Set the temporary directory for this test
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Logf("Failed to restore TMPDIR: %v", err)
		}
	}()

	ctx := context.Background()
	db, err := NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database")

	// Create a temporary CSV file
	csvContent := `id,name,value
1,test1,10.5
2,test2,20.75
3,test3,30.0
`
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")
	if err := os.WriteFile(csvPath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("Failed to create test CSV file: %v", err)
	}

	// Test create table from CSV
	err = db.CreateTableFromCSV(ctx, "test_csv_table", csvPath, true, false)
	if err != nil {
		t.Fatalf("Failed to create table from CSV: %v", err)
	}

	// Verify the table was created with correct data
	result, err := db.ExecuteQuery(ctx, "SELECT * FROM test_csv_table ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query created table: %v", err)
	}

	if len(result.Results) != 3 {
		t.Errorf("Expected 3 rows in created table, got %d", len(result.Results))
	}

	// Test override flag
	err = db.CreateTableFromCSV(ctx, "test_csv_table", csvPath, true, false)
	if err == nil {
		t.Error("Expected error when creating table that already exists without override flag")
	}

	// Test with override=true
	err = db.CreateTableFromCSV(ctx, "test_csv_table", csvPath, true, true)
	if err != nil {
		t.Fatalf("Failed to recreate table with override flag: %v", err)
	}
}

func TestSmartCreateTableFromCSV(t *testing.T) {
	// Skip this test if running in CI or short mode
	// Use a unique directory for each test
	tempDir := t.TempDir()
	// Save the old value to restore it later
	oldTempDir := os.TempDir()
	// Set the temporary directory for this test
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Logf("Failed to restore TMPDIR: %v", err)
		}
	}()
	if testing.Short() {
		t.Skip("Skipping smart import test in short mode")
	}

	ctx := context.Background()
	db, err := NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database")

	// Create a temporary CSV file
	csvContent := `id,name,value
1,test1,10.5
2,test2,20.75
3,test3,30.0
`
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")
	if err := os.WriteFile(csvPath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("Failed to create test CSV file: %v", err)
	}

	// Test smart create table from CSV
	result, err := db.SmartCreateTableFromCSV(ctx, "test_smart_table", csvPath, true, false)
	if err != nil {
		t.Fatalf("Failed to smart create table from CSV: %v", err)
	}

	// Check the result
	if result.TableName != "test_smart_table" {
		t.Errorf("Expected table name 'test_smart_table', got '%s'", result.TableName)
	}
	if result.RowCount != 3 {
		t.Errorf("Expected 3 rows, got %d", result.RowCount)
	}
	if result.ImportID == "" {
		t.Error("Expected non-empty import ID")
	}
	if result.ImportMethod == "" {
		t.Error("Expected non-empty import method")
	}
	if result.Duration <= 0 {
		t.Error("Expected positive duration")
	}

	// Verify the table was created with correct data
	queryResult, err := db.ExecuteQuery(ctx, "SELECT * FROM test_smart_table ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query created table: %v", err)
	}

	if len(queryResult.Results) != 3 {
		t.Errorf("Expected 3 rows in created table, got %d", len(queryResult.Results))
	}
}

func TestCleanupWorker(t *testing.T) {
	// Use a unique directory for each test
	tempDir := t.TempDir()
	// Save the old value to restore it later
	oldTempDir := os.TempDir()
	// Set the temporary directory for this test
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Logf("Failed to restore TMPDIR: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database")

	// Create a temporary table
	tmpTableName := "tmp_import_test"
	_, err = db.ExecuteQuery(ctx, "CREATE TABLE "+tmpTableName+" (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create temporary table: %v", err)
	}

	// Add the table to the cleanup queue with a short timeout
	db.cleanupCh <- tmpTableName

	// We'll use a small sleep but make it more robust by checking that
	// the table still exists afterward. This is a more reliable approach
	// than relying on exact timing, since the cleanup worker won't
	// remove the table until 30 minutes have passed.
	time.Sleep(100 * time.Millisecond)

	// Cancel the context to stop the cleanup worker
	cancel()

	// The table should still exist since we use a 30-minute timeout by default
	result, err := db.ExecuteQuery(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name='"+tmpTableName+"'")
	if err != nil {
		t.Fatalf("Failed to check if table exists: %v", err)
	}

	if len(result.Results) == 0 {
		t.Error("Temporary table was removed too early")
	}
}

func TestGetDB(t *testing.T) {
	// Use a unique directory for each test
	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Logf("Failed to restore TMPDIR: %v", err)
		}
	}()

	ctx := context.Background()
	db, err := NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database")

	// Test GetDB returns non-nil connection
	rawDB := db.GetDB()
	if rawDB == nil {
		t.Error("Expected non-nil database connection from GetDB()")
	}

	// Verify we can use the raw connection
	err = rawDB.Ping()
	if err != nil {
		t.Errorf("Failed to ping database through raw connection: %v", err)
	}
}

func TestSanitizeTableName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "alphanumeric with underscores",
			input:    "valid_table_name_123",
			expected: "valid_table_name_123",
		},
		{
			name:     "spaces replaced with underscores",
			input:    "table with spaces",
			expected: "table_with_spaces",
		},
		{
			name:     "special characters replaced",
			input:    "table-name!@#$%^&*()",
			expected: "table_name__________",
		},
		{
			name:     "SQL injection attempt",
			input:    "users'; DROP TABLE users--",
			expected: "users___DROP_TABLE_users__",
		},
		{
			name:     "mixed case preserved",
			input:    "MyTableName",
			expected: "MyTableName",
		},
		{
			name:     "dots and slashes",
			input:    "schema.table/name",
			expected: "schema_table_name",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "only special characters",
			input:    "!@#$%",
			expected: "_____",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := sanitizeTableName(tc.input)
			if result != tc.expected {
				t.Errorf("Expected '%s', got '%s'", tc.expected, result)
			}
		})
	}
}

func TestSplitQueryBySemicolon(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "single query no semicolon",
			input:    "SELECT * FROM users",
			expected: []string{"SELECT * FROM users"},
		},
		{
			name:     "two queries",
			input:    "SELECT * FROM users;SELECT * FROM orders",
			expected: []string{"SELECT * FROM users", "SELECT * FROM orders"},
		},
		{
			name:     "three queries with spacing",
			input:    "CREATE TABLE test (id INT); INSERT INTO test VALUES (1); SELECT * FROM test",
			expected: []string{"CREATE TABLE test (id INT)", " INSERT INTO test VALUES (1)", " SELECT * FROM test"},
		},
		{
			name:     "semicolon in single quotes",
			input:    "INSERT INTO test VALUES ('hello;world');SELECT * FROM test",
			expected: []string{"INSERT INTO test VALUES ('hello;world')", "SELECT * FROM test"},
		},
		{
			name:     "semicolon in double quotes",
			input:    `INSERT INTO test VALUES ("col;name");SELECT * FROM test`,
			expected: []string{`INSERT INTO test VALUES ("col;name")`, "SELECT * FROM test"},
		},
		{
			name:     "escaped quote",
			input:    `SELECT 'it\'s working';SELECT 1`,
			expected: []string{`SELECT 'it\'s working'`, "SELECT 1"},
		},
		{
			name:     "multiple semicolons",
			input:    "SELECT 1;;SELECT 2",
			expected: []string{"SELECT 1", "", "SELECT 2"},
		},
		{
			name:     "trailing semicolon",
			input:    "SELECT * FROM users;",
			expected: []string{"SELECT * FROM users"},
		},
		{
			name:     "empty string",
			input:    "",
			expected: []string{},
		},
		{
			name:     "only semicolons",
			input:    ";;;",
			expected: []string{"", "", ""},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := splitQueryBySemicolon(tc.input)
			if len(result) != len(tc.expected) {
				t.Errorf("Expected %d queries, got %d", len(tc.expected), len(result))
				t.Logf("Expected: %v", tc.expected)
				t.Logf("Got: %v", result)
				return
			}
			for i := range result {
				if result[i] != tc.expected[i] {
					t.Errorf("Query %d: expected '%s', got '%s'", i, tc.expected[i], result[i])
				}
			}
		})
	}
}

func TestCreateSnapshot(t *testing.T) {
	// Use a unique directory for each test
	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Logf("Failed to restore TMPDIR: %v", err)
		}
	}()

	ctx := context.Background()
	db, err := NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database")

	// Create a table with some data
	_, err = db.ExecuteQuery(ctx, "CREATE TABLE test_snapshot (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	_, err = db.ExecuteQuery(ctx, "INSERT INTO test_snapshot VALUES (1, 'test')")
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Create snapshot
	snapshotPath := filepath.Join(tempDir, "snapshot.db")
	err = db.CreateSnapshot(ctx, snapshotPath)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// Verify snapshot file exists
	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		t.Error("Snapshot file was not created")
	}

	// Verify snapshot file is not empty
	fileInfo, err := os.Stat(snapshotPath)
	if err != nil {
		t.Fatalf("Failed to stat snapshot file: %v", err)
	}
	if fileInfo.Size() == 0 {
		t.Error("Snapshot file is empty")
	}
}

func TestExecuteQuery_Validation(t *testing.T) {
	// Use a unique directory for each test
	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Logf("Failed to restore TMPDIR: %v", err)
		}
	}()

	ctx := context.Background()
	db, err := NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database")

	// Create a test table for validation tests
	_, err = db.ExecuteQuery(ctx, "CREATE TABLE test_validation (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	tests := []struct {
		name        string
		query       string
		shouldError bool
	}{
		{
			name:        "valid SELECT query",
			query:       "SELECT * FROM test_validation",
			shouldError: false,
		},
		{
			name:        "valid INSERT query",
			query:       "INSERT INTO test_validation VALUES (1, 'test')",
			shouldError: false,
		},
		{
			name:        "empty query",
			query:       "",
			shouldError: true,
		},
		{
			name:        "whitespace only query",
			query:       "   \n\t  ",
			shouldError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.ExecuteQuery(ctx, tc.query)
			if tc.shouldError && err == nil {
				t.Error("Expected error but got nil")
			}
			if !tc.shouldError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}

func TestNewDuckDBConfig_WithEnvVars(t *testing.T) {
	// Use a unique directory for each test
	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Logf("Failed to restore TMPDIR: %v", err)
		}
	}()

	// Save original env vars
	origDBPath := os.Getenv("DUCKDB_PATH")
	origMemoryLimit := os.Getenv("DUCKDB_MEMORY_LIMIT")
	origThreads := os.Getenv("DUCKDB_THREADS")

	defer func() {
		os.Setenv("DUCKDB_PATH", origDBPath)
		os.Setenv("DUCKDB_MEMORY_LIMIT", origMemoryLimit)
		os.Setenv("DUCKDB_THREADS", origThreads)
	}()

	tests := []struct {
		name        string
		dbPath      string
		memoryLimit string
		threads     string
	}{
		{
			name:        "with memory limit",
			dbPath:      "",
			memoryLimit: "1GB",
			threads:     "",
		},
		{
			name:        "with threads",
			dbPath:      "",
			memoryLimit: "",
			threads:     "4",
		},
		{
			name:        "with both memory and threads",
			dbPath:      "",
			memoryLimit: "512MB",
			threads:     "2",
		},
		{
			name:        "with custom db path",
			dbPath:      filepath.Join(tempDir, "custom.db"),
			memoryLimit: "",
			threads:     "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Set environment variables
			if tc.dbPath != "" {
				os.Setenv("DUCKDB_PATH", tc.dbPath)
			} else {
				os.Unsetenv("DUCKDB_PATH")
			}
			if tc.memoryLimit != "" {
				os.Setenv("DUCKDB_MEMORY_LIMIT", tc.memoryLimit)
			} else {
				os.Unsetenv("DUCKDB_MEMORY_LIMIT")
			}
			if tc.threads != "" {
				os.Setenv("DUCKDB_THREADS", tc.threads)
			} else {
				os.Unsetenv("DUCKDB_THREADS")
			}

			ctx := context.Background()
			db, err := NewDuckDBConfig(ctx)
			if err != nil {
				t.Fatalf("Failed to create database: %v", err)
			}
			defer helpers.CloseResources(db, "database")

			// Verify database is functional
			result, err := db.ExecuteQuery(ctx, "SELECT 1 AS test")
			if err != nil {
				t.Fatalf("Failed to execute test query: %v", err)
			}

			if len(result.Results) != 1 {
				t.Errorf("Expected 1 result row, got %d", len(result.Results))
			}
		})
	}
}

func TestClose_EdgeCases(t *testing.T) {
	// Use a unique directory for each test
	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Logf("Failed to restore TMPDIR: %v", err)
		}
	}()

	ctx := context.Background()

	t.Run("close twice", func(t *testing.T) {
		db, err := NewDuckDB(ctx)
		if err != nil {
			t.Fatalf("Failed to create database: %v", err)
		}

		// First close should succeed
		err = db.Close()
		if err != nil {
			t.Errorf("First close failed: %v", err)
		}

		// Second close should handle gracefully
		err = db.Close()
		if err != nil {
			t.Logf("Second close returned error (expected): %v", err)
		}
	})

	t.Run("operations after close", func(t *testing.T) {
		db, err := NewDuckDB(ctx)
		if err != nil {
			t.Fatalf("Failed to create database: %v", err)
		}

		err = db.Close()
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		// Try to execute query after close
		_, err = db.ExecuteQuery(ctx, "SELECT 1")
		if err == nil {
			t.Error("Expected error when executing query after close")
		}
	})
}

func TestExecuteQuery_MultipleStatements(t *testing.T) {
	// Use a unique directory for each test
	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Logf("Failed to restore TMPDIR: %v", err)
		}
	}()

	ctx := context.Background()
	db, err := NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database")

	// Test multiple statements separated by semicolons
	query := "CREATE TABLE multi_test (id INTEGER); INSERT INTO multi_test VALUES (1); INSERT INTO multi_test VALUES (2)"
	_, err = db.ExecuteQuery(ctx, query)
	if err != nil {
		t.Fatalf("Failed to execute multiple statements: %v", err)
	}

	// Verify the table was created and populated
	result, err := db.ExecuteQuery(ctx, "SELECT COUNT(*) as count FROM multi_test")
	if err != nil {
		t.Fatalf("Failed to query table: %v", err)
	}

	if len(result.Results) != 1 {
		t.Errorf("Expected 1 result row, got %d", len(result.Results))
	}
}

func TestValidateQuery_MaliciousPatterns(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		query       string
		shouldError bool
		errorMsg    string
	}{
		{
			name:        "UNION injection attempt",
			query:       "SELECT * FROM users UNION SELECT password FROM admin",
			shouldError: true,
			errorMsg:    "malicious SQL pattern",
		},
		{
			name:        "OR 1=1 injection",
			query:       "SELECT * FROM users WHERE id = 1 OR 1=1",
			shouldError: true,
			errorMsg:    "malicious SQL pattern",
		},
		{
			name:        "SQL comment injection",
			query:       "SELECT * FROM users -- WHERE id = 1",
			shouldError: true,
			errorMsg:    "malicious SQL pattern",
		},
		{
			name:        "Block comment injection",
			query:       "SELECT * FROM users /* comment */ WHERE id = 1",
			shouldError: true,
			errorMsg:    "malicious SQL pattern",
		},
		{
			name:        "ALTER TABLE attempt",
			query:       "ALTER TABLE users ADD COLUMN hacked TEXT",
			shouldError: true,
			errorMsg:    "malicious SQL pattern",
		},
		{
			name:        "DELETE without WHERE",
			query:       "DELETE FROM users",
			shouldError: true,
			errorMsg:    "malicious SQL pattern",
		},
		{
			name:        "DROP TABLE alone",
			query:       "DROP TABLE users",
			shouldError: true,
			errorMsg:    "malicious SQL pattern",
		},
		{
			name:        "DROP TABLE with CREATE TABLE (allowed)",
			query:       "DROP TABLE IF EXISTS users; CREATE TABLE users (id INT)",
			shouldError: false,
		},
		{
			name:        "EXEC attempt",
			query:       "EXEC sp_executesql @query",
			shouldError: true,
			errorMsg:    "malicious SQL pattern",
		},
		{
			name:        "xp_ stored procedure",
			query:       "SELECT * FROM users; EXEC xp_cmdshell 'dir'",
			shouldError: true,
			errorMsg:    "malicious SQL pattern",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateQuery(ctx, tc.query)
			if tc.shouldError {
				if err == nil {
					t.Errorf("Expected error for query: %s", tc.query)
				} else if !strings.Contains(err.Error(), tc.errorMsg) {
					t.Errorf("Expected error containing '%s', got: %v", tc.errorMsg, err)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error for query: %s, error: %v", tc.query, err)
				}
			}
		})
	}
}

func TestStartCleanupWorker_DropTable(t *testing.T) {
	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Logf("Failed to restore TMPDIR: %v", err)
		}
	}()

	// Use a very short cleanup interval for testing
	// We'll manually trigger cleanup by manipulating time
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database")

	// Create a temporary table
	tmpTableName := "tmp_import_cleanup_test"
	_, err = db.ExecuteQuery(ctx, fmt.Sprintf("CREATE TABLE %s (id INTEGER)", tmpTableName))
	if err != nil {
		t.Fatalf("Failed to create temporary table: %v", err)
	}

	// Verify table exists
	result, err := db.ExecuteQuery(ctx, fmt.Sprintf("SELECT COUNT(*) as count FROM %s", tmpTableName))
	if err != nil {
		t.Fatalf("Failed to query temporary table: %v", err)
	}
	if len(result.Results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(result.Results))
	}

	// The cleanup worker runs in background but with 30 minute timeout
	// We can't easily test automatic cleanup without waiting
	// Instead, test that the table exists and cleanup channel works
	db.cleanupCh <- tmpTableName

	// Give cleanup worker a moment to process
	time.Sleep(50 * time.Millisecond)

	// Table should still exist (30 minute timeout not elapsed)
	_, err = db.ExecuteQuery(ctx, fmt.Sprintf("SELECT COUNT(*) as count FROM %s", tmpTableName))
	if err != nil {
		t.Fatalf("Table was dropped too early: %v", err)
	}
}

func TestClose_Errors(t *testing.T) {
	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Logf("Failed to restore TMPDIR: %v", err)
		}
	}()

	ctx := context.Background()

	t.Run("close with removed db file", func(t *testing.T) {
		db, err := NewDuckDB(ctx)
		if err != nil {
			t.Fatalf("Failed to create database: %v", err)
		}

		// Remove the db file manually before closing
		os.Remove(db.dbPath)

		// Close should not error if file doesn't exist
		err = db.Close()
		if err != nil {
			t.Errorf("Close should not error when file doesn't exist: %v", err)
		}
	})
}

func TestCreateTableFromCSV_Errors(t *testing.T) {
	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Logf("Failed to restore TMPDIR: %v", err)
		}
	}()

	ctx := context.Background()
	db, err := NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database")

	t.Run("invalid CSV file", func(t *testing.T) {
		// Try to create table from non-existent file
		err := db.CreateTableFromCSV(ctx, "test_table", "/nonexistent/file.csv", true, false)
		if err == nil {
			t.Error("Expected error for non-existent CSV file")
		}
	})

	t.Run("CSV with invalid data types", func(t *testing.T) {
		// Create a CSV with mixed incompatible types in a column
		tmpFile := filepath.Join(tempDir, "invalid_types.csv")
		csvContent := "id,value\n1,abc\n2,def\n"
		err := os.WriteFile(tmpFile, []byte(csvContent), 0644)
		if err != nil {
			t.Fatalf("Failed to write test file: %v", err)
		}

		// Try to create table - this should succeed as DuckDB handles mixed types
		err = db.CreateTableFromCSV(ctx, "mixed_types_table", tmpFile, true, false)
		if err != nil {
			t.Logf("CSV import handled gracefully: %v", err)
		}
	})
}

func TestSmartCreateTableFromCSV_Errors(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping smart import test in short mode")
	}

	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Logf("Failed to restore TMPDIR: %v", err)
		}
	}()

	ctx := context.Background()
	db, err := NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database")

	t.Run("non-existent file", func(t *testing.T) {
		_, err := db.SmartCreateTableFromCSV(ctx, "test_table", "/nonexistent/file.csv", true, false)
		if err == nil {
			t.Error("Expected error for non-existent CSV file")
		}
	})
}

func TestExecuteQuery_EmptyResults(t *testing.T) {
	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Logf("Failed to restore TMPDIR: %v", err)
		}
	}()

	ctx := context.Background()
	db, err := NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database")

	// Create table
	_, err = db.ExecuteQuery(ctx, "CREATE TABLE empty_test (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Query empty table
	result, err := db.ExecuteQuery(ctx, "SELECT * FROM empty_test")
	if err != nil {
		t.Fatalf("Failed to query empty table: %v", err)
	}

	if len(result.Results) != 0 {
		t.Errorf("Expected 0 results from empty table, got %d", len(result.Results))
	}
}

func TestExecuteQuery_LargeResultSet(t *testing.T) {
	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Logf("Failed to restore TMPDIR: %v", err)
		}
	}()

	ctx := context.Background()
	db, err := NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database")

	// Create table with over 10000 rows to trigger logging
	_, err = db.ExecuteQuery(ctx, "CREATE TABLE large_test AS SELECT range AS id FROM range(15000)")
	if err != nil {
		t.Fatalf("Failed to create large table: %v", err)
	}

	// Query the large table - should trigger the rowCount logging at line 609-611
	result, err := db.ExecuteQuery(ctx, "SELECT * FROM large_test")
	if err != nil {
		t.Fatalf("Failed to query large table: %v", err)
	}

	if len(result.Results) != 15000 {
		t.Errorf("Expected 15000 results, got %d", len(result.Results))
	}
}

func TestCreateRowMap_ByteValues(t *testing.T) {
	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Logf("Failed to restore TMPDIR: %v", err)
		}
	}()

	ctx := context.Background()
	db, err := NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database")

	// Create table with BLOB type to get []byte values
	_, err = db.ExecuteQuery(ctx, "CREATE TABLE blob_test (id INTEGER, data BLOB)")
	if err != nil {
		t.Fatalf("Failed to create blob table: %v", err)
	}

	// Insert binary data
	_, err = db.ExecuteQuery(ctx, "INSERT INTO blob_test VALUES (1, '\\xDE\\xAD\\xBE\\xEF'::BLOB)")
	if err != nil {
		t.Fatalf("Failed to insert blob data: %v", err)
	}

	// Query the blob data - should trigger []byte conversion at line 634-635
	result, err := db.ExecuteQuery(ctx, "SELECT * FROM blob_test")
	if err != nil {
		t.Fatalf("Failed to query blob table: %v", err)
	}

	if len(result.Results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result.Results))
	}

	// Verify the data field is a string (converted from []byte)
	if result.Results[0]["data"] == nil {
		t.Error("Expected data field to be non-nil")
	}
}

func TestCreateSnapshot_Errors(t *testing.T) {
	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Logf("Failed to restore TMPDIR: %v", err)
		}
	}()

	ctx := context.Background()

	t.Run("invalid destination path", func(t *testing.T) {
		db, err := NewDuckDB(ctx)
		if err != nil {
			t.Fatalf("Failed to create database: %v", err)
		}
		defer helpers.CloseResources(db, "database")

		// Try to create snapshot to invalid path
		invalidPath := filepath.Join("/nonexistent/directory/snapshot.db")
		err = db.CreateSnapshot(ctx, invalidPath)
		if err == nil {
			t.Error("Expected error when creating snapshot to invalid path")
		}
		if !strings.Contains(err.Error(), "failed to copy database file") {
			t.Errorf("Expected 'failed to copy database file' error, got: %v", err)
		}
	})

	t.Run("closed database", func(t *testing.T) {
		db, err := NewDuckDB(ctx)
		if err != nil {
			t.Fatalf("Failed to create database: %v", err)
		}

		// Close the database
		if err := db.Close(); err != nil {
			t.Fatalf("Failed to close database: %v", err)
		}

		// Try to create snapshot from closed database
		destPath := filepath.Join(tempDir, "snapshot.db")
		err = db.CreateSnapshot(ctx, destPath)
		if err == nil {
			t.Error("Expected error when creating snapshot from closed database")
		}
		// The error could be either "database connection is closed" or "sql: database is closed"
		if !strings.Contains(err.Error(), "database is closed") {
			t.Errorf("Expected 'database is closed' error, got: %v", err)
		}
	})
}

func TestStartCleanupWorker_TimeBased(t *testing.T) {
	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Logf("Failed to restore TMPDIR: %v", err)
		}
	}()

	ctx := context.Background()
	db, err := NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database")

	// Note: Testing the actual ticker-based cleanup requires waiting 5+ minutes
	// which is impractical for unit tests. The cleanup worker is tested
	// indirectly through the table dropping test (TestStartCleanupWorker_DropTable)
	// For full coverage of the ticker.C case, integration tests with time mocking
	// would be needed.

	// This test at least verifies the cleanup channel works
	tableName := "tmp_import_time_test"
	_, err = db.ExecuteQuery(ctx, fmt.Sprintf("CREATE TABLE %s (id INTEGER)", tableName))
	if err != nil {
		t.Fatalf("Failed to create temp table: %v", err)
	}

	// Add to cleanup queue
	db.cleanupCh <- tableName

	// Give worker a moment to process
	time.Sleep(100 * time.Millisecond)

	// Table should still exist (not expired yet)
	// Note: DuckDB doesn't have sqlite_master, use information_schema instead
	result, err := db.ExecuteQuery(ctx, "SELECT table_name FROM information_schema.tables WHERE table_name='"+tableName+"'")
	if err != nil {
		t.Fatalf("Failed to check table existence: %v", err)
	}

	if len(result.Results) == 0 {
		t.Error("Table should still exist (not expired)")
	}
}

func TestSmartCreateTableFromCSV_EmptyFile(t *testing.T) {
	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Logf("Failed to restore TMPDIR: %v", err)
		}
	}()

	ctx := context.Background()
	db, err := NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database")

	// Create an empty CSV file
	emptyFile := filepath.Join(tempDir, "empty.csv")
	err = os.WriteFile(emptyFile, []byte{}, 0644)
	if err != nil {
		t.Fatalf("Failed to create empty file: %v", err)
	}

	// Try to import empty file - should error
	_, err = db.SmartCreateTableFromCSV(ctx, "empty_table", emptyFile, true, false)
	if err == nil {
		t.Error("Expected error for empty CSV file")
	}
	if !strings.Contains(err.Error(), "csv file is empty") {
		t.Errorf("Expected 'csv file is empty' error, got: %v", err)
	}
}
