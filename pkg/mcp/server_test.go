package mcp

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/aliengiraffe/spotdb/pkg/database"
	"github.com/aliengiraffe/spotdb/pkg/helpers"
)

func init() {
	// Silence standard Go logger
	helpers.SilenceLogOutput()
}

// setupTestServer encapsulates common setup for tests: configures TMPDIR, creates a DuckDB, and returns an A10eServer and the DB.
func setupTestServer(t *testing.T) (*A10eServer, *database.DuckDB) {
	t.Helper()
	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Fatalf("Failed to restore TMPDIR: %v", err)
		}
	})

	db, err := database.NewDuckDB(context.Background())
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	t.Cleanup(func() {
		helpers.CloseResources(db, "database connection")
	})

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	server := NewA10eServer(db, logger)
	if server == nil {
		t.Fatal("NewA10eServer returned nil")
	}
	return server, db
}

func TestNewA10eServer(t *testing.T) {
	server, db := setupTestServer(t)
	if server.db != db {
		t.Error("Server has incorrect database reference")
	}
	if server.insights == nil {
		t.Error("Server insights slice is nil")
	}
	if len(server.insights) != 0 {
		t.Errorf("Server insights slice should be empty, has %d elements", len(server.insights))
	}
}

func TestExecuteQuery(t *testing.T) {
	server, db := setupTestServer(t)

	// Create a test table
	var err error
	ctx := context.Background()
	_, err = db.ExecuteQuery(ctx, "CREATE TABLE test_table (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	_, err = db.ExecuteQuery(ctx, "INSERT INTO test_table VALUES (1, 'test1'), (2, 'test2')")
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	tests := []struct {
		name           string
		query          string
		expectedStatus string
		expectedError  bool
	}{
		{"select query", "SELECT * FROM test_table", "success", false},
		{"ls command", "LS", "success", false},
		{"describe command", "DESCRIBE test_table", "success", false},
		{"load command", "LOAD test_table", "success", false},
		{"create table", "CREATE TABLE new_table (id INTEGER)", "success", false},
		{"invalid table", "SELECT * FROM non_existent_table", "error", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := server.ExecuteQuery(ctx, tc.query)

			if tc.expectedError {
				if err == nil {
					t.Error("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}

				if result == "" {
					t.Error("Expected non-empty result string")
				}
			}
		})
	}
}

func TestListTables(t *testing.T) {
	server, db := setupTestServer(t)

	// Create a test table
	var err error
	ctx := context.Background()
	_, err = db.ExecuteQuery(ctx, "CREATE TABLE test_table (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Test list tables
	result, err := server.listTables(ctx)
	if err != nil {
		t.Fatalf("Failed to list tables: %v", err)
	}

	// Check that the result contains our test table
	if !strings.Contains(result, "test_table") {
		t.Error("Expected list tables result to contain 'test_table'")
	}
}

func TestDescribeTable(t *testing.T) {
	server, db := setupTestServer(t)

	// Create a test table
	var err error
	ctx := context.Background()
	_, err = db.ExecuteQuery(ctx, "CREATE TABLE test_table (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Test describe table
	result, err := server.describeTable(ctx, "test_table")
	if err != nil {
		t.Fatalf("Failed to describe table: %v", err)
	}

	// Log the result for inspection
	t.Logf("Describe result: %s", result)

	// Check for minimum expected content without being too rigid about format
	if !strings.Contains(result, "id") {
		t.Error("Expected describe table result to contain column 'id'")
	}
	if !strings.Contains(result, "name") {
		t.Error("Expected describe table result to contain column 'name'")
	}

	// Test with a fake non-existent table (create via direct query to prevent error)
	_, err = server.describeTable(ctx, "fake_table")
	if err == nil {
		t.Error("Expected error for non-existent table")
	} else if !strings.Contains(err.Error(), "does not exist") {
		t.Error("Expected 'not found' message for non-existent table")
	}
}

func TestExecuteSelectQuery(t *testing.T) {
	server, db := setupTestServer(t)

	// Create a test table
	var err error
	ctx := context.Background()
	_, err = db.ExecuteQuery(ctx, "CREATE TABLE test_table (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	_, err = db.ExecuteQuery(ctx, "INSERT INTO test_table VALUES (1, 'test1'), (2, 'test2')")
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Test select query
	result, err := server.executeSelectQuery(ctx, "SELECT * FROM test_table")
	if err != nil {
		t.Fatalf("Failed to execute select query: %v", err)
	}

	// Check that the result contains the correct data
	if !strings.Contains(result, "test1") || !strings.Contains(result, "test2") {
		t.Error("Expected select query result to contain the test data")
	}

	// Verify row count using a regex that's more flexible about formatting
	rowCountPattern := regexp.MustCompile(`(?i).*\b2\s+rows?\b.*`)
	if !rowCountPattern.MatchString(result) {
		t.Error("Expected select query result to indicate 2 rows returned")
	}

	// Test with empty result
	result, err = server.executeSelectQuery(ctx, "SELECT * FROM test_table WHERE id = 999")
	if err != nil {
		t.Fatalf("Failed to execute select query with no results: %v", err)
	}
	// Don't check exact wording as separatorRow content may vary depending on DuckDB version
	t.Logf("Empty result output: %s", result)

}

func TestSynthesizeMemo(t *testing.T) {
	server, _ := setupTestServer(t)

	// Test with no insights
	memo := server.SynthesizeMemo()
	if !strings.Contains(memo, "No business insights") {
		t.Error("Expected memo to indicate no insights available")
	}

	// Add insights
	server.insights = []string{
		"Sales have increased by 15% in Q3.",
		"Customer retention is highest in the West region.",
	}

	// Test with insights
	memo = server.SynthesizeMemo()
	if !strings.Contains(memo, "Sales have increased") {
		t.Error("Expected memo to contain the first insight")
	}
	if !strings.Contains(memo, "Customer retention") {
		t.Error("Expected memo to contain the second insight")
	}
	if !strings.Contains(memo, "2 key business insights") {
		t.Error("Expected memo to mention the count of insights")
	}
}

func TestApiKeyMiddleware(t *testing.T) {
	// Save original env var and restore after test
	origApiKey := os.Getenv("API_KEY")
	defer func() {
		if err := os.Setenv("API_KEY", origApiKey); err != nil {
			t.Fatalf("Failed to restore API_KEY: %v", err)
		}
	}()

	// Set up a test handler
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("OK"))
		if err != nil {
			t.Fatalf("Failed to write response: %v", err)
		}
	})

	// Create a logger for middleware
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Test with API key disabled
	if err := os.Setenv("API_KEY", ""); err != nil {
		t.Fatalf("Failed to set API_KEY to empty: %v", err)
	}
	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	handlerFunc := apiKeyMiddleware(logger)
	handler := handlerFunc(testHandler)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("Expected status code %d when API key is disabled, got %d", http.StatusOK, rec.Code)
	}

	// Test with API key enabled but missing
	if err := os.Setenv("API_KEY", "test-key"); err != nil {
		t.Fatalf("Failed to set API_KEY: %v", err)
	}
	req = httptest.NewRequest("GET", "/", nil)
	rec = httptest.NewRecorder()
	handlerFunc = apiKeyMiddleware(logger)
	handler = handlerFunc(testHandler)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("Expected status code %d when API key is missing, got %d", http.StatusUnauthorized, rec.Code)
	}

	// Test with API key enabled and valid
	req = httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Api-Key", "test-key")
	rec = httptest.NewRecorder()
	handlerFunc = apiKeyMiddleware(logger)
	handler = handlerFunc(testHandler)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("Expected status code %d when API key is valid, got %d", http.StatusOK, rec.Code)
	}

	// Test with API key enabled but invalid
	req = httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Api-Key", "wrong-key")
	rec = httptest.NewRecorder()
	handlerFunc = apiKeyMiddleware(logger)
	handler = handlerFunc(testHandler)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("Expected status code %d when API key is invalid, got %d", http.StatusUnauthorized, rec.Code)
	}
}
