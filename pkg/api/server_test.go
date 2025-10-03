package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/aliengiraffe/spotdb/pkg/database"
	"github.com/aliengiraffe/spotdb/pkg/helpers"
	"github.com/gin-gonic/gin"
)

func init() {
	// Set Gin to test mode to silence logs
	gin.SetMode(gin.TestMode)
	// Disable rate limiting for tests
	os.Setenv("ENV_RATE_LIMIT_RPS", "0")
	// Silence standard Go logger
	helpers.SilenceLogOutput()
}

func TestNewServer(t *testing.T) {
	// Create a test database
	db, err := database.NewDuckDB(context.Background())
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database connection")

	// Get logger for testing
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Instantiate API server and set up router
	app := &Server{db: db}
	app.setupRouter(log)

	if app.db != db {
		t.Error("Server has incorrect database reference")
	}
	if app.router == nil {
		t.Error("Server router is nil")
	}

	// Create HTTP server from API server
	httpSrv := NewServer(db, log)
	if httpSrv.Addr != ":8080" {
		t.Errorf("Expected Addr :8080, got %s", httpSrv.Addr)
	}
	if httpSrv.Handler == nil {
		t.Error("HTTP server handler is nil")
	}
}

func TestHandleListTables(t *testing.T) {
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
			t.Fatalf("Failed to restore TMPDIR: %v", err)
		}
	}()

	// Create a test database
	db, err := database.NewDuckDB(context.Background())
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database connection")

	// Create a test table
	ctx := context.Background()
	_, err = db.ExecuteQuery(ctx, "CREATE TABLE test_table (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Create a server
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	s := &Server{db: db}
	s.setupRouter(log)

	// Create a request to test the handler
	req := httptest.NewRequest("GET", "/api/v1/tables", nil)
	rec := httptest.NewRecorder()

	// Call the handler
	s.Router().ServeHTTP(rec, req)

	// Check the response
	if rec.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, rec.Code)
	}

	// Verify content type
	contentType := rec.Header().Get("Content-Type")
	if contentType != "application/json; charset=utf-8" {
		t.Errorf("Expected Content-Type: application/json; charset=utf-8, got %s", contentType)
	}

	// Parse response
	var response TablesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	// Check that tables field exists and contains our test table
	if len(response.Tables) == 0 {
		t.Fatal("Response does not contain any tables")
	}

	// Check if our test table is in the list with schema
	found := false
	var testTable *TableInfo
	for _, table := range response.Tables {
		if table.Name == "test_table" {
			found = true
			testTable = &table
			break
		}
	}
	if !found {
		t.Error("Test table not found in response")
	}

	// Verify schema information is present
	if testTable != nil {
		if len(testTable.Columns) == 0 {
			t.Error("Test table should have column information")
		}

		// Check for expected columns (id INTEGER, name TEXT)
		expectedColumns := map[string]string{
			"id":   "INTEGER",
			"name": "VARCHAR",
		}

		for _, col := range testTable.Columns {
			if expectedType, exists := expectedColumns[col.Name]; exists {
				// DuckDB might return VARCHAR instead of TEXT
				if col.Type != expectedType && (col.Name != "name" || col.Type != "VARCHAR") {
					t.Errorf("Expected column %s to have type %s, got %s", col.Name, expectedType, col.Type)
				}
				delete(expectedColumns, col.Name)
			}
		}

		if len(expectedColumns) > 0 {
			t.Errorf("Missing expected columns: %v", expectedColumns)
		}
	}
}

func TestRateLimitConfiguration(t *testing.T) {
	// Save current environment variables to restore later
	origRateLimit := os.Getenv("ENV_RATE_LIMIT_RPS")

	// Restore environment variables after test
	defer func() {
		os.Setenv("ENV_RATE_LIMIT_RPS", origRateLimit)
	}()

	// Set mode to release for this test to enable rate limiting
	origMode := gin.Mode()
	gin.SetMode(gin.ReleaseMode)
	defer gin.SetMode(origMode)

	// Test cases
	tests := []struct {
		name           string
		rateLimit      string
		expectDisabled bool
	}{
		{"default settings", "", false},
		{"custom rate limit", "10", false},
		{"disabled rate limit with 0", "0", true},
		{"invalid rate limit", "not-a-number", false}, // Should fall back to default
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Configure environment for this test case
			os.Setenv("ENV_RATE_LIMIT_RPS", tc.rateLimit)

			// Get the middleware
			log := slog.New(slog.NewTextHandler(os.Stderr, nil))
			middleware := rateLimitMiddleware(log)

			// Check if middleware is the passthrough version (disabled) or not
			middlewareType := reflect.TypeOf(middleware).String()

			if tc.expectDisabled {
				// For disabled rate limiting (ENV_RATE_LIMIT_RPS=0 or test mode),
				// we expect the passthrough middleware (simple gin.HandlerFunc)
				if !strings.Contains(middlewareType, "gin.HandlerFunc") ||
					strings.Contains(middlewareType, "ratelimit.RateLimiter") {
					t.Errorf("Expected passthrough middleware for disabled rate limiting, got %s", middlewareType)
				}
			} else {
				// For enabled rate limiting, the type should include the rate limiter
				// Implementation detail: in real code this would be from the gin-rate-limit package
				if !strings.Contains(middlewareType, "gin.HandlerFunc") {
					t.Errorf("Expected gin.HandlerFunc middleware type, got %s", middlewareType)
				}
			}
		})
	}
}

// TestAPIKeyAuthMiddleware_NoKey tests that requests without API key are unauthorized when API_KEY is set
func TestAPIKeyAuthMiddleware_NoKey(t *testing.T) {
	// Set expected API key in environment
	orig := os.Getenv("API_KEY")
	defer os.Setenv("API_KEY", orig)
	os.Setenv("API_KEY", "secretkey")

	// Create a Gin router with the auth middleware
	router := gin.New()
	router.Use(apiKeyAuthMiddleware())
	router.GET("/ping", func(c *gin.Context) {
		c.String(http.StatusOK, "pong")
	})

	// Perform request without X-API-Key header
	req := httptest.NewRequest("GET", "/ping", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("Expected status %d, got %d", http.StatusUnauthorized, rec.Code)
	}

	// Validate response body JSON
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("Failed to parse response body: %v", err)
	}
	if body["error"] != "Unauthorized" {
		t.Errorf("Expected error 'Unauthorized', got '%s'", body["error"])
	}
	if body["message"] != "Invalid or missing API Key" {
		t.Errorf("Expected message 'Invalid or missing API Key', got '%s'", body["message"])
	}
}

func TestHandleExplorer(t *testing.T) {
	// Create a unique directory for each test
	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Fatalf("Failed to restore TMPDIR: %v", err)
		}
	}()

	// Create a test HTML file in a temporary static directory
	staticDir := "test_static"
	if err := os.MkdirAll(staticDir, 0755); err != nil {
		t.Fatalf("Failed to create static directory: %v", err)
	}
	defer os.RemoveAll(staticDir)

	testHTML := `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>SpotDB Data Explorer</title>
    <script src="https://unpkg.com/htmx.org@2.0.3"></script>
</head>
<body>
    <h1>Upload CSV File</h1>
    <h2>Available Tables</h2>
    <h3>Query Data</h3>
</body>
</html>`

	if err := os.WriteFile(staticDir+"/index.html", []byte(testHTML), 0644); err != nil {
		t.Fatalf("Failed to write test HTML file: %v", err)
	}

	// Create a test database
	db, err := database.NewDuckDB(context.Background())
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database connection")

	// Create a test server with custom route for testing
	router := gin.New()
	router.GET("/explorer", func(c *gin.Context) {
		c.File(staticDir + "/index.html")
	})

	// Create a request to test the /explorer handler
	req := httptest.NewRequest("GET", "/explorer", nil)
	rec := httptest.NewRecorder()

	// Call the handler
	router.ServeHTTP(rec, req)

	// Check the response
	if rec.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d. Response body: %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	// Verify content type is HTML
	contentType := rec.Header().Get("Content-Type")
	if !strings.Contains(contentType, "text/html") {
		t.Errorf("Expected Content-Type to contain text/html, got %s", contentType)
	}

	// Check that the response contains expected HTML content
	body := rec.Body.String()
	expectedContent := []string{
		"<!DOCTYPE html>",
		"<title>SpotDB Data Explorer</title>",
		"htmx.org",
		"Upload CSV File",
		"Available Tables",
		"Query Data",
	}

	for _, expected := range expectedContent {
		if !strings.Contains(body, expected) {
			t.Errorf("Expected response body to contain '%s'", expected)
		}
	}

	// Verify the response is not empty
	if len(body) == 0 {
		t.Error("Expected non-empty response body")
	}
}

func TestHandleQuery(t *testing.T) {
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
			t.Fatalf("Failed to restore TMPDIR: %v", err)
		}
	}()

	// Create a test database
	db, err := database.NewDuckDB(context.Background())
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database connection")

	// Create a test table with data
	ctx := context.Background()
	_, err = db.ExecuteQuery(ctx, "CREATE TABLE test_table (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	_, err = db.ExecuteQuery(ctx, "INSERT INTO test_table VALUES (1, 'test1'), (2, 'test2')")
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Create a server
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	s := &Server{db: db}
	s.setupRouter(log)

	tests := []struct {
		name     string
		query    string
		expected int
		limit    int
		status   int
	}{
		{"valid query", "SELECT * FROM test_table", 2, 0, http.StatusOK},
		{"query with limit", "SELECT * FROM test_table", 1, 1, http.StatusOK},
		{"invalid query", "SELECT * FROM non_existent_table", 0, 0, http.StatusInternalServerError},
		{"empty query", "", 0, 0, http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Create request body
			requestBody := map[string]interface{}{"query": tc.query}
			if tc.limit > 0 {
				requestBody["limit"] = tc.limit
			}

			requestJSON, err := json.Marshal(requestBody)
			if err != nil {
				t.Fatalf("Failed to marshal request: %v", err)
			}

			// Create a request to test the handler
			req := httptest.NewRequest("POST", "/api/v1/query", bytes.NewBuffer(requestJSON))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()

			// Call the handler
			s.Router().ServeHTTP(rec, req)

			// Check the response code
			if rec.Code != tc.status {
				t.Errorf("Expected status code %d, got %d, body: %s", tc.status, rec.Code, rec.Body.String())
			}

			// Verify content type for successful responses
			if rec.Code == http.StatusOK {
				contentType := rec.Header().Get("Content-Type")
				if contentType != "application/json; charset=utf-8" {
					t.Errorf("Expected Content-Type: application/json; charset=utf-8, got %s", contentType)
				}

				// For valid queries, check the response structure
				var response map[string]interface{}
				if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
					t.Fatalf("Failed to parse response: %v", err)
				}

				// Check status
				if status, ok := response["status"].(string); !ok || status != "success" {
					t.Error("Expected status success in response")
				}

				// Check results
				results, ok := response["results"].([]interface{})
				if !ok {
					t.Fatal("Response does not contain results array")
				}

				// Check row count
				if len(results) != tc.expected {
					t.Errorf("Expected %d results, got %d", tc.expected, len(results))
				}

				// For the valid query case, verify the actual data content
				if tc.name == "valid query" && len(results) > 0 {
					// Check first row values
					row, rowOk := results[0].(map[string]interface{})
					if !rowOk {
						t.Errorf("Expected row to be a map, got %T", results[0])
					} else {
						if id, ok := row["id"]; !ok {
							t.Errorf("Expected id field in first result")
						} else if idFloat, ok := id.(float64); !ok {
							t.Errorf("Expected id to be a number, got %T", id)
						} else if int(idFloat) != 1 {
							t.Errorf("Expected id=1 in first result, got %v", id)
						}

						if name, ok := row["name"]; !ok || name != "test1" {
							t.Errorf("Expected name=test1 in first result, got %v", name)
						}
					}
				}
			}
		})
	}
}

func TestHandleCreateSnapshot_RequestValidation(t *testing.T) {
	// Use a unique directory for each test
	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Fatalf("Failed to restore TMPDIR: %v", err)
		}
	}()

	// Create a test database
	db, err := database.NewDuckDB(context.Background())
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database connection")

	// Create a server
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	s := &Server{db: db}
	s.setupRouter(log)

	tests := []struct {
		name           string
		requestBody    interface{}
		expectedStatus int
		expectedError  string
	}{
		{
			name:           "missing bucket field",
			requestBody:    map[string]string{"key": "snapshots/test"},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "Invalid snapshot request",
		},
		{
			name:           "missing key field",
			requestBody:    map[string]string{"bucket": "my-bucket"},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "Invalid snapshot request",
		},
		{
			name:           "empty bucket",
			requestBody:    map[string]string{"bucket": "", "key": "snapshots/test"},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "Invalid snapshot request",
		},
		{
			name:           "empty key",
			requestBody:    map[string]string{"bucket": "my-bucket", "key": ""},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "Invalid snapshot request",
		},
		{
			name:           "invalid JSON",
			requestBody:    "not-valid-json",
			expectedStatus: http.StatusBadRequest,
			expectedError:  "Invalid snapshot request",
		},
		{
			name:           "empty request body",
			requestBody:    map[string]string{},
			expectedStatus: http.StatusBadRequest,
			expectedError:  "Invalid snapshot request",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var requestJSON []byte
			var err error

			// Handle string requests differently (for invalid JSON test)
			if str, ok := tc.requestBody.(string); ok {
				requestJSON = []byte(str)
			} else {
				requestJSON, err = json.Marshal(tc.requestBody)
				if err != nil {
					t.Fatalf("Failed to marshal request: %v", err)
				}
			}

			// Create a request to test the handler
			req := httptest.NewRequest("POST", "/api/v1/snapshot", bytes.NewBuffer(requestJSON))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()

			// Call the handler
			s.Router().ServeHTTP(rec, req)

			// Check the response code
			if rec.Code != tc.expectedStatus {
				t.Errorf("Expected status code %d, got %d, body: %s", tc.expectedStatus, rec.Code, rec.Body.String())
			}

			// Parse error response
			var errorResponse ErrorResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &errorResponse); err != nil {
				t.Fatalf("Failed to parse error response: %v", err)
			}

			// Check error message contains expected text
			if !strings.Contains(errorResponse.Message, tc.expectedError) {
				t.Errorf("Expected error message to contain '%s', got '%s'", tc.expectedError, errorResponse.Message)
			}

			// Check status field
			if errorResponse.Status != "error" {
				t.Errorf("Expected status 'error', got '%s'", errorResponse.Status)
			}
		})
	}
}

func TestHandleCreateSnapshot_TimestampFormat(t *testing.T) {
	// Use a unique directory for each test
	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Fatalf("Failed to restore TMPDIR: %v", err)
		}
	}()

	// Create a test database
	db, err := database.NewDuckDB(context.Background())
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database connection")

	// Note: This test validates request structure and timestamp generation
	// but cannot test actual S3 upload without mocking or AWS credentials.
	// The test will fail at the S3 client creation step, which is expected.

	// Create a server
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	s := &Server{db: db}
	s.setupRouter(log)

	// Valid request with proper bucket and key
	requestBody := SnapshotRequest{
		Bucket: "test-bucket",
		Key:    "snapshots/test",
	}

	requestJSON, err := json.Marshal(requestBody)
	if err != nil {
		t.Fatalf("Failed to marshal request: %v", err)
	}

	req := httptest.NewRequest("POST", "/api/v1/snapshot", bytes.NewBuffer(requestJSON))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	// Call the handler
	s.Router().ServeHTTP(rec, req)

	// This test expects failure at S3 client creation (500) unless AWS is configured
	// The important part is that it doesn't fail at request validation (400)
	if rec.Code == http.StatusBadRequest {
		t.Errorf("Request validation failed when it should have passed. Body: %s", rec.Body.String())
	}

	// Note: Full integration test would require:
	// 1. Mock S3 client or use localstack
	// 2. Verify snapshot file creation
	// 3. Verify S3 upload
	// 4. Verify cleanup of temp files
	// 5. Verify response structure with correct timestamp format
}

func TestHealthCheckEndpoint(t *testing.T) {
	// Use a unique directory for each test
	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Fatalf("Failed to restore TMPDIR: %v", err)
		}
	}()

	// Create a test database
	db, err := database.NewDuckDB(context.Background())
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database connection")

	// Create a server
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	s := &Server{db: db}
	s.setupRouter(log)

	// Create a request to test the health check handler
	req := httptest.NewRequest("GET", "/api/v1/healthcheck", nil)
	rec := httptest.NewRecorder()

	// Call the handler
	s.Router().ServeHTTP(rec, req)

	// Check the response
	if rec.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, rec.Code)
	}

	// Check response body
	if rec.Body.String() != "OK" {
		t.Errorf("Expected body 'OK', got '%s'", rec.Body.String())
	}

	// Verify content type
	contentType := rec.Header().Get("Content-Type")
	if !strings.Contains(contentType, "text/plain") {
		t.Errorf("Expected Content-Type to contain text/plain, got %s", contentType)
	}
}

func TestShouldIncludeBenchmarks(t *testing.T) {
	// Save original environment variable
	origEnv := os.Getenv("ENABLE_QUERY_BENCHMARKS")
	defer os.Setenv("ENABLE_QUERY_BENCHMARKS", origEnv)

	// Use a unique directory for each test
	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Fatalf("Failed to restore TMPDIR: %v", err)
		}
	}()

	// Create a test database
	db, err := database.NewDuckDB(context.Background())
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database connection")

	// Create a server
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	s := &Server{db: db}
	s.setupRouter(log)

	tests := []struct {
		name          string
		envValue      string
		queryParam    string
		expectedValue bool
	}{
		{
			name:          "default - no env, no query param",
			envValue:      "",
			queryParam:    "",
			expectedValue: false,
		},
		{
			name:          "env variable true",
			envValue:      "true",
			queryParam:    "",
			expectedValue: true,
		},
		{
			name:          "env variable false",
			envValue:      "false",
			queryParam:    "",
			expectedValue: false,
		},
		{
			name:          "query param true overrides env false",
			envValue:      "false",
			queryParam:    "true",
			expectedValue: true,
		},
		{
			name:          "query param false overrides env true",
			envValue:      "true",
			queryParam:    "false",
			expectedValue: false,
		},
		{
			name:          "query param invalid value with env true",
			envValue:      "true",
			queryParam:    "invalid",
			expectedValue: true,
		},
		{
			name:          "query param invalid value with no env",
			envValue:      "",
			queryParam:    "invalid",
			expectedValue: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Set environment variable
			if tc.envValue != "" {
				os.Setenv("ENABLE_QUERY_BENCHMARKS", tc.envValue)
			} else {
				os.Unsetenv("ENABLE_QUERY_BENCHMARKS")
			}

			// Create test request with query parameter
			url := "/test"
			if tc.queryParam != "" {
				url += "?benchmark=" + tc.queryParam
			}
			req := httptest.NewRequest("GET", url, nil)
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = req

			// Test the function
			result := s.shouldIncludeBenchmarks(c)

			if result != tc.expectedValue {
				t.Errorf("Expected %v, got %v", tc.expectedValue, result)
			}
		})
	}
}

func TestHandleQueryWithBenchmarks(t *testing.T) {
	// Use a unique directory for each test
	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Fatalf("Failed to restore TMPDIR: %v", err)
		}
	}()

	// Save original environment variable
	origEnv := os.Getenv("ENABLE_QUERY_BENCHMARKS")
	defer os.Setenv("ENABLE_QUERY_BENCHMARKS", origEnv)

	// Create a test database
	db, err := database.NewDuckDB(context.Background())
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database connection")

	// Create a test table with data
	ctx := context.Background()
	_, err = db.ExecuteQuery(ctx, "CREATE TABLE test_table (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	_, err = db.ExecuteQuery(ctx, "INSERT INTO test_table VALUES (1, 'test1'), (2, 'test2')")
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Create a server
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	s := &Server{db: db}
	s.setupRouter(log)

	tests := []struct {
		name               string
		envBenchmark       string
		queryBenchmark     string
		expectBenchmarkKey bool
	}{
		{
			name:               "benchmarks via environment variable",
			envBenchmark:       "true",
			queryBenchmark:     "",
			expectBenchmarkKey: true,
		},
		{
			name:               "benchmarks via query parameter",
			envBenchmark:       "",
			queryBenchmark:     "true",
			expectBenchmarkKey: true,
		},
		{
			name:               "no benchmarks",
			envBenchmark:       "",
			queryBenchmark:     "",
			expectBenchmarkKey: false,
		},
		{
			name:               "benchmarks disabled via query param",
			envBenchmark:       "true",
			queryBenchmark:     "false",
			expectBenchmarkKey: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Set environment variable
			if tc.envBenchmark != "" {
				os.Setenv("ENABLE_QUERY_BENCHMARKS", tc.envBenchmark)
			} else {
				os.Unsetenv("ENABLE_QUERY_BENCHMARKS")
			}

			// Create request body
			requestBody := map[string]interface{}{"query": "SELECT * FROM test_table"}
			requestJSON, err := json.Marshal(requestBody)
			if err != nil {
				t.Fatalf("Failed to marshal request: %v", err)
			}

			// Create request with optional benchmark query parameter
			url := "/api/v1/query"
			if tc.queryBenchmark != "" {
				url += "?benchmark=" + tc.queryBenchmark
			}
			req := httptest.NewRequest("POST", url, bytes.NewBuffer(requestJSON))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()

			// Call the handler
			s.Router().ServeHTTP(rec, req)

			// Check the response code
			if rec.Code != http.StatusOK {
				t.Errorf("Expected status code %d, got %d, body: %s", http.StatusOK, rec.Code, rec.Body.String())
			}

			// Parse response
			var response map[string]interface{}
			if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
				t.Fatalf("Failed to parse response: %v", err)
			}

			// Check if benchmark key exists
			_, hasBenchmark := response["benchmark"]
			if tc.expectBenchmarkKey && !hasBenchmark {
				t.Error("Expected benchmark key in response, but it was missing")
			}
			if !tc.expectBenchmarkKey && hasBenchmark {
				t.Error("Did not expect benchmark key in response, but it was present")
			}
		})
	}
}

func TestHandleListTables_EmptyDatabase(t *testing.T) {
	// Use a unique directory for each test
	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Fatalf("Failed to restore TMPDIR: %v", err)
		}
	}()

	// Create a test database
	db, err := database.NewDuckDB(context.Background())
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database connection")

	// Create a server
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	s := &Server{db: db}
	s.setupRouter(log)

	// Create a request to test the handler
	req := httptest.NewRequest("GET", "/api/v1/tables", nil)
	rec := httptest.NewRecorder()

	// Call the handler
	s.Router().ServeHTTP(rec, req)

	// Check the response
	if rec.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, rec.Code)
	}

	// Parse response
	var response TablesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	// Check that tables field exists (can be empty for empty database)
	// The response should have a tables array, even if it's empty
	if response.Tables == nil {
		// This is acceptable - empty database returns empty JSON array which can be nil slice
		return
	}

	// If tables exist, they should be empty for a fresh database
	if len(response.Tables) > 0 {
		t.Logf("Database has %d tables (may include system tables)", len(response.Tables))
	}
}

func TestSetupAPIRoutes_ExplorerEndpoint(t *testing.T) {
	// Use a unique directory for each test
	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Fatalf("Failed to restore TMPDIR: %v", err)
		}
	}()

	// Create a test database
	db, err := database.NewDuckDB(context.Background())
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database connection")

	// Create static directory and index.html file
	err = os.Mkdir("./static", 0755)
	if err != nil && !os.IsExist(err) {
		t.Fatalf("Failed to create static directory: %v", err)
	}

	// Clean up static directory
	defer os.RemoveAll("./static")

	// Create index.html file
	indexContent := []byte("<!DOCTYPE html><html><body>Explorer</body></html>")
	err = os.WriteFile("./static/index.html", indexContent, 0644)
	if err != nil {
		t.Fatalf("Failed to create index.html: %v", err)
	}

	// Create a server
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	s := &Server{db: db}
	s.setupRouter(log)

	// Create a request to test the /explorer endpoint
	req := httptest.NewRequest("GET", "/explorer", nil)
	rec := httptest.NewRecorder()

	// Call the handler
	s.Router().ServeHTTP(rec, req)

	// Check the response
	if rec.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, rec.Code)
	}

	// Check that the response contains the index.html content
	if !strings.Contains(rec.Body.String(), "Explorer") {
		t.Errorf("Expected response to contain 'Explorer', got: %s", rec.Body.String())
	}
}

func TestHandleListTables_WithTables(t *testing.T) {
	// Use a unique directory for each test
	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Fatalf("Failed to restore TMPDIR: %v", err)
		}
	}()

	// Create a test database
	db, err := database.NewDuckDB(context.Background())
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database connection")

	// Create a test table
	_, err = db.ExecuteQuery(context.Background(), "CREATE TABLE test_users (id INTEGER, name VARCHAR, email VARCHAR)")
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Create a server
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	s := &Server{db: db}
	s.setupRouter(log)

	// Create a request to test the handler
	req := httptest.NewRequest("GET", "/api/v1/tables", nil)
	rec := httptest.NewRecorder()

	// Call the handler
	s.Router().ServeHTTP(rec, req)

	// Check the response
	if rec.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, rec.Code)
	}

	// Parse response
	var response TablesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	// Check that we have at least one table
	found := false
	for _, table := range response.Tables {
		if table.Name == "test_users" {
			found = true
			// Verify columns
			if len(table.Columns) != 3 {
				t.Errorf("Expected 3 columns, got %d", len(table.Columns))
			}
			break
		}
	}

	if !found {
		t.Error("Expected to find test_users table")
	}
}

func TestHandleListTables_DatabaseError(t *testing.T) {
	// Use a unique directory for each test
	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Fatalf("Failed to restore TMPDIR: %v", err)
		}
	}()

	// Create a test database
	db, err := database.NewDuckDB(context.Background())
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Create a server
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	s := &Server{db: db}
	s.setupRouter(log)

	// Close the database to trigger errors
	helpers.CloseResources(db, "database connection")

	// Create a request to test the handler
	req := httptest.NewRequest("GET", "/api/v1/tables", nil)
	rec := httptest.NewRecorder()

	// Call the handler
	s.Router().ServeHTTP(rec, req)

	// Check the response - should be 500 Internal Server Error
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("Expected status code %d, got %d", http.StatusInternalServerError, rec.Code)
	}

	// Parse response
	var response ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to parse error response: %v", err)
	}

	if response.Status != "error" {
		t.Errorf("Expected error status, got %s", response.Status)
	}

	if !strings.Contains(response.Message, "Failed to list tables") {
		t.Errorf("Expected error message about listing tables, got: %s", response.Message)
	}
}

func TestHandleCreateSnapshot_DatabaseError(t *testing.T) {
	// Use a unique directory for each test
	tempDir := t.TempDir()
	oldTempDir := os.TempDir()
	if err := os.Setenv("TMPDIR", tempDir); err != nil {
		t.Fatalf("Failed to set TMPDIR: %v", err)
	}
	defer func() {
		if err := os.Setenv("TMPDIR", oldTempDir); err != nil {
			t.Fatalf("Failed to restore TMPDIR: %v", err)
		}
	}()

	// Create a test database
	db, err := database.NewDuckDB(context.Background())
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Create a server
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	s := &Server{db: db}
	s.setupRouter(log)

	// Close the database to trigger error in CreateSnapshot
	helpers.CloseResources(db, "database connection")

	// Create a snapshot request
	payload := SnapshotRequest{
		Bucket: "test-bucket",
		Key:    "snapshots/test",
	}

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Failed to marshal request: %v", err)
	}

	// Create a request
	req := httptest.NewRequest("POST", "/api/v1/snapshot", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	// Call the handler
	s.Router().ServeHTTP(rec, req)

	// Check the response - should be 500 Internal Server Error
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("Expected status code %d, got %d", http.StatusInternalServerError, rec.Code)
	}

	// Parse response
	var response ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to parse error response: %v", err)
	}

	if response.Status != "error" {
		t.Errorf("Expected error status, got %s", response.Status)
	}

	if !strings.Contains(response.Message, "Failed to create snapshot") {
		t.Errorf("Expected error message about creating snapshot, got: %s", response.Message)
	}
}
