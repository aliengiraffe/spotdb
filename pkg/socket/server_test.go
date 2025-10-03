package socket

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aliengiraffe/spotdb/pkg/database"
	"github.com/aliengiraffe/spotdb/pkg/helpers"
	"github.com/gorilla/websocket"
)

func init() {
	// Silence standard Go logger
	helpers.SilenceLogOutput()
}

func TestNewServer(t *testing.T) {
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

	// Test server creation
	server, err := NewServer(db, "localhost:8081")
	if err != nil {
		t.Fatalf("Failed to create socket server: %v", err)
	}

	if server == nil {
		t.Fatal("NewServer returned nil")
		return
	}
	if server.db != db {
		t.Error("Server has incorrect database reference")
	}
	if server.address != "localhost:8081" {
		t.Errorf("Expected address 'localhost:8081', got '%s'", server.address)
	}
	if server.connections == nil {
		t.Error("Server connections map is nil")
	}
	if server.wsConnections == nil {
		t.Error("Server WebSocket connections map is nil")
	}
}

func TestSetAuthHandler(t *testing.T) {
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

	// Create a server
	server, err := NewServer(db, "localhost:8081")
	if err != nil {
		t.Fatalf("Failed to create socket server: %v", err)
	}

	// Verify initial state: default authHandler should be set
	if server.authHandler == nil {
		t.Error("Expected authHandler to be set initially")
	}

	// Set auth handler
	mockHandler := func(next http.Handler) http.Handler {
		return next
	}
	server.SetAuthHandler(mockHandler)

	// Verify handler was updated by SetAuthHandler
	if server.authHandler == nil {
		t.Error("Expected authHandler to be set by SetAuthHandler, got nil")
	}
}

// TestSocketAPIKeyMiddleware_NoKey tests apiKeyMiddleware when API_KEY is set but no or wrong key is provided
func TestSocketAPIKeyMiddleware_NoKey(t *testing.T) {
	// Set expected API key in environment
	orig := os.Getenv("API_KEY")
	defer os.Setenv("API_KEY", orig)
	os.Setenv("API_KEY", "secretkey")

	// Base handler to verify invocation
	called := false
	baseHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("ok")); err != nil {
			t.Fatalf("failed to write response: %v", err)
		}
	})
	wrapped := apiKeyMiddleware(baseHandler)

	// Case 1: no header
	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("Expected status %d, got %d", http.StatusUnauthorized, rec.Code)
	}
	if called {
		t.Error("Expected base handler not to be called when no API key provided")
	}
	if !strings.Contains(rec.Body.String(), "Unauthorized: invalid API key") {
		t.Errorf("Unexpected response body: %q", rec.Body.String())
	}

	// Case 2: wrong key
	req = httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-API-Key", "wrongkey")
	rec = httptest.NewRecorder()
	called = false
	wrapped.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("Expected status %d for wrong key, got %d", http.StatusUnauthorized, rec.Code)
	}
	if called {
		t.Error("Expected base handler not to be called when wrong API key provided")
	}

	// Case 3: correct key
	req = httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-API-Key", "secretkey")
	rec = httptest.NewRecorder()
	called = false
	wrapped.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("Expected status %d for correct key, got %d", http.StatusOK, rec.Code)
	}
	if !called {
		t.Error("Expected base handler to be called when correct API key provided")
	}
}

func TestProcessRequest(t *testing.T) {
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

	// Create a server
	server, err := NewServer(db, "localhost:8081")
	if err != nil {
		t.Fatalf("Failed to create socket server: %v", err)
	}

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

	tests := []struct {
		name       string
		reqType    string
		query      string
		expectFail bool
	}{
		{"valid query", "query", "SELECT * FROM test_table", false},
		{"empty query", "query", "", true},
		{"invalid query", "query", "SELECT * FROM non_existent_table", true},
		{"unknown type", "invalid_type", "", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := WSEvent{
				Type:  tc.reqType,
				Query: tc.query,
			}

			resp := server.processRequest(context.Background(), req)

			if tc.expectFail {
				if resp.Status != "error" {
					t.Errorf("Expected status 'error', got '%s'", resp.Status)
				}
				if resp.Error == "" {
					t.Error("Expected non-empty error message")
				}
			} else {
				if resp.Status != "success" {
					t.Errorf("Expected status 'success', got '%s': %s", resp.Status, resp.Error)
				}
				if resp.Results == nil {
					t.Error("Expected non-nil results")
				}
				if len(resp.Results) == 0 {
					t.Error("Expected non-empty results")
				}
			}
		})
	}
}

// TestWebSocketHandler tests the WebSocket handler.
// This is a more complex test that requires setting up a WebSocket client.
func TestWebSocketHandler(t *testing.T) {
	// Skip in short mode as WebSocket tests can be flaky
	if testing.Short() {
		t.Skip("Skipping WebSocket test in short mode")
	}

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

	// Create a server with a test HTTP server
	server, err := NewServer(db, "localhost:0")
	if err != nil {
		t.Fatalf("Failed to create socket server: %v", err)
	}

	// Create HTTP test server
	handler := http.HandlerFunc(server.handleWebSocket)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	// Convert HTTP URL to WebSocket URL
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	// Connect to the WebSocket server
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Failed to connect to WebSocket server: %v", err)
	}
	defer helpers.CloseResources(conn, "WebSocket connection")

	// Send a query request
	queryReq := WSEvent{
		Type:  "query",
		Query: "SELECT * FROM test_table",
	}
	if err := conn.WriteJSON(queryReq); err != nil {
		t.Fatalf("Failed to send query request: %v", err)
	}

	// Read the response
	var resp Response
	if err := conn.ReadJSON(&resp); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// Verify the response
	if resp.Status != "success" {
		t.Errorf("Expected status 'success', got '%s': %s", resp.Status, resp.Error)
	}
	if resp.Results == nil {
		t.Error("Expected non-nil results")
	}
	if len(resp.Results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(resp.Results))
	}

	// Send an invalid request
	invalidReq := WSEvent{
		Type:  "query",
		Query: "SELECT * FROM non_existent_table",
	}
	if err := conn.WriteJSON(invalidReq); err != nil {
		t.Fatalf("Failed to send invalid request: %v", err)
	}

	// Read the response
	if err := conn.ReadJSON(&resp); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// Verify the response
	if resp.Status != "error" {
		t.Errorf("Expected status 'error', got '%s'", resp.Status)
	}
	if resp.Error == "" {
		t.Error("Expected non-empty error message")
	}
}

func TestStartAndStop(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping start/stop test in short mode")
	}

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

	// Create a server
	server, err := NewServer(db, "localhost:0")
	if err != nil {
		t.Fatalf("Failed to create socket server: %v", err)
	}

	// Start the server in a goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a channel to signal server started
	started := make(chan struct{})
	go func() {
		// Signal that we're starting
		close(started)
		logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
		if err := server.Start(ctx, logger); err != nil && err != http.ErrServerClosed {
			t.Errorf("Server.Start error: %v", err)
		}
	}()

	// Wait for goroutine to start
	<-started
	// Brief pause to allow server initialization
	time.Sleep(10 * time.Millisecond)

	// Stop the server
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	server.Stop(logger)

	// Verify that the server was stopped
	if server.httpServer == nil {
		t.Error("Expected httpServer to be non-nil")
	}
	// Try making a request to verify server is not accepting connections
	_, err = http.Get(fmt.Sprintf("http://%s/", server.address))
	if err == nil {
		t.Error("Expected error connecting to stopped server, got nil")
	}
}

func TestResponseJSONMarshaling(t *testing.T) {
	// Create a Response struct
	resp := Response{
		Status: "success",
		Results: []map[string]any{
			{"id": 1, "name": "test1"},
			{"id": 2, "name": "test2"},
		},
	}

	// Marshal to JSON
	jsonData, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("Failed to marshal response: %v", err)
	}

	// Unmarshal back to check structure
	var respUnmarshaled Response
	if err := json.Unmarshal(jsonData, &respUnmarshaled); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	// Verify fields
	if respUnmarshaled.Status != resp.Status {
		t.Errorf("Expected status '%s', got '%s'", resp.Status, respUnmarshaled.Status)
	}
	if len(respUnmarshaled.Results) != len(resp.Results) {
		t.Errorf("Expected %d results, got %d", len(resp.Results), len(respUnmarshaled.Results))
	}
}
