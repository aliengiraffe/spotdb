//go:build !integration
// +build !integration

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/aliengiraffe/spotdb/pkg/database"
)

// TestUploadEndpointDirect tests the /api/v1/upload endpoint with direct import
func TestUploadEndpointDirect(t *testing.T) {
	// Initialize in-memory database
	ctx := context.Background()
	db, err := database.NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("failed to create DuckDB: %v", err)
	}
	defer db.Close() // Ensure cleanup
	// Create server without API key auth
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	server := &Server{db: db}
	server.setupRouter(log)

	// Prepare simple CSV content
	csvData := []byte("col1,col2\n1,2\n")
	// Build multipart form
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	// Form fields
	tableName := "test_import"
	writer.WriteField("table_name", tableName)      // nolint:errcheck
	writer.WriteField("has_header", "true")         // nolint:errcheck
	writer.WriteField("override", "false")          // nolint:errcheck
	writer.WriteField("smart", "false")             // nolint:errcheck
	writer.WriteField("csv_file_encoding", "utf-8") // nolint:errcheck
	// File field
	part, err := writer.CreateFormFile("csv_file", "test.csv")
	if err != nil {
		t.Fatalf("failed to create form file: %v", err)
	}
	if _, err := part.Write(csvData); err != nil {
		t.Fatalf("failed to write csv data: %v", err)
	}
	writer.Close()

	// Create HTTP request
	req := httptest.NewRequest(http.MethodPost, "/api/v1/upload", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	// Perform request
	respRec := httptest.NewRecorder()
	server.router.ServeHTTP(respRec, req)

	// Expect HTTP 200 OK
	if respRec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", respRec.Code, respRec.Body.String())
	}
	// Parse response
	var resp CSVUploadResponse
	if err := json.Unmarshal(respRec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	// Validate response fields
	if resp.Table != tableName {
		t.Errorf("expected table %q, got %q", tableName, resp.Table)
	}
	if resp.RowCount != 1 {
		t.Errorf("expected RowCount 1, got %d", resp.RowCount)
	}
	if len(resp.Columns) == 0 {
		t.Error("expected at least one column in response")
	}
	if method, ok := resp.Import["import_method"]; !ok || method == "" {
		t.Error("expected import_method in response.Import")
	}
}

// TestUploadEndpointSmart tests the /api/v1/upload endpoint with smart import
func TestUploadEndpointSmart(t *testing.T) {
	// Initialize in-memory database
	ctx := context.Background()
	db, err := database.NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("failed to create DuckDB: %v", err)
	}
	defer db.Close() // Ensure cleanup
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	server := &Server{db: db}
	server.setupRouter(log)

	// Prepare simple CSV content
	csvData := []byte("col1,col2\n3,4\n")
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	// Form fields
	tableName := "smart_table"
	writer.WriteField("table_name", tableName)      // nolint:errcheck
	writer.WriteField("has_header", "true")         // nolint:errcheck
	writer.WriteField("override", "false")          // nolint:errcheck
	writer.WriteField("smart", "true")              // nolint:errcheck
	writer.WriteField("csv_file_encoding", "utf-8") // nolint:errcheck
	// File field (prefix 'test' to skip mime check)
	part, err := writer.CreateFormFile("csv_file", "test_smart.csv")
	if err != nil {
		t.Fatalf("failed to create form file: %v", err)
	}
	if _, err := part.Write(csvData); err != nil {
		t.Fatalf("failed to write csv data: %v", err)
	}
	writer.Close()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/upload", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp CSVUploadResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.Table != tableName {
		t.Errorf("expected table %q, got %q", tableName, resp.Table)
	}
	if resp.RowCount != 1 {
		t.Errorf("expected RowCount 1, got %d", resp.RowCount)
	}
}

// TestHandleCSVUploadMissingFile tests upload endpoint with missing file field
func TestHandleCSVUploadMissingFile(t *testing.T) {
	ctx := context.Background()
	db, err := database.NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("NewDuckDB failed: %v", err)
	}
	defer db.Close() // Ensure cleanup
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	server := &Server{db: db}
	server.setupRouter(log)
	// Build form without csv_file
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	writer.WriteField("table_name", "tbl") // nolint:errcheck
	writer.Close()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/upload", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", w.Code)
	}
	var resp CSVErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal error response: %v", err)
	}
	if len(resp.Errors) == 0 || resp.Errors[0].Code != "INVALID_REQUEST_PARAMETERS" {
		t.Errorf("expected INVALID_REQUEST_PARAMETERS error code, got %v", resp.Errors)
	}
}

// TestUploadEndpointInvalidMimeType tests endpoint with invalid MIME type
func TestUploadEndpointInvalidMimeType(t *testing.T) {
	// Initialize in-memory database
	ctx := context.Background()
	db, err := database.NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("failed to create DuckDB: %v", err)
	}
	defer db.Close() // Ensure cleanup
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	server := &Server{db: db}
	server.setupRouter(log)

	// Binary content to trigger invalid MIME type
	data := []byte{0x00, 0x01, 0x02}
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	// Required form fields
	writer.WriteField("table_name", "bad_mime")     // nolint:errcheck
	writer.WriteField("has_header", "false")        // nolint:errcheck
	writer.WriteField("override", "false")          // nolint:errcheck
	writer.WriteField("smart", "false")             // nolint:errcheck
	writer.WriteField("csv_file_encoding", "utf-8") // nolint:errcheck
	// File part with non-CSV name and binary data
	part, err := writer.CreateFormFile("csv_file", "upload.bin")
	if err != nil {
		t.Fatalf("failed to create form file: %v", err)
	}
	if _, err := part.Write(data); err != nil {
		t.Fatalf("failed to write binary data: %v", err)
	}
	writer.Close()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/upload", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", w.Code)
	}
	var resp CSVErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal error response: %v", err)
	}
	if len(resp.Errors) == 0 {
		t.Error("expected at least one validation error")
	}
	// Accept either invalid format or MIME detection error
	code := resp.Errors[0].Code
	if code != "INVALID_FILE_FORMAT" && code != "MIME_TYPE_DETECTION_ERROR" {
		t.Errorf("unexpected error code: %s", code)
	}
}
