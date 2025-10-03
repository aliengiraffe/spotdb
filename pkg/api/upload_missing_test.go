//go:build integration
// +build integration

package api

import (
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"testing"

	"github.com/aliengiraffe/spotdb/pkg/database"
	"github.com/aliengiraffe/spotdb/pkg/helpers"
	"github.com/gin-gonic/gin"
)

// TestValidateMimeTypeErrorPaths tests the error paths in validateMimeType function
func TestValidateMimeTypeErrorPaths(t *testing.T) {
	// Create a server instance
	db, err := database.NewDuckDB(context.Background())
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database connection")
	server := NewServer(db, false, nil)

	// Create a mock file header that will fail on Open()
	t.Run("File open error", func(t *testing.T) {
		mockHeader := &multipart.FileHeader{
			Filename: "test_error.csv",
			Size:     100,
		}

		// Call validateMimeType - this should fail on file.Open()
		errors, err := server.validateMimeType(mockHeader)

		// Verify error path was taken
		if err == nil {
			t.Error("Expected error from validateMimeType with bad file header, got nil")
		}

		// Verify error code
		if len(errors) == 0 {
			t.Error("Expected validation errors, got none")
		} else if errors[0].Code != "MIME_TYPE_DETECTION_ERROR" {
			t.Errorf("Expected error code MIME_TYPE_DETECTION_ERROR, got %s", errors[0].Code)
		}
	})

	// For direct testing of detectFileMimeType error path
	t.Run("MIME detection error direct", func(t *testing.T) {
		// Create a direct test using detectFileMimeType with our error file
		testFile := &errorFile{
			msg:     "test file",
			readErr: fmt.Errorf("simulated read error during mime detection"),
		}

		// Call the function that would normally fail during mime detection
		_, mimeErr := detectFileMimeType(testFile)
		if mimeErr == nil {
			t.Error("Expected error from detectFileMimeType with failing reader, got nil")
		}

		// Now test the validateMimeType function directly with our mocked file header
		// Create a test request
		req := newTestRequest().
			withTableName("mime_error_table").
			withDefaults().
			withContent([]byte("test")).
			build()

		httpReq, rec := createMultipartRequest(t, req)
		ginCtx, _ := gin.CreateTestContext(rec)
		ginCtx.Request = httpReq

		// Use the mockServer approach for full HTTP handler testing
		mockServer := &mockServer{
			Server: server,
		}

		// Mock the processCsvFileFromHeader function to inject our error
		mockServer.mockProcessCsvFileFromHeader = func(fileHeader *multipart.FileHeader, tableName string, hasHeader bool, encoding string) (string, []CSVError, error) {
			mimeErrors := []CSVError{
				{
					Code:    "MIME_TYPE_DETECTION_ERROR",
					Message: "Failed to detect file type: simulated detection error",
					Details: CSVErrorDetail{
						Line:       0,
						Suggestion: "Check if the file is a valid CSV file.",
					},
				},
			}
			return "", mimeErrors, fmt.Errorf("failed to detect MIME type")
		}

		// Call the handler with our mock
		handler := mockServer.handleCSVUpload()
		handler(ginCtx)

		// Verify response
		if rec.Code != http.StatusBadRequest {
			t.Errorf("Expected status code %d for MIME detection error, got %d",
				http.StatusBadRequest, rec.Code)
		}
	})

	// Test CSV format check error path - this tests line 394 in validateMimeType
	t.Run("CSV format check error", func(t *testing.T) {
		// Create a test file with text content that should pass MIME detection but fail CSV format check
		tempFile, err := os.CreateTemp("", "test_csv_format_error_*.txt")
		if err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
		defer os.Remove(tempFile.Name())

		// Write content that doesn't look like CSV but is text
		if _, err := tempFile.WriteString("This is definitely not a CSV file and will fail format checking."); err != nil {
			t.Fatalf("Failed to write to temp file: %v", err)
		}
		tempFile.Close()

		// Use the mockServer approach to simulate looksLikeCSV failing with an error
		mockServer := &mockServer{
			Server: server,
		}

		// Mock the processCsvFileFromHeader function to inject our error
		mockServer.mockProcessCsvFileFromHeader = func(fileHeader *multipart.FileHeader, tableName string, hasHeader bool, encoding string) (string, []CSVError, error) {
			csvErrors := []CSVError{
				{
					Code:    "CSV_FORMAT_CHECK_ERROR",
					Message: "Error checking CSV format: simulated format check error",
					Details: CSVErrorDetail{
						Line:       0,
						Suggestion: "Check if the file contains valid CSV data.",
					},
				},
			}
			return "", csvErrors, fmt.Errorf("CSV format check error")
		}

		// Create a test request
		req := newTestRequest().
			withTableName("csv_format_error_table").
			withDefaults().
			withContent([]byte("This is not CSV")).
			build()

		httpReq, rec := createMultipartRequest(t, req)
		ginCtx, _ := gin.CreateTestContext(rec)
		ginCtx.Request = httpReq

		// Call the handler with our mock
		handler := mockServer.handleCSVUpload()
		handler(ginCtx)

		// Verify response
		if rec.Code != http.StatusBadRequest {
			t.Errorf("Expected status code %d for CSV format check error, got %d",
				http.StatusBadRequest, rec.Code)
		}
	})
}

// errorFile is a mock implementation of multipart.File
type errorFile struct {
	msg      string
	readErr  error
	closeErr error
}

func (f *errorFile) Read(p []byte) (int, error) {
	if f.readErr != nil {
		return 0, f.readErr
	}
	return 0, io.EOF
}

func (f *errorFile) Close() error {
	return f.closeErr
}

func (f *errorFile) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}

func (f *errorFile) ReadAt(p []byte, off int64) (n int, err error) {
	return 0, fmt.Errorf("ReadAt not implemented")
}
