package api

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/aliengiraffe/spotdb/pkg/database"
	"github.com/aliengiraffe/spotdb/pkg/helpers"
)

// TestCopyFileDataSizeExceeded tests the file size exceeded error path in copyFileData
// This test directly triggers the helpers.ErrMaxFileSizeExceeded error in copyFileData
func TestCopyFileDataSizeExceeded(t *testing.T) {
	// Create a server instance
	ctx := context.Background()
	db, err := database.NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer helpers.CloseResources(db, "database connection")
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	server := &Server{db: db}
	server.setupRouter(log)

	// Create a temporary file for the test
	tempFile, err := os.CreateTemp("", "test_copy_file_data_*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	// Define a mock that always generates content
	// but never returns EOF, causing a size limit error
	mockSrc := &fileSizeExceeder{}

	// Save the original function and replace it for testing
	originalCopyWithMaxSize := helpers.CopyWithMaxSize
	defer func() { helpers.CopyWithMaxSize = originalCopyWithMaxSize }()

	// Replace the function to always return a file size exceeded error
	helpers.CopyWithMaxSize = func(dst io.Writer, src io.Reader, bufferSize int, maxSize int64, validateBuffer helpers.BufferValidationFunc) (int64, *helpers.ValidationIssue, error) {
		return 0, nil, helpers.ErrMaxFileSizeExceeded
	}

	// Call copyFileData
	errors, err := server.copyFileData(ctx, mockSrc, tempFile, "test.csv", "utf-8")

	// Check for the expected error
	if err == nil {
		t.Fatal("Expected error for file size exceeded, got nil")
	}
	if len(errors) == 0 {
		t.Fatal("Expected CSVErrors, got none")
	}
	if errors[0].Code != "FILE_SIZE_EXCEEDED" {
		t.Errorf("Expected FILE_SIZE_EXCEEDED error, got %s", errors[0].Code)
	}
}

// fileSizeExceeder is an implementation of multipart.File that always generates data
type fileSizeExceeder struct{}

func (f *fileSizeExceeder) Read(p []byte) (int, error) {
	// Fill the buffer with data to simulate a very large file
	for i := range p {
		p[i] = 'A'
	}
	return len(p), nil
}

func (f *fileSizeExceeder) Close() error {
	return nil
}

func (f *fileSizeExceeder) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}

func (f *fileSizeExceeder) ReadAt(p []byte, off int64) (n int, err error) {
	return 0, nil
}

// TestLooksLikeCSV tests the looksLikeCSV function directly
func TestLooksLikeCSV(t *testing.T) {
	tests := []struct {
		name     string
		content  []byte
		fileName string
		expected bool
	}{
		{
			name:     "Valid CSV data",
			content:  []byte("id,name,value\n1,test1,10.5\n2,test2,20.75\n3,test3,30.0\n"),
			fileName: "test.csv",
			expected: true,
		},
		{
			name:     "Invalid CSV data",
			content:  []byte("not a csv file content"),
			fileName: "notcsv.txt",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a buffer to simulate a file
			fileContent := bytes.NewReader(tt.content)

			// Create a test file
			mockFile := &mockFileFromBytes{
				data: fileContent,
				name: tt.fileName,
			}

			// Test the function
			ctx := context.Background()
			result, err := looksLikeCSV(ctx, mockFile)

			if err != nil {
				t.Fatalf("looksLikeCSV returned error: %v", err)
			}

			if result != tt.expected {
				t.Errorf("looksLikeCSV(%q) = %v, want %v",
					string(tt.content), result, tt.expected)
			}
		})
	}
}

// mockFileFromBytes implements multipart.File from a byte slice
type mockFileFromBytes struct {
	data *bytes.Reader
	name string
}

func (m *mockFileFromBytes) Read(p []byte) (int, error) {
	return m.data.Read(p)
}

func (m *mockFileFromBytes) Seek(offset int64, whence int) (int64, error) {
	return m.data.Seek(offset, whence)
}

func (m *mockFileFromBytes) Close() error {
	return nil
}

func (m *mockFileFromBytes) ReadAt(p []byte, off int64) (n int, err error) {
	return m.data.ReadAt(p, off)
}
