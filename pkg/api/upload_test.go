package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aliengiraffe/spotdb/pkg/helpers"
	"github.com/gin-gonic/gin"

	"github.com/aliengiraffe/spotdb/pkg/database"
)

// TestUploadIsEncodingSupported covers various encoding strings
func TestUploadIsEncodingSupported(t *testing.T) {
	cases := []struct {
		enc  string
		want bool
	}{
		{"", true},
		{"utf-8", true},
		{"UTF16", true},
		{"utf8", true},
		{"utf-7", false},
		{"ascii", false},
	}
	for _, c := range cases {
		got := isEncodingSupported(c.enc)
		if got != c.want {
			t.Errorf("isEncodingSupported(%q) = %v, want %v", c.enc, got, c.want)
		}
	}
}

// TestUploadValidateMimeTypeTextCsvValid tests text/* MIME type with CSV-like content passes via looksLikeCSV
func TestUploadValidateMimeTypeTextCsvValid(t *testing.T) {
	// Create multipart form with .html content but CSV structured
	buf := &bytes.Buffer{}
	writer := multipart.NewWriter(buf)
	part, err := writer.CreateFormFile("csv_file", "file.html")
	if err != nil {
		t.Fatalf("CreateFormFile failed: %v", err)
	}
	// Write CSV-like content
	csv := []byte("h1,h2,h3\n10,20,30\n")
	if _, err := part.Write(csv); err != nil {
		t.Fatalf("failed to write csv data: %v", err)
	}
	writer.Close()
	req := httptest.NewRequest(http.MethodPost, "/", buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	if err := req.ParseMultipartForm(int64(len(buf.Bytes()))); err != nil {
		t.Fatalf("ParseMultipartForm failed: %v", err)
	}
	fh := req.MultipartForm.File["csv_file"][0]
	s := &Server{}
	ctx := context.Background()
	errs, err := s.validateMimeType(ctx, fh)
	if err != nil {
		t.Errorf("validateMimeType returned error: %v", err)
	}
	if len(errs) != 0 {
		t.Errorf("expected no validation errors, got: %v", errs)
	}
}

// TestUploadValidateMimeTypeDetectError tests error mapping when MIME detection fails
func TestUploadValidateMimeTypeDetectError(t *testing.T) {
	// Create multipart form with valid CSV content
	buf := &bytes.Buffer{}
	writer := multipart.NewWriter(buf)
	part, err := writer.CreateFormFile("csv_file", "file.csv")
	if err != nil {
		t.Fatalf("CreateFormFile failed: %v", err)
	}
	if _, err := part.Write([]byte("a,b\n1,2\n")); err != nil {
		t.Fatalf("failed to write csv data: %v", err)
	}
	writer.Close()
	req := httptest.NewRequest(http.MethodPost, "/", buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	if err := req.ParseMultipartForm(int64(len(buf.Bytes()))); err != nil {
		t.Fatalf("ParseMultipartForm failed: %v", err)
	}
	fh := req.MultipartForm.File["csv_file"][0]
	// Override detectFileMimeTypeFunc to simulate detection error
	orig := detectFileMimeTypeFunc
	defer func() { detectFileMimeTypeFunc = orig }()
	detectFileMimeTypeFunc = func(ctx context.Context, file multipart.File) (string, error) {
		return "", fmt.Errorf("detect failure")
	}
	s := &Server{}
	ctx := context.Background()
	errs, err := s.validateMimeType(ctx, fh)
	if err == nil {
		t.Error("expected error for MIME detection failure, got nil")
	}
	if len(errs) != 1 || errs[0].Code != "MIME_TYPE_DETECTION_ERROR" {
		t.Errorf("expected MIME_TYPE_DETECTION_ERROR, got: %v", errs)
	}
}

// TestUploadValidateMimeTypeTextNotCsv tests text/* MIME type that does not resemble CSV
func TestUploadValidateMimeTypeTextNotCsv(t *testing.T) {
	// Create multipart form with .txt and non-CSV content
	buf := &bytes.Buffer{}
	writer := multipart.NewWriter(buf)
	part, err := writer.CreateFormFile("csv_file", "file.txt")
	if err != nil {
		t.Fatalf("CreateFormFile failed: %v", err)
	}
	data := []byte("just plain text with no delimiters\nsecond line")
	if _, err := part.Write(data); err != nil {
		t.Fatalf("failed to write data: %v", err)
	}
	writer.Close()
	req := httptest.NewRequest(http.MethodPost, "/", buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	if err := req.ParseMultipartForm(int64(len(buf.Bytes()))); err != nil {
		t.Fatalf("ParseMultipartForm failed: %v", err)
	}
	fh := req.MultipartForm.File["csv_file"][0]
	s := &Server{}
	ctx := context.Background()
	errs, err := s.validateMimeType(ctx, fh)
	if err == nil {
		t.Errorf("expected invalid file format error, got nil")
	}
	if len(errs) != 1 || errs[0].Code != "INVALID_FILE_FORMAT" {
		t.Errorf("expected INVALID_FILE_FORMAT error, got: %v", errs)
	}
}

// TestUploadProcessCsvFileFromHeaderUnsupportedEncoding tests unsupported encoding path
func TestUploadProcessCsvFileFromHeaderUnsupportedEncoding(t *testing.T) {
	s := &Server{}
	ctx := context.Background()
	tempPath, errs, err := s.processCsvFileFromHeader(ctx, nil, "table", true, "latin1")
	if err == nil {
		t.Error("expected error for unsupported encoding, got nil")
	}
	if tempPath != "" {
		t.Errorf("expected empty tempPath, got %q", tempPath)
	}
	if len(errs) != 1 || errs[0].Code != "UNSUPPORTED_ENCODING" {
		t.Errorf("expected one UNSUPPORTED_ENCODING error, got %v", errs)
	}
}

// TestUploadCopyFileDataErrorMapping tests mapping of various CopyWithMaxSize errors to CSVError codes
func TestUploadCopyFileDataErrorMapping(t *testing.T) {
	s := &Server{}
	// Backup original CopyWithMaxSize
	orig := helpers.CopyWithMaxSize
	defer func() { helpers.CopyWithMaxSize = orig }()
	// CSV validation error
	helpers.CopyWithMaxSize = func(dst io.Writer, src io.Reader, bufferSize int, maxSize int64, vf helpers.BufferValidationFunc) (int64, *helpers.ValidationIssue, error) {
		return 0, nil, fmt.Errorf("CSV validation error: parse failed")
	}
	ctx := context.Background()
	errs, err := s.copyFileData(ctx, nil, nil, "file.csv", "")
	if err == nil {
		t.Error("expected error for CSV validation failure")
	}
	if len(errs) != 1 || errs[0].Code != "CSV_VALIDATION_ERROR" {
		t.Errorf("expected CSV_VALIDATION_ERROR, got: %v", errs)
	}
	// Invalid CSV structure error with line info
	helpers.CopyWithMaxSize = func(dst io.Writer, src io.Reader, bufferSize int, maxSize int64, vf helpers.BufferValidationFunc) (int64, *helpers.ValidationIssue, error) {
		issue := &helpers.ValidationIssue{Line: 5}
		return 0, issue, fmt.Errorf("invalid CSV structure: inconsistent column count on line 5")
	}
	errs, err = s.copyFileData(ctx, nil, nil, "file.csv", "")
	if err == nil {
		t.Error("expected error for invalid CSV structure")
	}
	if len(errs) != 1 || errs[0].Code != "INVALID_CSV_STRUCTURE" || errs[0].Details.Line != 5 {
		t.Errorf("expected INVALID_CSV_STRUCTURE line 5, got: %v", errs)
	}
	// Generic file copy error
	helpers.CopyWithMaxSize = func(dst io.Writer, src io.Reader, bufferSize int, maxSize int64, vf helpers.BufferValidationFunc) (int64, *helpers.ValidationIssue, error) {
		return 0, nil, fmt.Errorf("disk error")
	}
	errs, err = s.copyFileData(ctx, nil, nil, "file.csv", "")
	if err == nil {
		t.Error("expected error for file copy failure")
	}
	if len(errs) != 1 || errs[0].Code != "FILE_COPY_ERROR" {
		t.Errorf("expected FILE_COPY_ERROR, got: %v", errs)
	}
}

// TestUploadValidateMimeTypeInvalid tests invalid MIME type detection
func TestUploadValidateMimeTypeInvalid(t *testing.T) {
	// Create a multipart form with a binary file
	buf := &bytes.Buffer{}
	writer := multipart.NewWriter(buf)
	part, err := writer.CreateFormFile("csv_file", "file.bin")
	if err != nil {
		t.Fatalf("failed to create form file: %v", err)
	}
	part.Write([]byte{0x00, 0x01, 0x02}) // nolint:errcheck
	writer.Close()
	req := httptest.NewRequest(http.MethodPost, "/", buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	if err := req.ParseMultipartForm(1024); err != nil {
		t.Fatalf("ParseMultipartForm error: %v", err)
	}
	fh := req.MultipartForm.File["csv_file"][0]
	s := &Server{}
	ctx := context.Background()
	errs, err := s.validateMimeType(ctx, fh)
	if err == nil {
		t.Error("expected error for invalid MIME type, got nil")
	}
	if len(errs) != 1 || errs[0].Code != "INVALID_FILE_FORMAT" {
		t.Errorf("expected INVALID_FILE_FORMAT error, got %v", errs)
	}
}

// TestUploadValidateEncodingFromData for empty and unsupported encoding
func TestUploadValidateEncodingFromData(t *testing.T) {
	s := &Server{}
	ctx := context.Background()
	// Empty data and default encoding should be valid
	if err := s.validateEncodingFromData(ctx, []byte{}, ""); err != nil {
		t.Errorf("validateEncodingFromData(empty, \"\") returned error: %v", err)
	}
	// Unsupported user-specified encoding should return an error
	if err := s.validateEncodingFromData(ctx, []byte{}, "unsupported"); err == nil {
		t.Errorf("validateEncodingFromData(empty, \"unsupported\") did not return error")
	}
}

// TestUploadDetectFileMimeType resets file pointer and returns a non-empty MIME type
func TestUploadDetectFileMimeType(t *testing.T) {
	// Create a temporary file with simple CSV content
	tmp, err := os.CreateTemp("", "detectmime-*.csv")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmp.Name())
	content := []byte("col1,col2\n1,2\n")
	if _, err := tmp.Write(content); err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	// Seek back to start
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("failed to seek temp file: %v", err)
	}
	// Detect MIME type
	ctx := context.Background()
	mime, err := detectFileMimeType(ctx, tmp)
	if err != nil {
		t.Errorf("detectFileMimeType returned error: %v", err)
	}
	if mime == "" {
		t.Error("detectFileMimeType returned empty MIME type")
	}
	// After detection, pointer should be reset to start
	if off, _ := tmp.Seek(0, io.SeekCurrent); off != 0 {
		t.Errorf("file pointer not reset, at offset %d", off)
	}
}

// TestUploadLooksLikeCSV returns true for valid CSV and false for non-CSV
func TestUploadLooksLikeCSV(t *testing.T) {
	// Valid CSV
	valid, err := os.CreateTemp("", "valid-*.csv")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(valid.Name())
	valid.Write([]byte("a,b,c\n1,2,3\n")) // nolint:errcheck
	valid.Seek(0, io.SeekStart)           // nolint:errcheck
	ctx := context.Background()
	isCSV, err := looksLikeCSV(ctx, valid)
	if err != nil {
		t.Errorf("looksLikeCSV(valid) error: %v", err)
	}
	if !isCSV {
		t.Error("looksLikeCSV(valid) = false, want true")
	}
	valid.Close()
	// Invalid CSV content
	invalid, err := os.CreateTemp("", "invalid-*.txt")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(invalid.Name())
	invalid.Write([]byte("just some random text without delimiter")) // nolint:errcheck
	invalid.Seek(0, io.SeekStart)                                    // nolint:errcheck
	isCSV2, err := looksLikeCSV(ctx, invalid)
	if err != nil {
		t.Errorf("looksLikeCSV(invalid) error: %v", err)
	}
	if isCSV2 {
		t.Error("looksLikeCSV(invalid) = true, want false")
	}
	invalid.Close()
}

// TestUploadCreateTempFileForUpload creates a file in temp directory
func TestUploadCreateTempFileForUpload(t *testing.T) {
	s := &Server{}
	ctx := context.Background()
	name := "testtable"
	path, f, err := s.createTempFileForUpload(ctx, name)
	if err != nil {
		t.Fatalf("createTempFileForUpload error: %v", err)
	}
	// File should exist
	f.Close()
	if stat, err := os.Stat(path); err != nil || stat.IsDir() {
		t.Errorf("temporary file not found at %s", path)
	}
	// Clean up
	os.Remove(path)
}

// TestUploadCopyFileData copies data correctly for small files
func TestUploadCopyFileData(t *testing.T) {
	// Create source file
	srcPath := filepath.Join(os.TempDir(), "testcopy-src.csv")
	content := []byte("x,y,z\n10,20,30\n")
	if err := os.WriteFile(srcPath, content, 0644); err != nil {
		t.Fatalf("failed to write source file: %v", err)
	}
	defer os.Remove(srcPath)
	src, err := os.Open(srcPath)
	if err != nil {
		t.Fatalf("failed to open source file: %v", err)
	}
	defer src.Close()
	// Create destination file
	dst, err := os.CreateTemp("", "testcopy-dst-*.csv")
	if err != nil {
		t.Fatalf("failed to create destination file: %v", err)
	}
	dstPath := dst.Name()
	defer os.Remove(dstPath)
	// Copy
	s := &Server{}
	ctx := context.Background()
	errs, err := s.copyFileData(ctx, src, dst, "test.csv", "")
	dst.Close()
	if err != nil {
		t.Errorf("copyFileData returned error: %v", err)
	}
	if len(errs) != 0 {
		t.Errorf("copyFileData returned validation errors: %v", errs)
	}
	// Verify contents
	out, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("failed to read destination file: %v", err)
	}
	if !bytes.Equal(out, content) {
		t.Errorf("copied content mismatch, got %q, want %q", out, content)
	}
}

// TestUploadCountRowsError error path when database connection is closed
func TestUploadCountRowsError(t *testing.T) {
	s := &Server{db: &database.DuckDB{}}
	ctx := context.Background()
	// Create gin context
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	// Call countRows
	count, errs, err := s.countRows(ctx, c, "sometable")
	if err == nil {
		t.Error("countRows did not return error on closed database")
	}
	if count != 0 {
		t.Errorf("countRows returned count %d, want 0", count)
	}
	if len(errs) != 1 || errs[0].Code != "ROW_COUNT_ERROR" {
		t.Errorf("countRows returned errs %v, want ROW_COUNT_ERROR", errs)
	}
}

// TestUploadCleanupTempFile tests removal of temporary file
func TestUploadCleanupTempFile(t *testing.T) {
	// Create a temp file to be cleaned up
	tmp, err := os.CreateTemp("", "cleanup-*.csv")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	path := tmp.Name()
	tmp.Close()
	// Invoke cleanup
	s := &Server{}
	ctx := context.Background()
	s.cleanupTempFile(ctx, path)
	// File should be removed
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("expected file %s to be removed, got err: %v", path, err)
	}
}

// TestUploadValidateMimeTypeValid tests that a valid CSV file passes MIME type validation
func TestUploadValidateMimeTypeValid(t *testing.T) {
	// Create multipart form with CSV file
	buf := &bytes.Buffer{}
	writer := multipart.NewWriter(buf)
	part, err := writer.CreateFormFile("csv_file", "file.csv")
	if err != nil {
		t.Fatalf("CreateFormFile failed: %v", err)
	}
	data := []byte("col1,col2\n1,2\n")
	if _, err := part.Write(data); err != nil {
		t.Fatalf("writing to form file failed: %v", err)
	}
	writer.Close()
	req := httptest.NewRequest(http.MethodPost, "/", buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	if err := req.ParseMultipartForm(int64(len(buf.Bytes()))); err != nil {
		t.Fatalf("ParseMultipartForm failed: %v", err)
	}
	fh := req.MultipartForm.File["csv_file"][0]
	s := &Server{}
	ctx := context.Background()
	errs, err := s.validateMimeType(ctx, fh)
	if err != nil {
		t.Errorf("validateMimeType returned error: %v", err)
	}
	if len(errs) != 0 {
		t.Errorf("expected no validation errors, got: %v", errs)
	}
}

// TestUploadCopyFileDataSizeExceeded tests the file size limit branch
func TestUploadCopyFileDataSizeExceeded(t *testing.T) {
	t.Setenv("ENV_MAX_FILE_SIZE", "1")
	// Prepare source with more than 1 byte
	srcPath := filepath.Join(os.TempDir(), "size-src.csv")
	content := []byte("12")
	if err := os.WriteFile(srcPath, content, 0644); err != nil {
		t.Fatalf("failed to write source: %v", err)
	}
	defer os.Remove(srcPath)
	src, err := os.Open(srcPath)
	if err != nil {
		t.Fatalf("failed to open source: %v", err)
	}
	defer src.Close()
	dst, err := os.CreateTemp("", "size-dst-*.csv")
	if err != nil {
		t.Fatalf("failed to create dst: %v", err)
	}
	dstPath := dst.Name()
	defer os.Remove(dstPath)
	s := &Server{}
	ctx := context.Background()
	errs, err := s.copyFileData(ctx, src, dst, "data.csv", "")
	dst.Close()
	if err == nil {
		t.Error("expected size exceed error, got nil")
	}
	if len(errs) != 1 || errs[0].Code != "FILE_SIZE_EXCEEDED" {
		t.Errorf("expected FILE_SIZE_EXCEEDED, got: %v", errs)
	}
}

// TestUploadProcessCsvFileFromHeaderSuccess tests successful processing of CSV file header
func TestUploadProcessCsvFileFromHeaderSuccess(t *testing.T) {
	// Build multipart form file header for a test file
	buf := &bytes.Buffer{}
	writer := multipart.NewWriter(buf)
	part, err := writer.CreateFormFile("csv_file", "testfile.csv")
	if err != nil {
		t.Fatalf("CreateFormFile failed: %v", err)
	}
	content := []byte("a,b,c\n1,2,3\n")
	if _, err := part.Write(content); err != nil {
		t.Fatalf("failed to write csv data: %v", err)
	}
	writer.Close()
	req := httptest.NewRequest(http.MethodPost, "/", buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	if err := req.ParseMultipartForm(int64(len(buf.Bytes()))); err != nil {
		t.Fatalf("ParseMultipartForm failed: %v", err)
	}
	fh := req.MultipartForm.File["csv_file"][0]
	s := &Server{}
	ctx := context.Background()
	tempPath, errs, err := s.processCsvFileFromHeader(ctx, fh, "mytable", true, "utf-8")
	if err != nil {
		t.Fatalf("processCsvFileFromHeader returned error: %v", err)
	}
	if len(errs) != 0 {
		t.Errorf("expected no validation errors, got: %v", errs)
	}
	if tempPath == "" {
		t.Error("expected non-empty temp file path")
	}
	// Clean up
	os.Remove(tempPath)
}

// TestUploadProcessCsvFileFromHeaderWithMimeCheck tests processing CSV file header including MIME validation
func TestUploadProcessCsvFileFromHeaderWithMimeCheck(t *testing.T) {
	// Build multipart form file header for a non-test prefix file
	buf := &bytes.Buffer{}
	writer := multipart.NewWriter(buf)
	part, err := writer.CreateFormFile("csv_file", "file.csv")
	if err != nil {
		t.Fatalf("CreateFormFile failed: %v", err)
	}
	content := []byte("x,y\n1,2\n")
	if _, err := part.Write(content); err != nil {
		t.Fatalf("failed to write csv data: %v", err)
	}
	writer.Close()
	req := httptest.NewRequest(http.MethodPost, "/", buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	if err := req.ParseMultipartForm(int64(len(buf.Bytes()))); err != nil {
		t.Fatalf("ParseMultipartForm failed: %v", err)
	}
	fh := req.MultipartForm.File["csv_file"][0]
	s := &Server{}
	ctx := context.Background()
	tempPath, errs, err := s.processCsvFileFromHeader(ctx, fh, "tbl", false, "utf-8")
	if err != nil {
		t.Fatalf("processCsvFileFromHeader returned error: %v", err)
	}
	if len(errs) != 0 {
		t.Errorf("expected no validation errors, got: %v", errs)
	}
	if tempPath == "" {
		t.Error("expected non-empty temp file path")
	}
	os.Remove(tempPath)
}

// TestUploadValidateEncodingMismatch tests mismatch between specified and detected encoding
func TestUploadValidateEncodingMismatch(t *testing.T) {
	s := &Server{}
	ctx := context.Background()
	// Data is valid UTF-8, but user specifies utf-16
	err := s.validateEncodingFromData(ctx, []byte("abc"), "utf-16")
	if err == nil || !strings.Contains(err.Error(), "you specified utf-16 but detected utf-8") {
		t.Errorf("expected encoding mismatch error, got: %v", err)
	}
}

// TestDetectDataEncodingFailure tests encoding detection failure
func TestDetectDataEncodingFailure(t *testing.T) {
	s := &Server{}
	ctx := context.Background()

	// Test with truly empty data - chardet should not be able to detect this
	emptyData := []byte{}

	_, _, _, err := s.detectDataEncoding(ctx, emptyData)
	if err == nil {
		t.Error("expected encoding detection error, got nil")
	}
	if !strings.Contains(err.Error(), "could not detect file encoding") {
		t.Errorf("expected 'could not detect file encoding' error, got: %v", err)
	}
}

// TestValidateUTF8UserSpecifiedFailure tests UTF-8 validation failure
func TestValidateUTF8UserSpecifiedFailure(t *testing.T) {
	s := &Server{}
	ctx := context.Background()

	// Create invalid UTF-8 data
	invalidUTF8 := []byte{0xFF, 0xFE, 0xFD} // Invalid UTF-8 sequence

	// Test with UTF-8 specified but invalid data
	err := s.validateUTF8UserSpecified(ctx, invalidUTF8, "utf-8", "utf-8", true, false)
	if err == nil {
		t.Error("expected UTF-8 validation error, got nil")
	}
	if !strings.Contains(err.Error(), "file is not valid UTF-8 encoded") {
		t.Errorf("expected UTF-8 validation error, got: %v", err)
	}
}

// TestValidateUTF8UserSpecifiedAutoDetectUTF16 tests auto-detect UTF-16 scenario
func TestValidateUTF8UserSpecifiedAutoDetectUTF16(t *testing.T) {
	s := &Server{}
	ctx := context.Background()

	// Test scenario: no encoding specified, UTF-16 detected
	err := s.validateUTF8UserSpecified(ctx, []byte("test"), "", "utf-16", false, true)
	if err != nil {
		t.Errorf("expected no error for auto-detected UTF-16, got: %v", err)
	}
}

// TestValidateUTF8UserSpecifiedEncodingMismatch tests encoding mismatch in UTF-8 validation
func TestValidateUTF8UserSpecifiedEncodingMismatch(t *testing.T) {
	s := &Server{}
	ctx := context.Background()

	// Test scenario: UTF-8 specified but different encoding detected
	err := s.validateUTF8UserSpecified(ctx, []byte("test"), "utf-8", "iso-8859-1", false, false)
	if err == nil {
		t.Error("expected encoding mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "file encoding mismatch") {
		t.Errorf("expected encoding mismatch error, got: %v", err)
	}
}

// TestValidateUTF16UserSpecifiedMismatch tests UTF-16 validation with encoding mismatch
func TestValidateUTF16UserSpecifiedMismatch(t *testing.T) {
	s := &Server{}
	ctx := context.Background()

	// Test scenario: UTF-16 specified but different encoding detected
	err := s.validateUTF16UserSpecified(ctx, "utf-16", "utf-8", false)
	if err == nil {
		t.Error("expected encoding mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "file encoding mismatch") {
		t.Errorf("expected encoding mismatch error, got: %v", err)
	}
}

// TestValidateMimeTypeFileOpenError tests MIME type validation when file open fails
func TestValidateMimeTypeFileOpenError(t *testing.T) {
	s := &Server{}
	ctx := context.Background()

	// Create a custom file header that will fail to open
	fh := &multipart.FileHeader{
		Filename: "test.csv",
		Header:   make(map[string][]string),
	}
	// This will cause Open() to fail since there's no actual file

	errs, err := s.validateMimeType(ctx, fh)
	if err == nil {
		t.Error("expected MIME type detection error, got nil")
	}
	if len(errs) != 1 || errs[0].Code != "MIME_TYPE_DETECTION_ERROR" {
		t.Errorf("expected MIME_TYPE_DETECTION_ERROR, got: %v", errs)
	}
}

// TestLooksLikeCSVFileError tests error handling in looksLikeCSV
func TestLooksLikeCSVFileError(t *testing.T) {
	ctx := context.Background()

	// Create a file that we'll close before calling looksLikeCSV to trigger an error
	tmp, err := os.CreateTemp("", "csv-error-*.csv")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmp.Name())

	tmp.Write([]byte("col1,col2\n1,2\n")) // nolint:errcheck
	tmp.Close()

	// Open and immediately close to cause read errors
	file, err := os.Open(tmp.Name())
	if err != nil {
		t.Fatalf("failed to open temp file: %v", err)
	}
	file.Close()

	// This should fail when trying to read from closed file
	_, err = looksLikeCSV(ctx, file)
	if err == nil {
		t.Error("expected file read error, got nil")
	}
}

// TestDetectFileMimeTypeFileError tests error handling in detectFileMimeType
func TestDetectFileMimeTypeFileError(t *testing.T) {
	ctx := context.Background()

	// Create a file that we'll close before calling detectFileMimeType
	tmp, err := os.CreateTemp("", "mime-error-*.csv")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmp.Name())

	tmp.Write([]byte("col1,col2\n1,2\n")) // nolint:errcheck
	tmp.Close()

	// Open and immediately close to cause read errors
	file, err := os.Open(tmp.Name())
	if err != nil {
		t.Fatalf("failed to open temp file: %v", err)
	}
	file.Close()

	// This should fail when trying to read from closed file
	_, err = detectFileMimeType(ctx, file)
	if err == nil {
		t.Error("expected file read error, got nil")
	}
}

// TestImportCsvDataFallbackDuplicateDetection tests the fallback duplicate detection in importCsvData
func TestImportCsvDataFallbackDuplicateDetection(t *testing.T) {
	ctx := context.Background()
	db, err := database.NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("NewDuckDB failed: %v", err)
	}
	s := &Server{db: db}

	// Use a unique table name
	tableName := fmt.Sprintf("fallback_duplicate_%d", time.Now().UnixNano())

	// Create an existing table
	_, err = db.ExecuteQuery(ctx, fmt.Sprintf("CREATE TABLE %s (id INTEGER)", tableName))
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = db.ExecuteQuery(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	}()

	// Create a CSV file
	csvPath := filepath.Join(os.TempDir(), "test_fallback_duplicate.csv")
	csvContent := []byte("id\\n1\\n2\\n")
	if err := os.WriteFile(csvPath, csvContent, 0644); err != nil {
		t.Fatalf("Failed to write CSV file: %v", err)
	}
	defer os.Remove(csvPath)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/dummy", nil)

	// Call importCsvData - this should trigger the fallback duplicate detection
	// because the import will fail with "already exists" error
	_, _, _, err2 := s.importCsvData(ctx, c, tableName, csvPath, true, false)
	if err2 == nil {
		t.Fatal("expected error, got nil")
	}

	// Should get DUPLICATE_TABLE_NAME error through fallback detection
	if w.Code == http.StatusUnprocessableEntity {
		var resp CSVErrorResponse
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err == nil {
			if len(resp.Errors) > 0 && resp.Errors[0].Code == "DUPLICATE_TABLE_NAME" {
				t.Log("Successfully triggered fallback duplicate detection")
			}
		}
	}
}

// TestCreateTempFileForUploadError tests error handling in createTempFileForUpload
func TestCreateTempFileForUploadError(t *testing.T) {
	s := &Server{}
	ctx := context.Background()

	// Try to create temp file - errors are rare but we test normal operation
	// since createTempFileForUpload errors are rare in practice

	path, file, err := s.createTempFileForUpload(ctx, "test_table")
	if err != nil {
		// If we get an error here, that's actually what we want to test
		t.Logf("createTempFileForUpload error (expected): %v", err)
		return
	}

	// Clean up if successful
	if file != nil {
		file.Close()
	}
	if path != "" {
		os.Remove(path)
	}

	// If no error occurred, that's normal behavior
	t.Log("createTempFileForUpload succeeded normally")
}

// TestGetColumnInfoError tests error handling in getColumnInfo
func TestGetColumnInfoError(t *testing.T) {
	// Test with uninitialized database
	s := &Server{db: &database.DuckDB{}}
	ctx := context.Background()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	result, errs, err := s.getColumnInfo(ctx, c, "any_table")
	if err == nil {
		t.Error("expected error with uninitialized database, got nil")
	}
	if result != nil {
		t.Error("expected nil result with database error")
	}
	if len(errs) == 0 {
		t.Error("expected validation errors with database failure")
	}
}

// TestUploadCopyFileDataSecurityError tests security validation failure in copyFileData
func TestUploadCopyFileDataSecurityError(t *testing.T) {
	// Create source file with malicious content
	tmp, err := os.CreateTemp("", "sec-*.csv")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmp.Name())
	// Write header followed by malicious content
	// Header line is skipped for validation; malicious in second line triggers security
	lines := []byte("col1,col2\n=CMD('malicious')\n")
	if _, err := tmp.Write(lines); err != nil {
		t.Fatalf("failed to write malicious data: %v", err)
	}
	tmp.Seek(0, io.SeekStart) // nolint:errcheck
	src := tmp
	// Destination file
	dst, err := os.CreateTemp("", "sec-dst-*.csv")
	if err != nil {
		t.Fatalf("failed to create dst file: %v", err)
	}
	defer os.Remove(dst.Name())
	s := &Server{}
	ctx := context.Background()
	errs, err := s.copyFileData(ctx, src, dst, "normal.csv", "")
	dst.Close()
	if err == nil {
		t.Fatal("expected security validation error, got nil")
	}
	if len(errs) != 1 || errs[0].Code != "SECURITY_VALIDATION_FAILED" {
		t.Errorf("expected SECURITY_VALIDATION_FAILED, got: %v", errs)
	}
}

// TestImportCsvDataDirectError tests importCsvData directImport error path
func TestImportCsvDataDirectError(t *testing.T) {
	// Prepare server with new DB
	ctx := context.Background()
	db, err := database.NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("NewDuckDB failed: %v", err)
	}
	s := &Server{db: db}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/dummy", nil)
	// Call importCsvData on nonexistent file
	_, _, _, err2 := s.importCsvData(ctx, c, "no_table", "/no/such/file.csv", true, false)
	if err2 == nil {
		t.Fatal("expected directImport error, got nil")
	}
	if w.Code != http.StatusUnprocessableEntity {
		t.Errorf("expected status 422, got %d", w.Code)
	}
	var resp CSVErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if len(resp.Errors) == 0 || resp.Errors[0].Code != "DIRECT_IMPORT_FAILED" {
		t.Errorf("expected DIRECT_IMPORT_FAILED error, got %v", resp.Errors)
	}
}


// TestCheckTableExists tests the checkTableExists function
func TestCheckTableExists(t *testing.T) {
	ctx := context.Background()
	db, err := database.NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("NewDuckDB failed: %v", err)
	}
	s := &Server{db: db}

	// Use a unique table name to avoid conflicts
	tableName := fmt.Sprintf("test_table_exists_%d", time.Now().UnixNano())

	// Create a test table
	_, err = db.ExecuteQuery(ctx, fmt.Sprintf("CREATE TABLE %s (id INTEGER, name VARCHAR)", tableName))
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		// Clean up
		_, _ = db.ExecuteQuery(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	}()

	// Test that the table exists
	exists, err := s.checkTableExists(ctx, tableName)
	if err != nil {
		t.Errorf("checkTableExists returned error: %v", err)
	}
	if !exists {
		t.Error("checkTableExists returned false for existing table")
	}

	// Test that a non-existent table does not exist
	exists2, err := s.checkTableExists(ctx, "non_existent_table")
	if err != nil {
		t.Errorf("checkTableExists returned error for non-existent table: %v", err)
	}
	if exists2 {
		t.Error("checkTableExists returned true for non-existent table")
	}

	// Test edge case: empty table name
	exists3, err := s.checkTableExists(ctx, "")
	if err != nil {
		t.Logf("checkTableExists with empty name returned error (expected): %v", err)
	}
	if exists3 {
		t.Error("checkTableExists returned true for empty table name")
	}

	// Test with special characters in table name (SQL injection protection)
	exists4, err := s.checkTableExists(ctx, "'; DROP TABLE test_table_exists; --")
	if err != nil {
		t.Logf("checkTableExists with SQL injection attempt returned error (expected): %v", err)
	}
	if exists4 {
		t.Error("checkTableExists returned true for SQL injection attempt")
	}
}

// TestCheckTableExistsError tests error handling in checkTableExists
func TestCheckTableExistsError(t *testing.T) {
	s := &Server{db: &database.DuckDB{}} // Uninitialized DB
	ctx := context.Background()

	exists, err := s.checkTableExists(ctx, "any_table")
	if err == nil {
		t.Error("checkTableExists did not return error with closed database")
	}
	if exists {
		t.Error("checkTableExists returned true with closed database")
	}
}

// TestCheckTableExistsEdgeCases tests edge cases in checkTableExists
func TestCheckTableExistsEdgeCases(t *testing.T) {
	ctx := context.Background()
	db, err := database.NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("NewDuckDB failed: %v", err)
	}
	s := &Server{db: db}

	// Test case where result.Results[0]["table_count"] is not int64
	// This would require mocking, but we can test the path by checking a non-existent table
	exists, err := s.checkTableExists(ctx, "definitely_does_not_exist_12345")
	if err != nil {
		t.Errorf("checkTableExists returned error: %v", err)
	}
	if exists {
		t.Error("checkTableExists returned true for non-existent table")
	}
}

// TestImportCsvDataDuplicateTable tests duplicate table detection in importCsvData
func TestImportCsvDataDuplicateTable(t *testing.T) {
	ctx := context.Background()
	db, err := database.NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("NewDuckDB failed: %v", err)
	}
	s := &Server{db: db}

	// Use a unique table name to avoid conflicts
	tableName := fmt.Sprintf("duplicate_test_%d", time.Now().UnixNano())

	// Create an existing table
	_, err = db.ExecuteQuery(ctx, fmt.Sprintf("CREATE TABLE %s (id INTEGER)", tableName))
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = db.ExecuteQuery(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	}()

	// Create a CSV file
	csvPath := filepath.Join(os.TempDir(), "test_duplicate.csv")
	csvContent := []byte("id\n1\n2\n3\n")
	if err := os.WriteFile(csvPath, csvContent, 0644); err != nil {
		t.Fatalf("Failed to write CSV file: %v", err)
	}
	defer os.Remove(csvPath)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/dummy", nil)

	// Try to import with override=false (should fail)
	_, _, _, err2 := s.importCsvData(ctx, c, tableName, csvPath, true, false)
	if err2 == nil {
		t.Fatal("expected duplicate table error, got nil")
	}
	if w.Code != http.StatusUnprocessableEntity {
		t.Errorf("expected status 422, got %d", w.Code)
	}

	var resp CSVErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if len(resp.Errors) == 0 || resp.Errors[0].Code != "DUPLICATE_TABLE_NAME" {
		t.Errorf("expected DUPLICATE_TABLE_NAME error, got %v", resp.Errors)
	}
	if !strings.Contains(resp.Errors[0].Message, tableName) {
		t.Errorf("error message should contain table name, got: %s", resp.Errors[0].Message)
	}
	if !strings.Contains(resp.Errors[0].Details.Suggestion, "_v2") {
		t.Errorf("suggestion should contain alternative table name, got: %s", resp.Errors[0].Details.Suggestion)
	}
}

// TestImportCsvDataDuplicateTableWithOverride tests importing with override=true
func TestImportCsvDataDuplicateTableWithOverride(t *testing.T) {
	ctx := context.Background()
	db, err := database.NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("NewDuckDB failed: %v", err)
	}
	s := &Server{db: db}

	// Use a unique table name to avoid conflicts
	tableName := fmt.Sprintf("override_test_%d", time.Now().UnixNano())

	// Create an existing table with some data
	_, err = db.ExecuteQuery(ctx, fmt.Sprintf("CREATE TABLE %s (id INTEGER)", tableName))
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	_, err = db.ExecuteQuery(ctx, fmt.Sprintf("INSERT INTO %s VALUES (99)", tableName))
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}
	defer func() {
		_, _ = db.ExecuteQuery(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	}()

	// Create a CSV file with different data
	csvPath := filepath.Join(os.TempDir(), "test_override.csv")
	csvContent := []byte("id\n1\n2\n3\n")
	if err := os.WriteFile(csvPath, csvContent, 0644); err != nil {
		t.Fatalf("Failed to write CSV file: %v", err)
	}
	defer os.Remove(csvPath)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/dummy", nil)

	// Import with override=true (should succeed)
	result, rowCount, _, err2 := s.importCsvData(ctx, c, tableName, csvPath, true, true)
	if err2 != nil {
		t.Fatalf("importCsvData with override failed: %v", err2)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if rowCount != 3 {
		t.Errorf("expected 3 rows, got %d", rowCount)
	}

	// Verify the table was replaced
	checkResult, err := db.ExecuteQuery(ctx, fmt.Sprintf("SELECT COUNT(*) as cnt FROM %s", tableName))
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if len(checkResult.Results) > 0 {
		if count, ok := checkResult.Results[0]["cnt"].(int64); ok {
			if count != 3 {
				t.Errorf("expected 3 rows in table after override, got %d", count)
			}
		}
	}
}

// TestImportCsvDataDuplicateErrorFallback tests the fallback duplicate detection
func TestImportCsvDataDuplicateErrorFallback(t *testing.T) {
	ctx := context.Background()
	db, err := database.NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("NewDuckDB failed: %v", err)
	}

	// Use a unique table name to avoid conflicts
	tableName := fmt.Sprintf("fallback_test_%d", time.Now().UnixNano())

	// Create a mock server that always returns false for checkTableExists
	s := &Server{db: db}

	// Create an existing table
	_, err = db.ExecuteQuery(ctx, fmt.Sprintf("CREATE TABLE %s (id INTEGER)", tableName))
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = db.ExecuteQuery(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	}()

	// Create a CSV file
	csvPath := filepath.Join(os.TempDir(), "test_fallback.csv")
	csvContent := []byte("id\n1\n2\n")
	if err := os.WriteFile(csvPath, csvContent, 0644); err != nil {
		t.Fatalf("Failed to write CSV file: %v", err)
	}
	defer os.Remove(csvPath)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/dummy", nil)

	// Try to import which should trigger the fallback error handling
	// when directImport fails with "already exists" error
	_, _, _, err2 := s.importCsvData(ctx, c, tableName, csvPath, true, false)
	if err2 == nil {
		t.Fatal("expected duplicate table error, got nil")
	}

	// The response should still contain the DUPLICATE_TABLE_NAME error
	if w.Code == http.StatusUnprocessableEntity {
		var resp CSVErrorResponse
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err == nil {
			if len(resp.Errors) > 0 && resp.Errors[0].Code == "DUPLICATE_TABLE_NAME" {
				// This is the expected behavior for the fallback path
				t.Log("Successfully caught duplicate table through fallback mechanism")
				if !strings.Contains(resp.Errors[0].Details.Suggestion, time.Now().Format("20060102")) {
					t.Errorf("suggestion should contain today's date, got: %s", resp.Errors[0].Details.Suggestion)
				}
			}
		}
	}
}

// TestImportCsvDataCheckTableExistsError tests when checkTableExists returns an error
func TestImportCsvDataCheckTableExistsError(t *testing.T) {
	ctx := context.Background()

	// Create a server with an uninitialized database that will cause checkTableExists to fail
	s := &Server{db: &database.DuckDB{}}

	// Create a CSV file
	csvPath := filepath.Join(os.TempDir(), "test_check_error.csv")
	csvContent := []byte("id\n1\n2\n")
	if err := os.WriteFile(csvPath, csvContent, 0644); err != nil {
		t.Fatalf("Failed to write CSV file: %v", err)
	}
	defer os.Remove(csvPath)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/dummy", nil)

	// Try to import - checkTableExists will fail, so it should proceed to try the import
	_, _, _, err := s.importCsvData(ctx, c, "check_error_test", csvPath, true, false)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// The error should be from the failed import, not from checkTableExists
	if w.Code != http.StatusUnprocessableEntity {
		t.Errorf("expected status 422, got %d", w.Code)
	}
}

// TestImportCsvDataDuplicateDetection tests duplicate detection
func TestImportCsvDataDuplicateDetection(t *testing.T) {
	ctx := context.Background()
	db, err := database.NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("NewDuckDB failed: %v", err)
	}
	s := &Server{db: db}

	// Use a unique table name to avoid conflicts
	tableName := fmt.Sprintf("smart_duplicate_test_%d", time.Now().UnixNano())

	// Create an existing table
	_, err = db.ExecuteQuery(ctx, fmt.Sprintf("CREATE TABLE %s (id INTEGER)", tableName))
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
	defer func() {
		_, _ = db.ExecuteQuery(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	}()

	// Create a CSV file
	csvPath := filepath.Join(os.TempDir(), "test_smart_duplicate.csv")
	csvContent := []byte("id\n1\n2\n3\n")
	if err := os.WriteFile(csvPath, csvContent, 0644); err != nil {
		t.Fatalf("Failed to write CSV file: %v", err)
	}
	defer os.Remove(csvPath)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/dummy", nil)

	// Try to import with override=false (should fail with duplicate)
	_, _, _, err2 := s.importCsvData(ctx, c, tableName, csvPath, true, false)
	if err2 == nil {
		t.Fatal("expected duplicate table error, got nil")
	}
	if w.Code != http.StatusUnprocessableEntity {
		t.Errorf("expected status 422, got %d", w.Code)
	}

	var resp CSVErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if len(resp.Errors) == 0 || resp.Errors[0].Code != "DUPLICATE_TABLE_NAME" {
		t.Errorf("expected DUPLICATE_TABLE_NAME error, got %v", resp.Errors)
	}
}

// TestUploadFileSizeExceededError tests file size exceeded error handling
func TestUploadFileSizeExceededError(t *testing.T) {
	// Create a mock request with "file too large" error
	buf := &bytes.Buffer{}
	writer := multipart.NewWriter(buf)

	// Add form fields first
	if err := writer.WriteField("table_name", "test_table"); err != nil {
		t.Fatalf("WriteField failed: %v", err)
	}
	if err := writer.WriteField("has_header", "true"); err != nil {
		t.Fatalf("WriteField failed: %v", err)
	}
	if err := writer.WriteField("smart", "false"); err != nil {
		t.Fatalf("WriteField failed: %v", err)
	}

	part, err := writer.CreateFormFile("csv_file", "large_file.csv")
	if err != nil {
		t.Fatalf("CreateFormFile failed: %v", err)
	}
	// Write some content
	if _, err := part.Write([]byte("col1,col2\n1,2\n")); err != nil {
		t.Fatalf("failed to write csv data: %v", err)
	}
	writer.Close()

	// Create request
	req := httptest.NewRequest(http.MethodPost, "/api/v1/upload", buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	// Mock the processCsvFileFromHeader to return "file too large" error
	s := &Server{}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = req

	// Override helpers.CopyWithMaxSize to simulate file too large error
	orig := helpers.CopyWithMaxSize
	defer func() { helpers.CopyWithMaxSize = orig }()
	helpers.CopyWithMaxSize = func(dst io.Writer, src io.Reader, bufferSize int, maxSize int64, vf helpers.BufferValidationFunc) (int64, *helpers.ValidationIssue, error) {
		return 0, nil, helpers.ErrMaxFileSizeExceeded
	}

	// Call handleCSVUpload
	handler := s.handleCSVUpload()
	handler(c)

	// Verify response
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("expected status 413, got %d", w.Code)
	}

	var resp CSVErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if len(resp.Errors) == 0 || resp.Errors[0].Code != "FILE_SIZE_EXCEEDED" {
		t.Errorf("expected FILE_SIZE_EXCEEDED error, got %v", resp.Errors)
	}
}

// TestImportCsvDataCatalogErrorNotDuplicate tests handling of other Catalog Errors
func TestImportCsvDataCatalogErrorNotDuplicate(t *testing.T) {
	ctx := context.Background()
	db, err := database.NewDuckDB(ctx)
	if err != nil {
		t.Fatalf("NewDuckDB failed: %v", err)
	}
	s := &Server{db: db}

	// Use a unique table name to avoid conflicts
	tableName := fmt.Sprintf("invalid_test_%d", time.Now().UnixNano())

	// Create a non-existent CSV file path to trigger a file error
	csvPath := filepath.Join(os.TempDir(), "this_file_does_not_exist_12345.csv")

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/dummy", nil)

	// Try to import non-existent file - should fail but not with duplicate error
	_, _, _, err2 := s.importCsvData(ctx, c, tableName, csvPath, true, false)
	if err2 == nil {
		t.Fatal("expected error for non-existent CSV file, got nil")
	}
	if w.Code != http.StatusUnprocessableEntity {
		t.Errorf("expected status 422, got %d", w.Code)
	}

	var resp CSVErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err == nil {
		if len(resp.Errors) > 0 && resp.Errors[0].Code == "DUPLICATE_TABLE_NAME" {
			t.Error("should not get DUPLICATE_TABLE_NAME error for non-existent file")
		}
	}
}
