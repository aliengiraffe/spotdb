//go:build integration
// +build integration

package api

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCSVInjectionRejection tests that CSV files with injection attacks are properly rejected
func TestCSVInjectionRejection(t *testing.T) {
	// Setup test environment
	env := setupTestEnvironment(t)
	defer env.cleanup()

	// Directory containing injection test files
	injectionDir := "../../testdata/csv/injection/reject"

	// Clean up any existing tables from previous test runs
	cleanSQL := "DROP TABLE IF EXISTS injection_test;"
	if _, err := env.db.ExecuteQuery(cleanSQL); err != nil {
		t.Logf("Warning: Failed to clean up test table: %v", err)
	}

	// Get all files in the reject directory
	files, err := os.ReadDir(injectionDir)
	if err != nil {
		t.Fatalf("Failed to read injection directory: %v", err)
	}

	if len(files) == 0 {
		t.Fatal("No injection test files found in testdata/csv/injection/reject/")
	}

	// Process each injection file
	for _, fileInfo := range files {
		if fileInfo.IsDir() {
			continue
		}

		fileName := fileInfo.Name()
		filePath := filepath.Join(injectionDir, fileName)

		t.Run(fileName, func(t *testing.T) {
			testInjectionRejection(t, env, filePath)
		})
	}
}

// testInjectionRejection tests a single injection file to ensure it's rejected
func testInjectionRejection(t *testing.T, env *testEnv, filePath string) {
	t.Helper()

	// Read file to make sure it exists and is accessible
	fileContent, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read test file %s: %v", filePath, err)
	}

	// Log sample of the file content for debugging
	if len(fileContent) > 50 {
		t.Logf("Testing injection file: %s (first 50 bytes: %q...)", filePath, fileContent[:50])
	} else {
		t.Logf("Testing injection file: %s (content: %q)", filePath, fileContent)
	}

	// Create request using the builder
	request := TestRequest{
		FormValues: map[string]string{
			"table_name": "injection_test",
			"has_header": "true",
			"smart":      "false", // Use direct import for testing
		},
		FileContent: fileContent,
		FileName:    filepath.Base(filePath),
		MimeType:    "text/csv",
	}
	req, rec := createMultipartRequest(t, request)

	// Call the handler
	env.server.Router().ServeHTTP(rec, req)

	// Verify the response is a rejection
	validateInjectionRejection(t, rec, filepath.Base(filePath))
}

// validateInjectionRejection verifies that the response indicates rejection of malicious content
func validateInjectionRejection(t *testing.T, rec *httptest.ResponseRecorder, fileName string) {
	// Check that the response is an error (rejection)
	if rec.Code == http.StatusOK {
		t.Errorf("Expected injection file %s to be rejected, but got success response", fileName)
		t.Logf(respBodyFormat, rec.Body.String())
		return
	}

	// Verify the error response contains security-related message
	bodyStr := rec.Body.String()
	securityRelatedTerms := []string{
		"security", "validation", "invalid", "malicious", "injection", "suspicious", "failed",
	}

	foundSecurityTerm := false
	for _, term := range securityRelatedTerms {
		if strings.Contains(strings.ToLower(bodyStr), term) {
			foundSecurityTerm = true
			break
		}
	}

	if !foundSecurityTerm {
		t.Errorf("Expected error message to mention security concerns for file %s, got: %s",
			fileName, bodyStr)
	}

	// For detailed security errors, try to parse the JSON response and check for specific details
	parseAndValidateSecurityError(t, rec, fileName)
}

// parseAndValidateSecurityError attempts to parse the response as a JSON security error
func parseAndValidateSecurityError(t *testing.T, rec *httptest.ResponseRecorder, fileName string) {
	// Parse response
	response, err := parseJSONResponse(t, rec)
	if err != nil {
		// If we can't parse the response, that's okay - might be a different error format
		return
	}

	// Check for errors array in the response
	errs, ok := response["errors"].([]interface{})
	if !ok || len(errs) == 0 {
		return // Not a structured error response
	}

	// Get first error
	firstErr, ok := errs[0].(map[string]interface{})
	if !ok {
		return
	}

	// Check error code
	code, ok := firstErr["code"].(string)
	if ok && code == "SECURITY_VALIDATION_FAILED" {
		t.Logf("Validation correctly detected security issues in %s", fileName)
	}

	// Check error details if present
	if details, ok := firstErr["details"].(map[string]interface{}); ok {
		if value, ok := details["foundValue"].(string); ok {
			t.Logf("Malicious value detected: %s", value)
		}
		if col, ok := details["column"].(string); ok {
			t.Logf("Issue found in column: %s", col)
		}
	}
}
