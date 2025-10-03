package api

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// Constants used in tests
const (
	testDataRoot = "testdata/csv/common"
	successDir   = "success"
	rejectDir    = "reject"
)

// CSVTestSuite groups the common test data and utilities
type CSVTestSuite struct {
	ProjectRoot string
	SuccessPath string
	RejectPath  string
}

// NewCSVTestSuite creates a new test suite with properly initialized paths
func NewCSVTestSuite(t *testing.T) *CSVTestSuite {
	projectRoot := getProjectRoot(t)
	return &CSVTestSuite{
		ProjectRoot: projectRoot,
		SuccessPath: filepath.Join(projectRoot, testDataRoot, successDir),
		RejectPath:  filepath.Join(projectRoot, testDataRoot, rejectDir),
	}
}

// GetSuccessFiles returns all files in the success directory
func (suite *CSVTestSuite) GetSuccessFiles(t *testing.T) []os.DirEntry {
	return suite.getFilesInDir(t, suite.SuccessPath)
}

// GetRejectFiles returns all files in the reject directory
func (suite *CSVTestSuite) GetRejectFiles(t *testing.T) []os.DirEntry {
	return suite.getFilesInDir(t, suite.RejectPath)
}

// getFilesInDir is a helper to get files in a directory
func (suite *CSVTestSuite) getFilesInDir(t *testing.T, dir string) []os.DirEntry {
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("Failed to read directory %s: %v", dir, err)
	}
	return files
}

// OpenFile opens a file and returns a reader for it
func (suite *CSVTestSuite) OpenFile(t *testing.T, dirPath, fileName string) io.ReadSeeker {
	filePath := filepath.Join(dirPath, fileName)
	reader, err := createReaderFromFile(filePath)
	if err != nil {
		t.Fatalf("Failed to open file %s: %v", filePath, err)
	}
	return reader
}

// Helper function to create a ReadSeeker from a file
func createReaderFromFile(filePath string) (io.ReadSeeker, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	// Read the entire file content
	content, err := io.ReadAll(file)
	if err != nil {
		file.Close()
		return nil, err
	}

	// Close the file as we now have the content
	file.Close()

	// Return a ReadSeeker for the content
	return bytes.NewReader(content), nil
}

// getProjectRoot returns the project root directory
func getProjectRoot(t *testing.T) string {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Could not get current working directory: %v", err)
	}
	return filepath.Dir(filepath.Dir(cwd))
}

// Unit tests that directly use the exported data functions instead
// of the helper functions that rely on file reading

// Unit tests for helper functions
// These tests are independent from file based tests and use in-memory data

func TestCSVHelperFunctions(t *testing.T) {
	// Define common test data
	commonRecords := [][]string{
		{"ID", "Name", "Address"},
		{"1", "Alice", "123 Main St"},
		{"2", "Bob", "981 Oak Ave"},
	}

	inconsistentRecords := [][]string{
		{"ID", "Name", "Address"},
		{"1", "Bender", "789 Main St"},
		{"2", "Lila", "1456 Oak Ave", "Extra"},
	}

	recordsWithQuotes := [][]string{
		{"ID", "Name", "Address"},
		{"1", "Tiger", "456 Main St"},
		{"2", "Leon with \"quotes\"", "456 Oak Ave"},
	}

	recordsWithEmptyFields := [][]string{
		{"ID", "Name", "Address"},
		{"1", "Fry", ""},
		{"2", "", "113 Oak Ave"},
	}

	// Group related tests
	t.Run("Field analysis", func(t *testing.T) {
		t.Run("hasQuotedFields", func(t *testing.T) {
			testCases := []struct {
				name     string
				records  [][]string
				expected bool
			}{
				{"No quotes", commonRecords, false},
				{"With quotes", recordsWithQuotes, true},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					result := hasQuotedFields(tc.records)
					if result != tc.expected {
						t.Errorf("hasQuotedFields() = %v, want %v", result, tc.expected)
					}
				})
			}
		})

		t.Run("calculateEmptyFieldsRatio", func(t *testing.T) {
			testCases := []struct {
				name     string
				records  [][]string
				expected float64
			}{
				{"No empty fields", commonRecords, 0.0},
				{"Some empty fields", recordsWithEmptyFields, 2.0 / 9.0}, // 2 empty fields out of 9 total
				{"Empty records", [][]string{}, 1.0},                     // All empty for empty records
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					result := calculateEmptyFieldsRatio(tc.records)
					if !almostEqual(result, tc.expected, 0.0001) {
						t.Errorf("calculateEmptyFieldsRatio() = %v, want %v", result, tc.expected)
					}
				})
			}
		})

		t.Run("hasConsistentColumnCount", func(t *testing.T) {
			testCases := []struct {
				name        string
				records     [][]string
				columnCount int
				expected    bool
			}{
				{"Consistent columns", commonRecords, 3, true},
				{"Inconsistent columns", inconsistentRecords, 3, false},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					result := hasConsistentColumnCount(tc.records, tc.columnCount)
					if result != tc.expected {
						t.Errorf("hasConsistentColumnCount() = %v, want %v", result, tc.expected)
					}
				})
			}
		})
	})

	t.Run("Line terminator detection", func(t *testing.T) {
		testCases := []struct {
			name     string
			content  string
			expected string
		}{
			{"Unix line endings", "Line1\nLine2\nLine3", "\n"},
			{"Windows line endings", "Line1\r\nLine2\r\nLine3", "\r\n"},
			{"Old Mac line endings", "Line1\rLine2\rLine3", "\r"},
			{"No line endings", "SingleLineNoEnding", "\n"}, // Default to Unix
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				sample := []byte(tc.content)
				result := detectLineTerminator(sample)
				if result != tc.expected {
					t.Errorf("detectLineTerminator() = %q, want %q", result, tc.expected)
				}
			})
		}
	})
}

// Helper function to compare floating point values with tolerance
func almostEqual(a, b, tolerance float64) bool {
	return (a-b) < tolerance && (b-a) < tolerance
}
