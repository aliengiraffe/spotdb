package api

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"unicode/utf8"

	"github.com/wlynxg/chardet"
)

// Constants used in tests
const (
	encodingTestDataRoot = "testdata/csv/encoding"
	encodingSuccessDir   = "success"
	encodingRejectDir    = "reject"
)

// EncodingTestSuite handles testing encoding-related functionality
type EncodingTestSuite struct {
	ProjectRoot string
	SuccessPath string
	RejectPath  string
}

// NewEncodingTestSuite creates a new test suite with properly initialized paths
func NewEncodingTestSuite(t *testing.T) *EncodingTestSuite {
	// Get project root
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Could not get current working directory: %v", err)
	}
	projectRoot := filepath.Dir(filepath.Dir(cwd))

	return &EncodingTestSuite{
		ProjectRoot: projectRoot,
		SuccessPath: filepath.Join(projectRoot, encodingTestDataRoot, encodingSuccessDir),
		RejectPath:  filepath.Join(projectRoot, encodingTestDataRoot, encodingRejectDir),
	}
}

// GetSuccessFiles returns all files in the success directory
func (suite *EncodingTestSuite) GetSuccessFiles(t *testing.T) []os.DirEntry {
	return suite.getFilesInDir(t, suite.SuccessPath)
}

// GetRejectFiles returns all files in the reject directory
func (suite *EncodingTestSuite) GetRejectFiles(t *testing.T) []os.DirEntry {
	return suite.getFilesInDir(t, suite.RejectPath)
}

// getFilesInDir is a helper to get files in a directory
func (suite *EncodingTestSuite) getFilesInDir(t *testing.T, dir string) []os.DirEntry {
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("Failed to read directory %s: %v", dir, err)
	}
	return files
}

// GetFilePath returns the full path for a file in the given directory
func (suite *EncodingTestSuite) GetFilePath(dirPath, fileName string) string {
	return filepath.Join(dirPath, fileName)
}

// ReadFileContent reads a file and returns its content as []byte
func (suite *EncodingTestSuite) ReadFileContent(t *testing.T, dirPath, fileName string) []byte {
	filePath := suite.GetFilePath(dirPath, fileName)
	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read file %s: %v", filePath, err)
	}
	return content
}

// OpenFile opens a file and returns both the content and the file handle
func (suite *EncodingTestSuite) OpenFile(t *testing.T, dirPath, fileName string) ([]byte, *os.File) {
	filePath := suite.GetFilePath(dirPath, fileName)

	// Read file contents
	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read file %s: %v", filePath, err)
	}

	// Open the file for streaming operations
	file, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("Failed to open file %s: %v", filePath, err)
	}

	return content, file
}

// TestSuccessEncodings tests that files in the success directory pass encoding validation
func TestSuccessEncodings(t *testing.T) {
	suite := NewEncodingTestSuite(t)
	testEncodingFiles(t, suite, suite.SuccessPath, true)
}

// TestRejectEncodings tests that files in the reject directory fail encoding validation
func TestRejectEncodings(t *testing.T) {
	suite := NewEncodingTestSuite(t)
	testEncodingFiles(t, suite, suite.RejectPath, false)
}

// testEncodingFiles provides a reusable function for testing file encodings
func testEncodingFiles(t *testing.T, suite *EncodingTestSuite, dirPath string, expectSuccess bool) {
	// Create a mock server
	s := &Server{}

	// Get all files in the directory
	files := suite.getFilesInDir(t, dirPath)

	// Directory type is used for test context
	dirType := filepath.Base(dirPath)

	// Process each file in the directory
	for _, fileInfo := range files {
		if fileInfo.IsDir() {
			continue
		}

		fileName := fileInfo.Name()

		t.Run(dirType+"/"+fileName, func(t *testing.T) {
			// Read and validate the file
			fileContent, file := suite.OpenFile(t, dirPath, fileName)
			defer file.Close()

			// Tests are organized in subtests for better reporting
			t.Run("UTF-8 Validation", func(t *testing.T) {
				validateUTF8(t, fileContent, fileName, expectSuccess)
			})

			t.Run("Server Encoding Validation", func(t *testing.T) {
				// Create multipart file and test encoding validation
				multipartFile := &multipartTestFile{File: file}
				validateEncoding(t, s, multipartFile, fileName, expectSuccess)
			})

			t.Run("Encoding Detection", func(t *testing.T) {
				// Detect and log encoding information
				detectAndLogEncoding(t, fileContent, fileName)
			})
		})
	}
}

// TestInMemoryEncoding tests encoding validation with in-memory content
func TestInMemoryEncoding(t *testing.T) {
	// Create a mock server
	s := &Server{}

	// Define test cases with in-memory content
	testCases := []struct {
		name          string
		content       []byte
		expectSuccess bool
	}{
		{
			name: "Valid UTF-8",
			content: []byte(`id,name,value
1,"José García",10.5
2,"Miß Schmidt",20.75
3,"汉字 Zhang",30.0
`),
			expectSuccess: true,
		},
		{
			name:          "Invalid UTF-8",
			content:       []byte{0x49, 0x44, 0x2C, 0x4E, 0x61, 0x6D, 0x65, 0x0A, 0x31, 0x2C, 0xFF, 0xFE, 0xFF, 0x0A},
			expectSuccess: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test UTF-8 validation
			t.Run("UTF-8 Validation", func(t *testing.T) {
				validateUTF8(t, tc.content, tc.name, tc.expectSuccess)
			})

			// Test server encoding validation
			t.Run("Server Encoding Validation", func(t *testing.T) {
				// Create temp file
				tmpFile, err := os.CreateTemp("", "encoding-test-*.csv")
				if err != nil {
					t.Fatalf("Failed to create temp file: %v", err)
				}
				defer os.Remove(tmpFile.Name())
				defer tmpFile.Close()

				// Write content to temp file
				if _, err := tmpFile.Write(tc.content); err != nil {
					t.Fatalf("Failed to write content to temp file: %v", err)
				}

				// Reset file position
				if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
					t.Fatalf("Failed to reset file position: %v", err)
				}

				// Create multipart file wrapper
				multipartFile := &multipartTestFile{File: tmpFile}

				// Test encoding validation
				validateEncoding(t, s, multipartFile, tc.name, tc.expectSuccess)
			})

			// Test encoding detection
			t.Run("Encoding Detection", func(t *testing.T) {
				detectAndLogEncoding(t, tc.content, tc.name)
			})
		})
	}
}

// Implement a simple wrapper around os.File to make it compatible with multipart.File
type multipartTestFile struct {
	*os.File
}

func (f *multipartTestFile) ReadString(delim byte) (string, error) {
	return "", nil // Not needed for the test
}

// validateUTF8 checks if the file content is valid UTF-8
func validateUTF8(t *testing.T, fileContent []byte, fileName string, expectValid bool) {
	isValid := utf8.Valid(fileContent)

	if expectValid && !isValid {
		t.Errorf("Expected file %s to be valid UTF-8, but it's not", fileName)
	} else if !expectValid && isValid {
		t.Logf("Warning: File %s in reject directory is unexpectedly valid UTF-8", fileName)
	}
}

// validateEncoding tests the server's encoding validation function
func validateEncoding(t *testing.T, s *Server, multipartFile *multipartTestFile, fileName string, expectSuccess bool) {
	// Reset file position to start
	if _, err := multipartFile.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("Failed to reset file position: %v", err)
	}

	// Read a sample of the file
	const sampleSize = 8192 * 4 // 32KB sample
	buffer := make([]byte, sampleSize)

	n, err := multipartFile.Read(buffer)
	if err != nil && err != io.EOF {
		t.Fatalf("Failed to read file for encoding detection: %v", err)
	}

	// Test encoding validation with auto-detection (empty string)
	ctx := context.Background()
	err = s.validateEncodingFromData(ctx, buffer[:n], "")

	if expectSuccess {
		if err != nil {
			t.Errorf("Expected encoding validation to pass for %s, but got error: %v", fileName, err)
		}
	} else {
		if err == nil {
			t.Errorf("Expected encoding validation to fail for %s, but it passed", fileName)
		} else {
			t.Logf("Validation correctly failed for %s: %v", fileName, err)
		}
	}
}

// detectAndLogEncoding detects and logs file encoding information
func detectAndLogEncoding(t *testing.T, fileContent []byte, fileName string) {
	detector := chardet.NewUniversalDetector(0)
	detector.Feed(fileContent)
	result := detector.GetResult()

	if result.Encoding == "" {
		t.Logf("No encoding detected for %s", fileName)
	} else {
		t.Logf("File %s detected as %s with confidence %f", fileName, result.Encoding, result.Confidence)
	}
}

// TestEmptyDataEncoding tests validation of empty data
func TestEmptyDataEncoding(t *testing.T) {
	// Create a mock server
	s := &Server{}

	// Test cases for empty data with different encodings
	testCases := []struct {
		name          string
		encoding      string
		expectSuccess bool
	}{
		{
			name:          "Empty data with UTF-8",
			encoding:      "utf-8",
			expectSuccess: true,
		},
		{
			name:          "Empty data with UTF-16",
			encoding:      "utf-16",
			expectSuccess: true,
		},
		{
			name:          "Empty data with no encoding specified",
			encoding:      "",
			expectSuccess: true,
		},
		{
			name:          "Empty data with unsupported encoding",
			encoding:      "latin1",
			expectSuccess: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Empty data
			emptyData := []byte{}

			// Test validation with empty data
			ctx := context.Background()
			err := s.validateEncodingFromData(ctx, emptyData, tc.encoding)

			if tc.expectSuccess {
				if err != nil {
					t.Errorf("Expected empty data validation to pass with encoding %s, but got error: %v",
						tc.encoding, err)
				}
			} else {
				if err == nil {
					t.Errorf("Expected empty data validation to fail with encoding %s, but it passed",
						tc.encoding)
				} else {
					t.Logf("Validation correctly failed for empty data with encoding %s: %v",
						tc.encoding, err)
				}
			}
		})
	}
}

// TestEncodingWithSpecifiedEncoding tests validation with different specified encodings
func TestEncodingWithSpecifiedEncoding(t *testing.T) {
	// Create a mock server
	s := &Server{}

	// Test cases for different encoding specifications
	testCases := []struct {
		name              string
		specifiedEncoding string
		content           []byte
		expectSuccess     bool
	}{
		{
			name:              "UTF-8 specified with UTF-8 content",
			specifiedEncoding: "utf-8",
			content: []byte(`id,name,value
1,"José García",10.5
2,"Miß Schmidt",20.75
3,"汉字 Zhang",30.0
`),
			expectSuccess: true,
		},
		{
			name:              "UTF-16 specified with UTF-8 content",
			specifiedEncoding: "utf-16",
			content: []byte(`id,name,value
1,"José García",10.5
2,"Miß Schmidt",20.75
3,"汉字 Zhang",30.0
`),
			expectSuccess: false,
		},
		{
			name:              "Unsupported encoding specified",
			specifiedEncoding: "latin1",
			content: []byte(`id,name,value
1,"José García",10.5
2,"Miß Schmidt",20.75
3,"汉字 Zhang",30.0
`),
			expectSuccess: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create temp file
			tmpFile, err := os.CreateTemp("", "encoding-test-*.csv")
			if err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}
			defer os.Remove(tmpFile.Name())
			defer tmpFile.Close()

			// Write content to temp file
			if _, err := tmpFile.Write(tc.content); err != nil {
				t.Fatalf("Failed to write content to temp file: %v", err)
			}

			// Reset file position
			if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
				t.Fatalf("Failed to reset file position: %v", err)
			}

			// Create multipart file wrapper
			multipartFile := &multipartTestFile{File: tmpFile}

			// Test encoding validation with specified encoding
			// Read a sample of the file
			const sampleSize = 8192 * 4 // 32KB sample
			buffer := make([]byte, sampleSize)

			n, readErr := multipartFile.Read(buffer)
			if readErr != nil && readErr != io.EOF {
				t.Fatalf("Failed to read file for encoding detection: %v", readErr)
			}

			// Test encoding validation with specified encoding
			ctx := context.Background()
			err = s.validateEncodingFromData(ctx, buffer[:n], tc.specifiedEncoding)

			if tc.expectSuccess {
				if err != nil {
					t.Errorf("Expected encoding validation to pass for %s with encoding %s, but got error: %v",
						tc.name, tc.specifiedEncoding, err)
				}
			} else {
				if err == nil {
					t.Errorf("Expected encoding validation to fail for %s with encoding %s, but it passed",
						tc.name, tc.specifiedEncoding)
				} else {
					t.Logf("Validation correctly failed for %s with encoding %s: %v",
						tc.name, tc.specifiedEncoding, err)
				}
			}
		})
	}
}
