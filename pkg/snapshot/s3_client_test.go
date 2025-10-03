package snapshot

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseS3URI(t *testing.T) {
	tests := []struct {
		name        string
		s3URI       string
		wantBucket  string
		wantKey     string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid S3 URI with simple key",
			s3URI:       "s3://my-bucket/my-key",
			wantBucket:  "my-bucket",
			wantKey:     "my-key",
			expectError: false,
		},
		{
			name:        "valid S3 URI with nested key",
			s3URI:       "s3://my-bucket/path/to/snapshot.db",
			wantBucket:  "my-bucket",
			wantKey:     "path/to/snapshot.db",
			expectError: false,
		},
		{
			name:        "valid S3 URI with deeply nested key",
			s3URI:       "s3://production-backups/2024/01/15/db-snapshot.db",
			wantBucket:  "production-backups",
			wantKey:     "2024/01/15/db-snapshot.db",
			expectError: false,
		},
		{
			name:        "invalid URI - missing s3:// prefix",
			s3URI:       "http://my-bucket/my-key",
			expectError: true,
			errorMsg:    "invalid S3 URI: must start with s3://",
		},
		{
			name:        "invalid URI - no prefix",
			s3URI:       "my-bucket/my-key",
			expectError: true,
			errorMsg:    "invalid S3 URI: must start with s3://",
		},
		{
			name:        "invalid URI - missing key",
			s3URI:       "s3://my-bucket",
			expectError: true,
			errorMsg:    "invalid S3 URI: must contain bucket and key (s3://bucket/key)",
		},
		{
			name:        "invalid URI - missing key with trailing slash",
			s3URI:       "s3://my-bucket/",
			expectError: true,
			errorMsg:    "invalid S3 URI: bucket and key cannot be empty",
		},
		{
			name:        "invalid URI - empty bucket",
			s3URI:       "s3:///my-key",
			expectError: true,
			errorMsg:    "invalid S3 URI: bucket and key cannot be empty",
		},
		{
			name:        "invalid URI - only s3://",
			s3URI:       "s3://",
			expectError: true,
			errorMsg:    "invalid S3 URI: must contain bucket and key (s3://bucket/key)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, key, err := ParseS3URI(tt.s3URI)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
				assert.Empty(t, bucket)
				assert.Empty(t, key)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantBucket, bucket)
				assert.Equal(t, tt.wantKey, key)
			}
		})
	}
}

func TestCopyFile(t *testing.T) {
	// Create a temporary directory for test files
	tempDir := t.TempDir()

	t.Run("successful file copy", func(t *testing.T) {
		// Create source file with test content
		srcPath := filepath.Join(tempDir, "source.txt")
		content := []byte("test content for copy")
		err := os.WriteFile(srcPath, content, 0644)
		require.NoError(t, err)

		// Copy to destination
		dstPath := filepath.Join(tempDir, "destination.txt")
		err = CopyFile(srcPath, dstPath)
		require.NoError(t, err)

		// Verify destination file exists and has same content
		dstContent, err := os.ReadFile(dstPath)
		require.NoError(t, err)
		assert.Equal(t, content, dstContent)
	})

	t.Run("copy large file", func(t *testing.T) {
		// Create a larger file
		srcPath := filepath.Join(tempDir, "large-source.txt")
		content := make([]byte, 1024*1024) // 1MB
		for i := range content {
			content[i] = byte(i % 256)
		}
		err := os.WriteFile(srcPath, content, 0644)
		require.NoError(t, err)

		// Copy to destination
		dstPath := filepath.Join(tempDir, "large-destination.txt")
		err = CopyFile(srcPath, dstPath)
		require.NoError(t, err)

		// Verify destination file exists and has same content
		dstContent, err := os.ReadFile(dstPath)
		require.NoError(t, err)
		assert.Equal(t, content, dstContent)
	})

	t.Run("source file does not exist", func(t *testing.T) {
		srcPath := filepath.Join(tempDir, "nonexistent.txt")
		dstPath := filepath.Join(tempDir, "dest.txt")

		err := CopyFile(srcPath, dstPath)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to open source file")
	})

	t.Run("destination directory does not exist", func(t *testing.T) {
		// Create source file
		srcPath := filepath.Join(tempDir, "src.txt")
		err := os.WriteFile(srcPath, []byte("content"), 0644)
		require.NoError(t, err)

		// Try to copy to non-existent directory
		dstPath := filepath.Join(tempDir, "nonexistent-dir", "dest.txt")
		err = CopyFile(srcPath, dstPath)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create destination file")
	})

	t.Run("overwrite existing destination file", func(t *testing.T) {
		// Create source file
		srcPath := filepath.Join(tempDir, "overwrite-src.txt")
		newContent := []byte("new content")
		err := os.WriteFile(srcPath, newContent, 0644)
		require.NoError(t, err)

		// Create existing destination file
		dstPath := filepath.Join(tempDir, "overwrite-dst.txt")
		oldContent := []byte("old content")
		err = os.WriteFile(dstPath, oldContent, 0644)
		require.NoError(t, err)

		// Copy should overwrite
		err = CopyFile(srcPath, dstPath)
		require.NoError(t, err)

		// Verify new content
		dstContent, err := os.ReadFile(dstPath)
		require.NoError(t, err)
		assert.Equal(t, newContent, dstContent)
	})

	t.Run("copy empty file", func(t *testing.T) {
		// Create empty source file
		srcPath := filepath.Join(tempDir, "empty-src.txt")
		err := os.WriteFile(srcPath, []byte{}, 0644)
		require.NoError(t, err)

		// Copy to destination
		dstPath := filepath.Join(tempDir, "empty-dst.txt")
		err = CopyFile(srcPath, dstPath)
		require.NoError(t, err)

		// Verify destination is also empty
		dstContent, err := os.ReadFile(dstPath)
		require.NoError(t, err)
		assert.Empty(t, dstContent)
	})

	t.Run("copy with special characters in filename", func(t *testing.T) {
		// Create source file with special chars in name
		srcPath := filepath.Join(tempDir, "file with spaces & special.txt")
		content := []byte("special chars test")
		err := os.WriteFile(srcPath, content, 0644)
		require.NoError(t, err)

		// Copy to destination
		dstPath := filepath.Join(tempDir, "dest-with-special.txt")
		err = CopyFile(srcPath, dstPath)
		require.NoError(t, err)

		// Verify content
		dstContent, err := os.ReadFile(dstPath)
		require.NoError(t, err)
		assert.Equal(t, content, dstContent)
	})
}

func TestCopyFile_Permissions(t *testing.T) {
	tempDir := t.TempDir()

	t.Run("copy file with different permissions", func(t *testing.T) {
		// Create source file with specific permissions
		srcPath := filepath.Join(tempDir, "perm-source.txt")
		content := []byte("permission test")
		err := os.WriteFile(srcPath, content, 0600)
		require.NoError(t, err)

		// Copy to destination
		dstPath := filepath.Join(tempDir, "perm-dest.txt")
		err = CopyFile(srcPath, dstPath)
		require.NoError(t, err)

		// Verify content was copied
		dstContent, err := os.ReadFile(dstPath)
		require.NoError(t, err)
		assert.Equal(t, content, dstContent)
	})
}

func TestParseS3URI_EdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		s3URI       string
		wantBucket  string
		wantKey     string
		expectError bool
	}{
		{
			name:        "URI with query parameters (not standard)",
			s3URI:       "s3://bucket/key?param=value",
			wantBucket:  "bucket",
			wantKey:     "key?param=value",
			expectError: false,
		},
		{
			name:        "URI with fragment",
			s3URI:       "s3://bucket/key#fragment",
			wantBucket:  "bucket",
			wantKey:     "key#fragment",
			expectError: false,
		},
		{
			name:        "URI with encoded characters",
			s3URI:       "s3://bucket/path%20with%20spaces/file.db",
			wantBucket:  "bucket",
			wantKey:     "path%20with%20spaces/file.db",
			expectError: false,
		},
		{
			name:        "very long key path",
			s3URI:       "s3://bucket/" + filepath.Join("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "file.db"),
			wantBucket:  "bucket",
			wantKey:     filepath.Join("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "file.db"),
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, key, err := ParseS3URI(tt.s3URI)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantBucket, bucket)
				assert.Equal(t, tt.wantKey, key)
			}
		})
	}
}

func TestS3Client_DownloadSnapshot_Validation(t *testing.T) {
	ctx := context.Background()

	t.Run("invalid S3 URI", func(t *testing.T) {
		// Create a temporary directory for the test
		tempDir := t.TempDir()
		localPath := filepath.Join(tempDir, "downloaded.db")

		// Create a minimal S3 client (will fail at AWS connection but that's ok)
		// We're testing the validation logic before AWS calls
		client := &S3Client{
			client:   nil, // nil client will cause failure, but we test URI parsing first
			uploader: nil,
		}

		// Test with invalid URI
		err := client.DownloadSnapshot(ctx, "invalid-uri", localPath)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid S3 URI")
	})

	t.Run("empty S3 URI", func(t *testing.T) {
		tempDir := t.TempDir()
		localPath := filepath.Join(tempDir, "downloaded.db")

		client := &S3Client{
			client:   nil,
			uploader: nil,
		}

		err := client.DownloadSnapshot(ctx, "", localPath)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid S3 URI")
	})

	t.Run("missing bucket in URI", func(t *testing.T) {
		tempDir := t.TempDir()
		localPath := filepath.Join(tempDir, "downloaded.db")

		client := &S3Client{
			client:   nil,
			uploader: nil,
		}

		err := client.DownloadSnapshot(ctx, "s3://", localPath)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid S3 URI")
	})
}

func TestS3Client_UploadSnapshot_Validation(t *testing.T) {
	ctx := context.Background()

	t.Run("file does not exist", func(t *testing.T) {
		tempDir := t.TempDir()
		nonExistentPath := filepath.Join(tempDir, "nonexistent.db")

		client := &S3Client{
			client:   nil,
			uploader: nil,
		}

		// This should fail when trying to open the file
		_, err := client.UploadSnapshot(ctx, nonExistentPath, "test-bucket", "test-key")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to open local file")
	})

	t.Run("empty local path", func(t *testing.T) {
		client := &S3Client{
			client:   nil,
			uploader: nil,
		}

		// This should fail when trying to open an empty path
		_, err := client.UploadSnapshot(ctx, "", "test-bucket", "test-key")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to open local file")
	})
}

// Note: Full integration tests for NewS3Client, DownloadSnapshot, and UploadSnapshot require:
// - AWS credentials/config (via environment or mock)
// - AWS SDK mocking with libraries like aws-sdk-go-v2/service/s3
// - Or integration testing with localstack using testcontainers-go
//
// The validation tests above cover the error handling before AWS SDK calls.
// For production systems, consider adding:
// 1. Mock-based unit tests with AWS SDK mocks
// 2. Integration tests with localstack
// 3. Contract tests against real S3 with test credentials
