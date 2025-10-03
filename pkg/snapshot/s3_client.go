package snapshot

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/aliengiraffe/spotdb/pkg/helpers"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Client wraps AWS S3 operations for snapshot management
type S3Client struct {
	client   *s3.Client
	uploader *manager.Uploader
}

// NewS3Client creates a new S3 client using default AWS configuration from environment
func NewS3Client(ctx context.Context) (*S3Client, error) {
	log := helpers.GetLoggerFromContext(ctx)

	// Load AWS configuration from environment variables
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Error("Failed to load AWS config", slog.Any("error", err))
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg)
	uploader := manager.NewUploader(client)

	log.Info("S3 client initialized successfully")

	return &S3Client{
		client:   client,
		uploader: uploader,
	}, nil
}

// ParseS3URI parses an S3 URI (s3://bucket/key/path) into bucket and key components
func ParseS3URI(s3URI string) (bucket, key string, err error) {
	if !strings.HasPrefix(s3URI, "s3://") {
		return "", "", fmt.Errorf("invalid S3 URI: must start with s3://")
	}

	// Remove s3:// prefix
	path := strings.TrimPrefix(s3URI, "s3://")

	// Split into bucket and key
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 2 {
		return "", "", fmt.Errorf("invalid S3 URI: must contain bucket and key (s3://bucket/key)")
	}

	bucket = parts[0]
	key = parts[1]

	if bucket == "" || key == "" {
		return "", "", fmt.Errorf("invalid S3 URI: bucket and key cannot be empty")
	}

	return bucket, key, nil
}

// DownloadSnapshot downloads a snapshot file from S3 to a local path
func (c *S3Client) DownloadSnapshot(ctx context.Context, s3URI, localPath string) error {
	log := helpers.GetLoggerFromContext(ctx)

	bucket, key, err := ParseS3URI(s3URI)
	if err != nil {
		return err
	}

	log.Info("Downloading snapshot from S3",
		slog.String("bucket", bucket),
		slog.String("key", key),
		slog.String("localPath", localPath))

	// Create the local file
	file, err := os.Create(localPath)
	if err != nil {
		log.Error("Failed to create local file", slog.Any("error", err))
		return fmt.Errorf("failed to create local file: %w", err)
	}
	defer file.Close()

	// Download the object from S3
	downloader := manager.NewDownloader(c.client)
	numBytes, err := downloader.Download(ctx, file, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		log.Error("Failed to download snapshot from S3", slog.Any("error", err))
		return fmt.Errorf("failed to download snapshot from S3: %w", err)
	}

	log.Info("Snapshot downloaded successfully",
		slog.String("bucket", bucket),
		slog.String("key", key),
		slog.Int64("bytes", numBytes))

	return nil
}

// UploadSnapshot uploads a local snapshot file to S3
func (c *S3Client) UploadSnapshot(ctx context.Context, localPath, bucket, key string) (string, error) {
	log := helpers.GetLoggerFromContext(ctx)

	log.Info("Uploading snapshot to S3",
		slog.String("bucket", bucket),
		slog.String("key", key),
		slog.String("localPath", localPath))

	// Open the local file
	file, err := os.Open(localPath)
	if err != nil {
		log.Error("Failed to open local file", slog.Any("error", err))
		return "", fmt.Errorf("failed to open local file: %w", err)
	}
	defer file.Close()

	// Get file info for size logging
	fileInfo, err := file.Stat()
	if err != nil {
		log.Error("Failed to get file info", slog.Any("error", err))
		return "", fmt.Errorf("failed to get file info: %w", err)
	}

	// Upload the file to S3
	result, err := c.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   file,
	})

	if err != nil {
		log.Error("Failed to upload snapshot to S3", slog.Any("error", err))
		return "", fmt.Errorf("failed to upload snapshot to S3: %w", err)
	}

	// Construct the S3 URI
	s3URI := fmt.Sprintf("s3://%s/%s", bucket, key)

	log.Info("Snapshot uploaded successfully",
		slog.String("bucket", bucket),
		slog.String("key", key),
		slog.Int64("bytes", fileInfo.Size()),
		slog.String("s3URI", s3URI),
		slog.String("location", result.Location))

	return s3URI, nil
}

// CopyFile is a helper function to copy files
func CopyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %w", err)
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	if err != nil {
		return fmt.Errorf("failed to copy file: %w", err)
	}

	// Sync to ensure data is written to disk
	err = destFile.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync destination file: %w", err)
	}

	return nil
}
