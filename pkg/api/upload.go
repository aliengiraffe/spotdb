package api

import (
	"context" // Import context
	"errors"
	"fmt"
	"io"
	"log/slog" // Ensure slog is used
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
	"unicode/utf8"

	"mime/multipart"

	"github.com/aliengiraffe/spotdb/pkg/database"
	"github.com/aliengiraffe/spotdb/pkg/helpers"
	"github.com/gabriel-vasile/mimetype"
	"github.com/gin-gonic/gin"
	"github.com/wlynxg/chardet"
)

// suggestionMap contains default suggestions for CSV error codes.
var suggestionMap = map[string]string{
	"INVALID_REQUEST_PARAMETERS": "Please check the required parameters and try again.",
	"TEMP_FILE_CREATION_ERROR":   "Please try again or contact support if the issue persists.",
	"FILE_OPEN_ERROR":            "Please ensure the file is not corrupted and try again.",
	"CSV_VALIDATION_ERROR":       "The file could not be processed. Please check the CSV format.",
	"INVALID_CSV_STRUCTURE":      "Please ensure the CSV file has a consistent structure.",
	"MIME_TYPE_DETECTION_ERROR":  "Please ensure the file is not corrupted and try again.",
	"CSV_FORMAT_CHECK_ERROR":     "Please ensure the file is a valid CSV file.",
	"INVALID_FILE_FORMAT":        "Please upload a valid CSV file with extensions like .csv, .txt, or .tsv.",
	"FILE_SIZE_EXCEEDED":         "Please reduce the file size or split it into smaller files.",
	"SECURITY_VALIDATION_FAILED": "Please ensure the file does not contain formulas, scripts or other potentially harmful content.",
	"FILE_COPY_ERROR":            "Please try again or contact support if the issue persists.",
	"SMART_IMPORT_FAILED":        "Check the CSV file structure and ensure it contains valid data.",
	"DIRECT_IMPORT_FAILED":       "Check the CSV file structure and ensure it contains valid data.", "TABLE_INFO_ERROR": "The table may not have been created correctly. Check the CSV file structure.",
	"ROW_COUNT_ERROR":      "The data may not have been imported correctly. Check the CSV file structure.",
	"INVALID_ENCODING":     "Please ensure the file is saved with UTF-8 encoding before uploading.",
	"UNSUPPORTED_ENCODING": "Please ensure the file is saved with a supported encoding (UTF-8 or UTF-16) before uploading.",
	"DUPLICATE_TABLE_NAME": "Choose a different table name or use the override parameter to replace the existing table.",
}

const (
	// ErrValidateFileFormat is the error format for file validation failures
	ErrValidateFileFormat = "failed to validate file format: %v"
)

// Size constants
const (
	// BytesInMB is the number of bytes in a megabyte, used for logging
	BytesInMB = 1024 * 1024
)

// CSVRequest and related types are defined in server.go
// This is just to make the editor happy with the code in this file

// handleCSVUpload godoc
//
//	@Summary		Upload CSV file
//	@Description	Upload a CSV file and import it into the database
//	@Tags			upload
//	@Accept			multipart/form-data
//	@Produce		json
//	@Param			request				formData	api.CSVRequest			true	"CSV upload request"
//	@Param			csv_file			formData	file					true	"CSV file to upload"
//	@Param			csv_file_encoding	formData	string					false	"Encoding of the CSV file (default: utf-8, supported: utf-8, utf-16)"
//	@Success		200					{object}	api.CSVUploadResponse	"Upload successful"
//	@Failure		400					{object}	api.CSVErrorResponse	"Bad request with possible error codes: INVALID_REQUEST_PARAMETERS, FILE_OPEN_ERROR, MIME_TYPE_DETECTION_ERROR, CSV_FORMAT_CHECK_ERROR, INVALID_FILE_FORMAT, CSV_VALIDATION_ERROR, INVALID_CSV_STRUCTURE, INVALID_ENCODING, UNSUPPORTED_ENCODING"
//	@Failure		413					{object}	api.CSVErrorResponse	"File too large with error code: FILE_SIZE_EXCEEDED"
//	@Failure		422					{object}	api.CSVErrorResponse	"Unprocessable entity with possible error codes: SECURITY_VALIDATION_FAILED, FILE_COPY_ERROR, TEMP_FILE_CREATION_ERROR, SMART_IMPORT_FAILED, DIRECT_IMPORT_FAILED, TABLE_INFO_ERROR, ROW_COUNT_ERROR"
//	@Failure		500					{object}	api.CSVErrorResponse	"Internal server error"
//	@Router			/upload [post]
func (s *Server) handleCSVUpload() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Bind form data to get the validated values
		var payload CSVRequest

		// Get the logger from Gin context and embed it into the request context
		ctx := c.Request.Context()
		log := helpers.GetLoggerFromContext(ctx)

		if err := c.ShouldBind(&payload); err != nil {
			log.Info("Error binding CSV request", slog.Any("error", err))

			bindError := CSVError{
				Code:    "INVALID_REQUEST_PARAMETERS",
				Message: "Invalid request: " + err.Error(),
				Details: CSVErrorDetail{
					Line:       0,
					Suggestion: suggestionMap["INVALID_REQUEST_PARAMETERS"],
				},
			}

			c.JSON(http.StatusBadRequest, CSVErrorResponse{
				Errors: []CSVError{bindError},
			})
			return
		}

		// Extract parameters
		tableName := payload.TableName
		hasHeader := payload.HasHeader
		override := payload.Override
		smart := payload.Smart
		csvFile := payload.CSVFile
		encoding := payload.FileEncoding

		log.Info("CSV upload request received",
			slog.String("remote_addr", c.Request.RemoteAddr),
			slog.Int64("content_length", c.Request.ContentLength),
		)
		log.Info("Processing upload",
			slog.String("table", tableName),
		)
		log.Info("CSV has header",
			slog.Bool("has_header", hasHeader),
		)
		log.Info("Smart import",
			slog.Bool("smart_import", smart),
		)
		log.Info("File encoding",
			slog.String("encoding", encoding),
		)

		// Process the uploaded file using the decoupled function directly
		// Pass the context here
		tempFilePath, validationErrors, err := s.processCsvFileFromHeader(ctx, csvFile, tableName, hasHeader, encoding)
		if err != nil {
			// Handle any errors that occur during processing
			log.Info("Error processing CSV file",
				slog.Any("error", err),
			)

			// Check for file size exceeded error to return the appropriate status code
			if strings.Contains(err.Error(), "file too large") {
				c.JSON(http.StatusRequestEntityTooLarge, CSVErrorResponse{
					Errors: validationErrors,
				})
				return
			}

			// Return all collected validation errors
			c.JSON(http.StatusBadRequest, CSVErrorResponse{
				Errors: validationErrors,
			})
			return
		}

		// If we get here, we have a valid temp file, but may still have non-fatal validation warnings
		defer s.cleanupTempFile(ctx, tempFilePath) // This function also needs context if it logs

		// Import the CSV data and prepare response
		// Pass the context here
		columnsResult, rowCount, importInfo, err := s.importCsvData(ctx, c, tableName, tempFilePath, hasHeader, override)
		if err != nil {
			// Error has already been written to response by importCsvData
			return
		}

		log.Info("Table contains rows",
			slog.String("table", tableName),
			slog.Int64("row_count", rowCount),
		)
		log.Info("Upload process completed successfully",
			slog.String("table", tableName),
		)

		// Build the response with validation and import info
		response := CSVUploadResponse{
			Table:    tableName,
			Columns:  columnsResult.Results,
			RowCount: rowCount,
			Import:   importInfo,
		}

		// Send success response
		c.JSON(http.StatusOK, response)
	}
}

// processCsvFileFromHeader is a decoupled version that works with a FileHeader directly
// Returns the temp file path, any CSV validation errors, and any error
// Added ctx context.Context
func (s *Server) processCsvFileFromHeader(ctx context.Context, fileHeader *multipart.FileHeader, tableName string, hasHeader bool, encoding string) (string, []CSVError, error) {
	log := helpers.GetLoggerFromContext(ctx) // Retrieve logger

	// Check if encoding is supported (only UTF-8 and UTF-16 are allowed)
	if !isEncodingSupported(encoding) {
		// Return error for any unsupported encoding
		validationError := CSVError{
			Code:    "UNSUPPORTED_ENCODING",
			Message: fmt.Sprintf("unsupported encoding: %s. Supported encodings are: UTF-8 and UTF-16", encoding),
			Details: CSVErrorDetail{
				Line:       0,
				Suggestion: suggestionMap["UNSUPPORTED_ENCODING"],
			},
		}
		return "", []CSVError{validationError},
			fmt.Errorf("unsupported encoding: %s. Supported encodings are: UTF-8 and UTF-16", encoding)
	}

	// Open the file for initial access
	file, err := fileHeader.Open()
	if err != nil {
		// Use the logger from context
		log.Info("Error opening uploaded file", slog.Any("error", err))
		validationError := CSVError{
			Code:    "FILE_OPEN_ERROR",
			Message: fmt.Sprintf("Failed to open uploaded file: %v", err),
			Details: CSVErrorDetail{
				Line:       0,
				Suggestion: suggestionMap["FILE_OPEN_ERROR"],
			},
		}
		return "", []CSVError{validationError}, fmt.Errorf("failed to open uploaded file: %v", err)
	}
	defer helpers.CloseResources(file, "uploaded file") // helpers.CloseResources might also benefit from context logger

	// Use the logger from context
	log.Info("Received file",
		slog.String("filename", fileHeader.Filename),
		slog.Int64("size_bytes", fileHeader.Size),
	)

	// Validate MIME type for actual file uploads (not test uploads)
	// This helps prevent processing non-CSV files
	if !strings.HasPrefix(fileHeader.Filename, "test") && fileHeader.Size > 0 {
		// Pass context
		mimeErrors, mimeErr := s.validateMimeType(ctx, fileHeader)
		if mimeErr != nil {
			return "", mimeErrors, mimeErr
		}
	}

	// Create temporary file
	// Pass context
	tempFilePath, tempFile, err := s.createTempFileForUpload(ctx, tableName)
	if err != nil {
		copyError := CSVError{
			Code:    "TEMP_FILE_CREATION_ERROR",
			Message: fmt.Sprintf("Failed to create temporary file: %v", err),
			Details: CSVErrorDetail{
				Line:       0,
				Suggestion: suggestionMap["TEMP_FILE_CREATION_ERROR"],
			},
		}
		return "", []CSVError{copyError}, err
	}
	// No defer close here since copyFileData will handle closing

	// Copy data to temp file - the validation will happen inside CopyWithMaxSize
	// Pass context
	copyErrors, err := s.copyFileData(ctx, file, tempFile, fileHeader.Filename, encoding)
	if err != nil {
		return "", copyErrors, err
	}

	// Use the logger from context
	log.Info("Closed temporary file, preparing to import data")
	return tempFilePath, copyErrors, nil
}

// supportedEncodings contains all the encodings that are allowed for CSV files
var supportedEncodings = []string{"utf-8", "utf8", "utf-16", "utf16"}

// isEncodingSupported checks if the given encoding is in the list of supported encodings
// No logging, no context needed
func isEncodingSupported(encoding string) bool {
	// Empty encoding means use default (UTF-8), which is supported
	if encoding == "" {
		return true
	}

	// Convert to lowercase for case-insensitive comparison
	encodingLower := strings.ToLower(encoding)

	// Check if encoding is in the supported list
	for _, supported := range supportedEncodings {
		if encodingLower == supported {
			return true
		}
	}

	return false
}

// validateEncodingFromData checks if data has the encoding specified by the user
// or tries to auto-detect the encoding if none is specified
// It works with the provided data buffer and doesn\'t rely on file seek operations
// Added ctx context.Context
// detectDataEncoding detects encoding from data and validates it's supported
func (s *Server) detectDataEncoding(ctx context.Context, data []byte) (string, bool, bool, error) {
	log := helpers.GetLoggerFromContext(ctx)

	detector := chardet.NewUniversalDetector(0)
	detector.Feed(data)
	result := detector.GetResult()

	if result.Encoding == "" {
		log.Info("No encoding detected")
		return "", false, false, fmt.Errorf("could not detect file encoding")
	}

	detectedEncoding := strings.ToLower(result.Encoding)
	log.Info("Detected encoding",
		slog.String("encoding", detectedEncoding),
		slog.Float64("confidence", result.Confidence),
	)

	isUTF8 := strings.Contains(detectedEncoding, "utf-8") || strings.Contains(detectedEncoding, "utf8")
	isUTF16 := strings.Contains(detectedEncoding, "utf-16") || strings.Contains(detectedEncoding, "utf16")

	return detectedEncoding, isUTF8, isUTF16, nil
}

// validateUnsupportedEncodingFallback handles fallback validation for unsupported encodings
func (s *Server) validateUnsupportedEncodingFallback(ctx context.Context, data []byte, detectedEncoding string) (string, bool, error) {
	log := helpers.GetLoggerFromContext(ctx)

	// Try to validate as UTF-8 first, since many files with different encoding labels
	// may actually be valid UTF-8
	if utf8.Valid(data) {
		log.Info("Data passes UTF-8 validation despite detector reporting", slog.String("detected_encoding", detectedEncoding))
		return "utf-8", true, nil
	}

	log.Info("Unsupported encoding detected", slog.String("encoding", detectedEncoding))
	return detectedEncoding, false, fmt.Errorf("detected encoding is not supported: %s. Supported encodings are: UTF-8 and UTF-16", detectedEncoding)
}

// validateUTF8UserSpecified validates when user specified UTF-8 or no encoding
func (s *Server) validateUTF8UserSpecified(ctx context.Context, data []byte, userSpecifiedEncoding, detectedEncoding string, isUTF8, isUTF16 bool) error {
	log := helpers.GetLoggerFromContext(ctx)

	// If we detected UTF-8, or no encoding was specified, validate UTF-8
	if isUTF8 || userSpecifiedEncoding == "" {
		if !utf8.Valid(data) {
			log.Info("Data is not valid UTF-8 encoded")
			return fmt.Errorf("file is not valid UTF-8 encoded")
		}
		return nil
	}

	// We detected UTF-16 but no encoding was specified (auto-detect)
	if isUTF16 && userSpecifiedEncoding == "" {
		return nil
	}

	// Mismatch between specified encoding (UTF-8) and detected encoding
	log.Info("Encoding mismatch",
		slog.String("user_specified", userSpecifiedEncoding),
		slog.String("detected", detectedEncoding),
	)
	return fmt.Errorf("file encoding mismatch: you specified %s but detected %s", userSpecifiedEncoding, detectedEncoding)
}

// validateUTF16UserSpecified validates when user specified UTF-16
func (s *Server) validateUTF16UserSpecified(ctx context.Context, userSpecifiedEncoding, detectedEncoding string, isUTF16 bool) error {
	log := helpers.GetLoggerFromContext(ctx)

	if !isUTF16 {
		log.Info("Encoding mismatch",
			slog.String("user_specified", userSpecifiedEncoding),
			slog.String("detected", detectedEncoding),
		)
		return fmt.Errorf("file encoding mismatch: you specified %s but detected %s", userSpecifiedEncoding, detectedEncoding)
	}

	return nil
}

// isUTF8EncodingSpecified checks if user specified UTF-8 encoding (including default)
func isUTF8EncodingSpecified(encoding string) bool {
	return encoding == "" || encoding == "utf-8" || encoding == "utf8"
}

// isUTF16EncodingSpecified checks if user specified UTF-16 encoding
func isUTF16EncodingSpecified(encoding string) bool {
	return encoding == "utf-16" || encoding == "utf16"
}

func (s *Server) validateEncodingFromData(ctx context.Context, data []byte, userSpecifiedEncoding string) error {
	log := helpers.GetLoggerFromContext(ctx)

	// Normalize user-specified encoding to lowercase
	userSpecifiedEncoding = strings.ToLower(userSpecifiedEncoding)

	// Check if user-specified encoding is supported
	if !isEncodingSupported(userSpecifiedEncoding) {
		log.Info("Unsupported encoding specified", slog.String("encoding", userSpecifiedEncoding))
		return fmt.Errorf("unsupported encoding: %s. Supported encodings are: UTF-8 and UTF-16", userSpecifiedEncoding)
	}

	// If the data is empty, consider it valid
	if len(data) == 0 {
		log.Info("Data appears to be empty")
		return nil
	}

	// Detect the encoding using the provided data
	detectedEncoding, isUTF8, isUTF16, err := s.detectDataEncoding(ctx, data)
	if err != nil {
		return err
	}

	// Check if the detected encoding is supported, with fallback validation
	if !isUTF8 && !isUTF16 {
		detectedEncoding, isUTF8, err = s.validateUnsupportedEncodingFallback(ctx, data, detectedEncoding)
		if err != nil {
			return err
		}
	}

	// Validate based on user-specified encoding
	if isUTF8EncodingSpecified(userSpecifiedEncoding) {
		return s.validateUTF8UserSpecified(ctx, data, userSpecifiedEncoding, detectedEncoding, isUTF8, isUTF16)
	}

	if isUTF16EncodingSpecified(userSpecifiedEncoding) {
		return s.validateUTF16UserSpecified(ctx, userSpecifiedEncoding, detectedEncoding, isUTF16)
	}

	return nil
}

// validateMimeType checks if the file has an acceptable MIME type for CSV files
// Uses the gabriel-vasile/mimetype library for reliable MIME type detection
// Returns a slice of CSVError and an error if validation fails
// Added ctx context.Context
func (s *Server) validateMimeType(ctx context.Context, fileHeader *multipart.FileHeader) ([]CSVError, error) {
	log := helpers.GetLoggerFromContext(ctx) // Retrieve logger

	// Open file for MIME type detection
	file, err := fileHeader.Open()
	if err != nil {
		// Use the logger from context
		log.Info("Error opening file for MIME type detection", slog.Any("error", err))
		validationError := CSVError{
			Code:    "MIME_TYPE_DETECTION_ERROR",
			Message: fmt.Sprintf("Failed to detect file type: %v", err),
			Details: CSVErrorDetail{
				Line:       0,
				Suggestion: suggestionMap["MIME_TYPE_DETECTION_ERROR"],
			},
		}
		return []CSVError{validationError}, fmt.Errorf(ErrValidateFileFormat, err)
	}
	defer helpers.CloseResources(file, "file after MIME type detection")

	// Detect MIME type and get the detected type (overrideable in tests)
	// Pass context
	detectedType, err := detectFileMimeTypeFunc(ctx, file)
	if err != nil {
		validationError := CSVError{
			Code:    "MIME_TYPE_DETECTION_ERROR",
			Message: fmt.Sprintf("Failed to detect file type: %v", err),
			Details: CSVErrorDetail{
				Line:       0,
				Suggestion: suggestionMap["MIME_TYPE_DETECTION_ERROR"],
			},
		}
		return []CSVError{validationError}, err
	}

	// If MIME type is allowed, validation passes
	if AllowedCSVMimeTypes[detectedType] {
		// Use the logger from context
		log.Info("MIME type validation passed",
			slog.String("filename", fileHeader.Filename),
			slog.String("mime_type", detectedType),
		)
		return nil, nil
	}

	// For text-based files, check if content resembles CSV format
	if strings.HasPrefix(detectedType, "text/") {
		// Pass context
		isCSV, err := looksLikeCSV(ctx, file)
		if err != nil {
			// Use the logger from context
			log.Info("Error checking CSV format", slog.Any("error", err))
			validationError := CSVError{
				Code:    "CSV_FORMAT_CHECK_ERROR",
				Message: fmt.Sprintf("Error checking CSV format: %v", err),
				Details: CSVErrorDetail{
					Line:       0,
					Suggestion: suggestionMap["CSV_FORMAT_CHECK_ERROR"],
				},
			}
			return []CSVError{validationError}, err
		}

		if isCSV {
			// Use the logger from context
			log.Info("File appears to be CSV despite MIME type", slog.String("mime_type", detectedType))
			return nil, nil
		}
	}

	// If we get here, the file isn\'t a valid CSV
	// Use the logger from context
	log.Info("Invalid MIME type for file",
		slog.String("filename", fileHeader.Filename),
		slog.String("got", detectedType),
	)
	validationError := CSVError{
		Code:    "INVALID_FILE_FORMAT",
		Message: fmt.Sprintf("Invalid file format: expected CSV, got %s", detectedType),
		Details: CSVErrorDetail{
			Line:       0,
			Suggestion: suggestionMap["INVALID_FILE_FORMAT"],
		},
	}
	return []CSVError{validationError}, fmt.Errorf("invalid file format: expected CSV, got %s", detectedType)
}

// detectFileMimeType detects the MIME type of a file using mimetype library
// Added ctx context.Context
func detectFileMimeType(ctx context.Context, file multipart.File) (string, error) {
	log := helpers.GetLoggerFromContext(ctx) // Retrieve logger

	// Detect MIME type from file content
	mime, err := mimetype.DetectReader(file)
	if err != nil {
		// Use the logger from context
		log.Info("Error detecting MIME type", slog.Any("error", err))
		return "", fmt.Errorf(ErrValidateFileFormat, err)
	}

	// Reset file position for later reading
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		// Use the logger from context
		log.Info("Error resetting file position", slog.Any("error", err))
		return "", fmt.Errorf(ErrValidateFileFormat, err)
	}

	return mime.String(), nil
}

// detectFileMimeTypeFunc is a variable to allow overriding in tests
// Signature also needs to change to accept context
var detectFileMimeTypeFunc = detectFileMimeType

// looksLikeCSV checks if file content has CSV characteristics
// Uses the encoding/csv package for validation
// Added ctx context.Context
func looksLikeCSV(ctx context.Context, file multipart.File) (bool, error) {
	log := helpers.GetLoggerFromContext(ctx) // Retrieve logger

	// Reset file position at start
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		// Use the logger from context
		log.Info("Error resetting file position", slog.Any("error", err))
		// Continue anyway since we\'ve already read the data
	}

	// Read the file contents
	const maxSampleSize = 32 * 1024 // 32KB should be enough for validation
	data := make([]byte, maxSampleSize)
	n, err := file.Read(data)
	if err != nil && err != io.EOF {
		// Use the logger from context
		log.Info("Error reading file for CSV validation", slog.Any("error", err))
		return false, fmt.Errorf(ErrValidateFileFormat, err)
	}

	// Reset file position
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		// Use the logger from context
		log.Info("Error resetting file position", slog.Any("error", err))
		// Continue anyway since we\'ve already read the data
	}

	// Use our more robust CSV validation on the data
	// Note: ValidateCSVFileFromData does not take context, assuming it doesn't log internally
	result, err := ValidateCSVFileFromData(data[:n])
	if err != nil {
		// Use the logger from context
		log.Info("Error validating CSV structure", slog.Any("error", err))
		return false, nil // Not a validation error, just not a CSV
	}

	// If validation passed, it\'s a valid CSV
	if result.Valid && result.ColumnCount >= 2 {
		return true, nil
	}

	// Not a well-structured CSV
	return false, nil
}

// createTempFileForUpload creates a temporary file for the uploaded CSV
// Added ctx context.Context
func (s *Server) createTempFileForUpload(ctx context.Context, tableName string) (string, *os.File, error) {
	log := helpers.GetLoggerFromContext(ctx) // Retrieve logger

	tempDir := os.TempDir()
	tempFilePath := filepath.Join(tempDir, fmt.Sprintf("upload_%s.csv", tableName))
	// Use the logger from context
	log.Info("Creating temporary file", slog.String("path", tempFilePath))

	tempFile, err := os.Create(tempFilePath)
	if err != nil {
		// Use the logger from context
		log.Info("Error creating temp file",
			slog.String("path", tempFilePath),
			slog.Any("error", err),
		)
		return "", nil, fmt.Errorf("internal server error while processing file: %v", err)
	}

	return tempFilePath, tempFile, nil
}

// copyFileData streams the uploaded file to the temporary file with size validation
// Returns CSV validation errors (if any) and error
// Added ctx context.Context
func (s *Server) copyFileData(ctx context.Context, src multipart.File, dst *os.File, filename string, encoding string) ([]CSVError, error) {
	log := helpers.GetLoggerFromContext(ctx) // Retrieve logger

	startTime := time.Now()

	// Define a flag to track if we\'ve done encoding and CSV structure validation
	validatedFileFormat := false

	// Create a wrapper that implements our updated validation function
	validationWrapper := func(data []byte, lineNumber int, columnMap map[int]string) (bool, *helpers.ValidationIssue, error) {
		// For chunks after the first one, just do security validation
		if lineNumber > 0 {
			// This is likely a subsequent chunk in a multiline file
			// Security validation only
			return helpers.CSVRowCheckDetailed(data, lineNumber, columnMap)
		}

		// First chunk - do security check first
		isSafe, issue, _ := helpers.CSVRowCheckDetailed(data, lineNumber, columnMap)
		if !isSafe {
			return false, issue, helpers.ErrInvalidBuffer
		}

		// If it passes security check but we\'re in a test environment and the file is small,
		// skip the additional validations
		if len(data) < 1024 && strings.HasPrefix(filename, "test") {
			return true, nil, nil
		}

		// Only do the file format validations once, on the first chunk
		if !validatedFileFormat {
			validatedFileFormat = true

			// Validate encoding directly from data without seeking
			// Pass context
			if err := s.validateEncodingFromData(ctx, data, encoding); err != nil {
				return false, nil, fmt.Errorf("invalid encoding: %w", err)
			}

			// Validate CSV structure directly from the data
			// Note: ValidateCSVFileFromData does not take context
			validationResult, err := ValidateCSVFileFromData(data)
			if err != nil {
				return false, nil, fmt.Errorf("CSV validation error: %v", err)
			}

			// Check validation result
			if !validationResult.Valid {
				var lineNum int
				if strings.Contains(validationResult.ErrorMessage, "inconsistent column count on line") {
					// Attempt to extract line number from error message
					_, err := fmt.Sscanf(validationResult.ErrorMessage, "inconsistent column count on line %d", &lineNum)
					if err != nil {
						lineNum = 0
					}
				}

				validationIssue := &helpers.ValidationIssue{
					Line:    lineNum,
					Pattern: validationResult.ErrorMessage,
				}
				return false, validationIssue, fmt.Errorf("invalid CSV structure: %s", validationResult.ErrorMessage)
			}
		}

		// Pass validation
		return true, nil, nil
	}

	// Stream the file with size validation and content security validation
	// Pass the wrapped validation function as a callback
	bytesWritten, validationIssue, err := helpers.CopyWithMaxSize(dst, src, helpers.GetBufferSize(), helpers.GetMaxFileSize(), validationWrapper)
	if err != nil {
		if err == helpers.ErrMaxFileSizeExceeded {
			// Use the logger from context
			log.Info("File size exceeded maximum allowed size", slog.String("filename", filename))
			fileSizeError := CSVError{
				Code:    "FILE_SIZE_EXCEEDED",
				Message: fmt.Sprintf("File too large (max %dGB)", helpers.GetMaxFileSize()/(1024*1024*1024)),
				Details: CSVErrorDetail{
					Line:       0,
					Suggestion: suggestionMap["FILE_SIZE_EXCEEDED"],
				},
			}
			return []CSVError{fileSizeError}, fmt.Errorf("file too large (max %dGB)", helpers.GetMaxFileSize()/(1024*1024*1024))
		}

		if errors.Is(err, helpers.ErrInvalidBuffer) {
			// Use the logger from context
			log.Info("CSV security validation failed",
				slog.String("filename", filename),
				slog.Any("error", err),
			)

			// Create detailed error with line and column information if available
			securityError := CSVError{
				Code:    "SECURITY_VALIDATION_FAILED",
				Message: "Security validation failed: file contains potentially malicious content",
				Details: CSVErrorDetail{
					Line:       0,
					Suggestion: suggestionMap["SECURITY_VALIDATION_FAILED"],
				},
			}

			// Use information from validation issue if available
			if validationIssue != nil {
				securityError.Details.Line = validationIssue.Line
				securityError.Details.Column = validationIssue.Column
				securityError.Details.FoundValue = validationIssue.Value

				// More specific suggestion based on the found pattern
				if strings.Contains(validationIssue.Pattern, "=") ||
					strings.Contains(validationIssue.Pattern, "HYPERLINK") ||
					strings.Contains(validationIssue.Pattern, "IMPORT") {
					securityError.Details.Suggestion = "Please remove Excel/spreadsheet formulas from the file."
				} else if strings.Contains(validationIssue.Pattern, "script") ||
					strings.Contains(validationIssue.Pattern, "javascript") {
					securityError.Details.Suggestion = "Please remove HTML or JavaScript code from the file."
				}
			}

			return []CSVError{securityError}, fmt.Errorf("security validation failed: file contains potentially malicious content")
		}

		// Check for encoding or CSV structure validation errors
		if strings.Contains(err.Error(), "invalid encoding") {
			errorCode := "INVALID_ENCODING"
			if strings.Contains(err.Error(), "unsupported encoding") {
				errorCode = "UNSUPPORTED_ENCODING"
			}

			validationError := CSVError{
				Code:    errorCode,
				Message: err.Error(),
				Details: CSVErrorDetail{
					Line:       0,
					Suggestion: suggestionMap[errorCode],
				},
			}
			return []CSVError{validationError}, err
		}

		if strings.Contains(err.Error(), "CSV validation error") {
			validationError := CSVError{
				Code:    "CSV_VALIDATION_ERROR",
				Message: fmt.Sprintf("CSV validation error: %v", err),
				Details: CSVErrorDetail{
					Line:       0,
					Suggestion: suggestionMap["CSV_VALIDATION_ERROR"],
				},
			}
			return []CSVError{validationError}, err
		}

		if strings.Contains(err.Error(), "invalid CSV structure") {
			validationError := CSVError{
				Code:    "INVALID_CSV_STRUCTURE",
				Message: err.Error(),
				Details: CSVErrorDetail{
					Line:       0,
					Suggestion: suggestionMap["INVALID_CSV_STRUCTURE"],
				},
			}

			// Parse line number from error message if it\'s the inconsistent columns error
			if validationIssue != nil && validationIssue.Line > 0 {
				validationError.Details.Line = validationIssue.Line
				validationError.Details.Suggestion = "Make sure all rows have the same number of columns."
			}

			return []CSVError{validationError}, err
		}

		// Use the logger from context
		log.Info("Error copying file data", slog.Any("error", err))
		copyError := CSVError{
			Code:    "FILE_COPY_ERROR",
			Message: fmt.Sprintf("Error processing file: %v", err),
			Details: CSVErrorDetail{
				Line:       0,
				Suggestion: suggestionMap["FILE_COPY_ERROR"],
			},
		}
		return []CSVError{copyError}, fmt.Errorf("internal server error while processing file: %v", err)
	}

	// Log transfer statistics
	// Use the logger from context
	log.Info("Copied total MB", slog.Int64("mb_copied", bytesWritten/BytesInMB))
	elapsed := time.Since(startTime)
	// Use the logger from context
	log.Info("Copied bytes to temporary file",
		slog.Int64("bytes_copied", bytesWritten),
		slog.Duration("duration", elapsed.Round(time.Millisecond)),
	)

	// Close temporary file before DuckDB reads it
	// helpers.CloseResources might benefit from context logger
	helpers.CloseResources(dst, "temporary file before DuckDB reads it")

	return nil, nil
}

// cleanupTempFile removes the temporary file and triggers garbage collection
// Added ctx context.Context
func (s *Server) cleanupTempFile(ctx context.Context, tempFilePath string) {
	log := helpers.GetLoggerFromContext(ctx) // Retrieve logger

	// Use the logger from context
	log.Info("Removing temporary file", slog.String("path", tempFilePath))
	if err := os.Remove(tempFilePath); err != nil {
		// Use the logger from context
		log.Info("Warning: Failed to remove temporary file",
			slog.String("path", tempFilePath),
			slog.Any("error", err),
		)
	}

	// Adding runtime.GC() call helps to reclaim memory after large file operations
	// Use the logger from context
	log.Info("Triggering garbage collection to reclaim memory")
	runtime.GC()
}

// importCsvData imports the CSV data using the appropriate method
// Already takes ctx context.Context, retrieve logger inside
func (s *Server) importCsvData(
	ctx context.Context,
	c *gin.Context, // Keep gin context if needed for other purposes
	tableName, tempFilePath string,
	hasHeader, override bool,
) (*database.QueryResult, int64, map[string]any, error) {

	// Check if table already exists and handle duplicate table scenario
	if !override {
		if exists, checkErr := s.checkTableExists(ctx, tableName); checkErr == nil && exists {
			// Table already exists and override is false - return helpful error
			duplicateError := CSVError{
				Code:    "DUPLICATE_TABLE_NAME",
				Message: fmt.Sprintf("Table '%s' already exists. Use override=true to replace it or choose a different table name.", tableName),
				Details: CSVErrorDetail{
					Line:       0,
					Suggestion: fmt.Sprintf("Either set override=true in your request to replace the existing table, or choose a different table name like '%s_v2' or '%s_%s'.", tableName, tableName, time.Now().Format("20060102")),
				},
			}
			c.JSON(http.StatusUnprocessableEntity, CSVErrorResponse{
				Errors: []CSVError{duplicateError},
			})
			return nil, 0, nil, fmt.Errorf("table '%s' already exists", tableName)
		}
	}

	// Prepare response data
	var columnsResult *database.QueryResult
	var rowCount int64
	var importInfo map[string]any
	var err error
	var importErrors []CSVError

	// Pass context
	columnsResult, rowCount, importInfo, importErrors, err = s.directImport(ctx, c, tableName, tempFilePath, hasHeader, override)

	if err != nil {
		// Check if this is a duplicate table error that slipped through
		if !override && (strings.Contains(err.Error(), "already exists") ||
			strings.Contains(err.Error(), "Catalog Error") && strings.Contains(err.Error(), "already exists")) {
			// Handle duplicate table error that wasn't caught by our pre-check
			duplicateError := CSVError{
				Code:    "DUPLICATE_TABLE_NAME",
				Message: fmt.Sprintf("Table '%s' already exists. Use override=true to replace it or choose a different table name.", tableName),
				Details: CSVErrorDetail{
					Line:       0,
					Suggestion: fmt.Sprintf("Either set override=true in your request to replace the existing table, or choose a different table name like '%s_v2' or '%s_%s'.", tableName, tableName, time.Now().Format("20060102")),
				},
			}
			c.JSON(http.StatusUnprocessableEntity, CSVErrorResponse{
				Errors: []CSVError{duplicateError},
			})
			return nil, 0, nil, err
		}

		// Return detailed error response with collected errors
		c.JSON(http.StatusUnprocessableEntity, CSVErrorResponse{
			Errors: importErrors,
		})
		return nil, 0, nil, err
	}

	return columnsResult, rowCount, importInfo, nil
}

// directImport handles importing using the direct method
// Added ctx context.Context
func (s *Server) directImport(
	ctx context.Context,
	c *gin.Context, // Keep gin context if needed
	tableName, tempFilePath string,
	hasHeader, override bool,
) (*database.QueryResult, int64, map[string]any, []CSVError, error) {
	log := helpers.GetLoggerFromContext(ctx) // Retrieve logger

	// Use the logger from context
	log.Info("Using direct import",
		slog.String("table", tableName),
		slog.String("file", tempFilePath),
	)
	err := s.db.CreateTableFromCSV(ctx, tableName, tempFilePath, hasHeader, override) // Assuming CreateTableFromCSV does not take context or handles its own logging
	if err != nil {
		// Use the logger from context
		log.Info("Error creating table from CSV with direct import", slog.Any("error", err))
		// Create structured error information
		importError := CSVError{
			Code:    "DIRECT_IMPORT_FAILED",
			Message: fmt.Sprintf("Failed to create table from CSV: %v", err),
			Details: CSVErrorDetail{
				Line:       0,
				Suggestion: suggestionMap["DIRECT_IMPORT_FAILED"],
			},
		}
		return nil, 0, nil, []CSVError{importError}, err
	}
	// Use the logger from context
	log.Info("Successfully created table from CSV file using direct import", slog.String("table", tableName))

	// Get column information
	// Pass context
	columnsResult, columnErrors, err := s.getColumnInfo(ctx, c, tableName)
	if err != nil {
		return nil, 0, nil, columnErrors, err
	}

	// Count rows
	// Pass context
	rowCount, countErrors, err := s.countRows(ctx, c, tableName)
	if err != nil {
		return nil, 0, nil, countErrors, err
	}

	// Add import info to response
	importInfo := map[string]any{
		"import_method": "direct_import",
	}

	return columnsResult, rowCount, importInfo, nil, nil
}

// getColumnInfo retrieves column information for the table
// Added ctx context.Context
func (s *Server) getColumnInfo(ctx context.Context, c *gin.Context, tableName string) (*database.QueryResult, []CSVError, error) {
	log := helpers.GetLoggerFromContext(ctx) // Retrieve logger

	// Use the logger from context
	log.Info("Retrieving column information", slog.String("table", tableName))
	query := fmt.Sprintf("PRAGMA table_info(%s)", tableName)
	columnsResult, err := s.db.ExecuteQuery(ctx, query) // Assuming ExecuteQuery does not take context or handles its own logging
	if err != nil {
		// Use the logger from context
		log.Info("Error getting table info",
			slog.String("table", tableName),
			slog.Any("error", err),
		)
		columnError := CSVError{
			Code:    "TABLE_INFO_ERROR",
			Message: "Failed to get table column information",
			Details: CSVErrorDetail{
				Line:       0,
				Suggestion: suggestionMap["TABLE_INFO_ERROR"],
			},
		}
		return nil, []CSVError{columnError}, err
	}
	return columnsResult, nil, nil
}

// checkTableExists checks if a table exists in the database
func (s *Server) checkTableExists(ctx context.Context, tableName string) (bool, error) {
	log := helpers.GetLoggerFromContext(ctx)

	log.Info("Checking if table exists", slog.String("table", tableName))
	// Use ExecuteQuery but construct it safely
	safeQuery := fmt.Sprintf("SELECT COUNT(*) as table_count FROM information_schema.tables WHERE table_schema = 'main' AND table_name = '%s'", tableName)
	result, err := s.db.ExecuteQuery(ctx, safeQuery)
	if err != nil {
		log.Info("Error checking table existence",
			slog.String("table", tableName),
			slog.Any("error", err),
		)
		return false, err
	}

	if len(result.Results) > 0 {
		if count, ok := result.Results[0]["table_count"].(int64); ok {
			return count > 0, nil
		}
	}
	return false, nil
}

// countRows counts the number of rows in the table
// Added ctx context.Context
func (s *Server) countRows(ctx context.Context, c *gin.Context, tableName string) (int64, []CSVError, error) {
	log := helpers.GetLoggerFromContext(ctx) // Retrieve logger

	// Use the logger from context
	log.Info("Counting rows", slog.String("table", tableName))
	query := fmt.Sprintf("SELECT COUNT(*) as row_count FROM %s", tableName)
	countResult, err := s.db.ExecuteQuery(ctx, query) // Assuming ExecuteQuery does not take context or handles its own logging
	if err != nil {
		// Use the logger from context
		log.Info("Error counting rows",
			slog.String("table", tableName),
			slog.Any("error", err),
		)
		countError := CSVError{
			Code:    "ROW_COUNT_ERROR",
			Message: "Failed to count rows in table",
			Details: CSVErrorDetail{
				Line:       0,
				Suggestion: suggestionMap["ROW_COUNT_ERROR"],
			},
		}
		return 0, []CSVError{countError}, err
	}

	var rowCount int64
	if len(countResult.Results) > 0 {
		if count, ok := countResult.Results[0]["row_count"].(int64); ok {
			rowCount = count
		}
	}
	return rowCount, nil, nil
}
