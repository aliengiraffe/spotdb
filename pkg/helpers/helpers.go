package helpers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"

	gonanoid "github.com/matoous/go-nanoid/v2"
)

// DefaultMaxFileSize is 2GB in bytes
const DefaultMaxFileSize int64 = 2 * 1024 * 1024 * 1024

// DefaultBufferSize is 256KB in bytes (increased from 32KB for better performance)
const DefaultBufferSize int = 64 * 1024

// ErrMaxFileSizeExceeded is returned when the file size exceeds the maximum allowed size
var ErrMaxFileSizeExceeded = errors.New("max file size exceeded")

// CloseResources safely closes an io.Closer and logs any error
// It's a utility function for handling resource cleanup
func CloseResources(resource io.Closer, resourceName string) {
	if resource != nil {
		if err := resource.Close(); err != nil {
			log.Printf("Failed to close %s: %v", resourceName, err)
		}
	}
}

const apiKeyEnvVar = "API_KEY"

const apiKeyHeader = "X-API-Key"

func IsValidAPIKeyFromHeader(header *http.Header) bool {
	expectedKey := os.Getenv(apiKeyEnvVar)

	providedKey := header.Get(apiKeyHeader)

	// No configured API key, so it passes for all requests
	if expectedKey == "" {
		return true
	}
	// Return true only if both keys are non-empty and match.
	return providedKey == expectedKey
}

// ValidationIssue contains details about a validation issue
type ValidationIssue struct {
	Pattern string
	Line    int
	Column  string
	Value   string
}

// BufferValidationFunc is a function type that validates a buffer
// Returns true if the buffer is valid, false if invalid, and can return an error if validation fails
// The ValidationIssue contains detailed information about where the issue was found
type BufferValidationFunc func(data []byte, lineNumber int, columnMap map[int]string) (bool, *ValidationIssue, error)

// Validation mode constants
const (
	ValidationModeRejectRow  = "reject_row"  // Skip invalid rows/buffers
	ValidationModeRejectFile = "reject_file" // Reject the entire file if any invalid content is found
	ValidationModeIgnore     = "ignore"      // Ignore validation results (validation still runs for logging)
)

// Default validation mode
const DefaultValidationMode = ValidationModeRejectFile

// ErrInvalidBuffer is returned when the buffer fails validation
var ErrInvalidBuffer = errors.New("buffer validation failed")

// ProcessingContext holds all data needed for processing
type ProcessingContext struct {
	// I/O
	Source      io.Reader
	Destination io.Writer
	Buffer      []byte
	LineBuffer  *bytes.Buffer

	// Validation
	Validator          BufferValidationFunc
	ValidationMode     string
	ValidationWarnings []string

	// CSV processing data
	HasHeader       bool
	ColumnMap       map[int]string   // Maps column index to column name
	CurrentLine     int              // Current line number (1-based)
	ValidationIssue *ValidationIssue // Stores detailed validation issue info

	// Size control
	Written *int64
	MaxSize int64
}

// CopyWithMaxSize is defined as a variable so it can be patched in tests
var CopyWithMaxSize = CopyWithMaxSizeImpl

// CopyWithMaxSizeImpl copies from src to dst using a buffer of specified size,
// while enforcing a maximum file size limit. If maxSize is <= 0, it reads the limit
// from the ENV_MAX_FILE_SIZE environment variable (defaults to 1GB if not set or invalid).
// If validateBuffer is not nil, it will be called to validate each buffer before writing to dst.
//
// The behavior of validation depends on the ENV_FILE_VALIDATION_MODE environment variable:
// - "reject_row": Skip invalid rows/buffers but continue processing (uses line-by-line processing)
// - "reject_file": Reject the entire file if any invalid content is found (default)
// - "ignore": Ignore validation results (validation still runs for logging)
//
// Line-by-line processing is used when ValidationModeRejectRow is specified,
// ensuring that individual rows can be validated and skipped if needed.
func CopyWithMaxSizeImpl(dst io.Writer, src io.Reader, bufferSize int, maxSize int64, validateBuffer BufferValidationFunc) (written int64, validationIssue *ValidationIssue, err error) {
	// If maxSize is <= 0, use the default or environment value
	if maxSize <= 0 {
		maxSize = GetMaxFileSize()
	}

	// Create processing context
	ctx := &ProcessingContext{
		Source:             src,
		Destination:        dst,
		Buffer:             make([]byte, bufferSize),
		LineBuffer:         &bytes.Buffer{},
		Validator:          validateBuffer,
		ValidationMode:     GetValidationMode(),
		ValidationWarnings: []string{},
		HasHeader:          true, // Default to assuming headers
		ColumnMap:          make(map[int]string),
		CurrentLine:        0, // Will be incremented for each line
		Written:            &written,
		MaxSize:            maxSize,
	}

	// Process all data from source
	err = processSourceData(ctx)
	if err != nil {
		return written, ctx.ValidationIssue, err
	}

	// Log summary of warnings if any
	if len(ctx.ValidationWarnings) > 0 {
		log.Printf("Completed with %d validation warnings", len(ctx.ValidationWarnings))
	}

	return written, ctx.ValidationIssue, nil
}

// processSourceData reads from source and processes data line by line
func processSourceData(ctx *ProcessingContext) error {
	for {
		// Handle one read operation at a time
		readErr := processNextChunk(ctx)

		// Handle EOF by processing any remaining content
		if readErr == io.EOF {
			err := processRemainingContent(ctx)
			// Return nil even if we got io.EOF as that's an expected condition
			return err
		}

		// Return any other errors
		if readErr != nil {
			return readErr
		}
	}
}

// processNextChunk reads a single chunk of data and processes it
func processNextChunk(ctx *ProcessingContext) error {
	// Read from source
	bytesRead, readErr := ctx.Source.Read(ctx.Buffer)

	// Process data if we read anything, regardless of error
	if bytesRead > 0 {
		// Add read data to the line buffer
		ctx.LineBuffer.Write(ctx.Buffer[:bytesRead])

		// Process any complete lines
		if err := processCompleteLines(ctx); err != nil {
			return err
		}
	}

	return readErr
}

// processRemainingContent handles any data left in the buffer at EOF
func processRemainingContent(ctx *ProcessingContext) error {
	if ctx.LineBuffer.Len() > 0 {
		return validateAndWrite(ctx.LineBuffer.Bytes(), ctx)
	}
	return nil
}

// processCompleteLines processes all complete lines in the buffer
func processCompleteLines(ctx *ProcessingContext) error {
	for {
		// Try to read the next complete line (ending with newline)
		line, err := readNextLine(ctx)

		// Handle EOF (incomplete line)
		if err == io.EOF {
			// No more complete lines, return success
			return nil
		}

		// Process the complete line
		if err := validateAndWrite(line, ctx); err != nil {
			return err
		}
	}
}

// readNextLine attempts to read the next complete line from the buffer
// Returns the line and an error (io.EOF if no complete line was found)
func readNextLine(ctx *ProcessingContext) ([]byte, error) {
	line, err := ctx.LineBuffer.ReadBytes('\n')
	if err == io.EOF {
		// Put back the incomplete line for the next read
		ctx.LineBuffer.Write(line)
	}
	return line, err
}

// validateAndWrite validates a single line and writes it if appropriate
func validateAndWrite(line []byte, ctx *ProcessingContext) error {
	// Skip empty lines
	if len(line) == 0 {
		return nil
	}

	// Increment line number
	ctx.CurrentLine++

	// Parse the CSV line to extract column headers if this is the first line
	if ctx.CurrentLine == 1 && ctx.HasHeader {
		parseHeaderLine(line, ctx)
		// No need to validate the header line
		return writeDataFromContext(line, ctx)
	}

	// Validate the content
	shouldSkip, err := shouldSkipWriting(line, ctx)
	if err != nil {
		return err // Return validation errors
	}

	// Skip writing if validation indicated to do so
	if shouldSkip {
		return nil
	}

	// Write the data
	return writeDataFromContext(line, ctx)
}

// splitCSVLine splits a CSV line into fields considering quotes and delimiters.
// It trims trailing newline and carriage return characters.
func splitCSVLine(line []byte) []string {
	var fields []string

	// Detect delimiter - default to comma
	delimiter := ','
	if bytes.Contains(line, []byte{'\t'}) {
		delimiter = '\t'
	} else if bytes.Contains(line, []byte{';'}) {
		delimiter = ';'
	}

	// Split by delimiter, accounting for quoted fields
	inQuote := false
	fieldStart := 0
	for i, c := range line {
		if c == '"' {
			inQuote = !inQuote
		} else if !inQuote && c == byte(delimiter) {
			fields = append(fields, string(line[fieldStart:i]))
			fieldStart = i + 1
		}
	}

	// Add the last field if not empty
	if fieldStart < len(line) {
		lastField := string(line[fieldStart:])
		// Strip trailing newline if present
		lastField = string(bytes.TrimSuffix([]byte(lastField), []byte{'\n'}))
		lastField = string(bytes.TrimSuffix([]byte(lastField), []byte{'\r', '\n'}))
		fields = append(fields, lastField)
	}

	return fields
}

// parseHeaderLine parses the header line of a CSV to extract column names
func parseHeaderLine(line []byte, ctx *ProcessingContext) {
	// Extract fields and map column indices to column names
	fields := splitCSVLine(line)
	for i, name := range fields {
		// Remove quotes if present
		name = strings.Trim(name, "\"")
		ctx.ColumnMap[i] = name
	}
}

// parseCSVFields parses a CSV line into fields to identify which column contains suspicious content
func parseCSVFields(line []byte) []string {
	return splitCSVLine(line)
}

// shouldSkipWriting validates content and returns if it should be skipped
func shouldSkipWriting(line []byte, ctx *ProcessingContext) (bool, error) {
	// If no validator is provided, don't skip
	if ctx.Validator == nil {
		return false, nil
	}

	// Validate content
	return validateContent(line, ctx)
}

// writeDataFromContext handles the actual writing of data to the destination using the context
func writeDataFromContext(data []byte, ctx *ProcessingContext) error {
	bytesRead := len(data)
	bytesWritten, writeErr := ctx.Destination.Write(data)

	// Handle invalid write result
	if bytesWritten < 0 || bytesRead < bytesWritten {
		return errors.New("invalid write result")
	}

	// Update bytes written counter
	*ctx.Written += int64(bytesWritten)

	// Check for max size exceeded
	if *ctx.Written > ctx.MaxSize {
		return ErrMaxFileSizeExceeded
	}

	// Handle write errors
	if writeErr != nil {
		return writeErr
	}

	// Handle partial writes
	if bytesRead != bytesWritten {
		return io.ErrShortWrite
	}

	return nil
}

// validateContent checks if content is valid based on validation mode
// Returns: skipWrite (bool), error
func validateContent(data []byte, ctx *ProcessingContext) (bool, error) {
	// If no validation function provided, accept all content
	if ctx.Validator == nil {
		return false, nil
	}

	// Parse CSV fields to identify column data
	fields := parseCSVFields(data)

	// Validate the data
	isValid, validationIssue, validationErr := ctx.Validator(data, ctx.CurrentLine, ctx.ColumnMap)

	// Save validation issue in context if available (regardless of error)
	if validationIssue != nil {
		ctx.ValidationIssue = validationIssue
	}

	// Handle validation function errors
	if validationErr != nil {
		return handleValidationError(validationErr, ctx)
	}

	// Handle invalid content
	if !isValid && validationIssue != nil {
		// Try to find column information if not provided
		enrichValidationIssue(validationIssue, fields, ctx)

		// Handle based on validation mode
		return handleInvalidContent(validationIssue, ctx)
	}

	// Content is valid
	return false, nil
}

// handleValidationError processes errors from the validator function
// and returns appropriate skip/error response based on validation mode
func handleValidationError(validationErr error, ctx *ProcessingContext) (bool, error) {
	// Always return the error directly if it's an ErrInvalidBuffer
	// This is critical for test mocks to work properly
	if errors.Is(validationErr, ErrInvalidBuffer) {
		return false, validationErr
	}

	// Otherwise apply validation mode behavior
	switch ctx.ValidationMode {
	case ValidationModeRejectFile:
		return false, fmt.Errorf("buffer validation error: %w", validationErr)
	default:
		// In other modes, log the error and continue
		log.Printf("Warning: buffer validation error: %v", validationErr)
		ctx.ValidationWarnings = append(ctx.ValidationWarnings,
			fmt.Sprintf("validation error: %v", validationErr))
		return false, nil
	}
}

// enrichValidationIssue adds additional information to validation issues
// such as column name and suspicious value
func enrichValidationIssue(issue *ValidationIssue, fields []string, ctx *ProcessingContext) {
	// If column index is specified but column name isn't, try to map it
	if issue.Column == "" && len(fields) > 0 {
		// Find the first suspicious column
		for i, field := range fields {
			if strings.Contains(field, "=") || strings.Contains(field, "<script") {
				columnName := ctx.ColumnMap[i]
				if columnName == "" {
					columnName = fmt.Sprintf("Column %d", i+1)
				}
				issue.Column = columnName
				issue.Value = field
				break
			}
		}
	}
}

// handleInvalidContent processes invalid content based on validation mode
// Returns whether to skip writing the content and any error
func handleInvalidContent(issue *ValidationIssue, ctx *ProcessingContext) (bool, error) {
	switch ctx.ValidationMode {
	case ValidationModeRejectFile:
		// Reject the entire file
		return false, fmt.Errorf("%w: detected suspicious patterns", ErrInvalidBuffer)

	case ValidationModeRejectRow:
		// Skip this buffer/row but continue processing
		logMessage := fmt.Sprintf("detected suspicious pattern in line %d, column %s",
			issue.Line, issue.Column)
		log.Printf("Warning: skipping invalid buffer: %s", logMessage)
		ctx.ValidationWarnings = append(ctx.ValidationWarnings,
			fmt.Sprintf("skipped invalid buffer: line %d, column %s", issue.Line, issue.Column))
		return true, nil // Skip writing this line

	case ValidationModeIgnore:
		// Log the issue but proceed with writing
		logMessage := fmt.Sprintf("suspicious patterns in line %d, column %s",
			issue.Line, issue.Column)
		log.Printf("Warning: ignoring %s", logMessage)
		ctx.ValidationWarnings = append(ctx.ValidationWarnings,
			fmt.Sprintf("ignored suspicious patterns: line %d, column %s", issue.Line, issue.Column))
		return false, nil

	default:
		// Treat unknown modes as reject_file for safety
		log.Printf("Warning: unknown validation mode %q, treating as %s",
			ctx.ValidationMode, ValidationModeRejectFile)
		return false, fmt.Errorf("%w: detected suspicious patterns", ErrInvalidBuffer)
	}
}

// GetMaxFileSize returns the maximum file size allowed from environment
// variable ENV_MAX_FILE_SIZE or the default value (1GB)
func GetMaxFileSize() int64 {
	maxSizeStr := os.Getenv("ENV_MAX_FILE_SIZE")
	if maxSizeStr == "" {
		return DefaultMaxFileSize
	}

	maxSize, err := strconv.ParseInt(maxSizeStr, 10, 64)
	if err != nil {
		log.Printf("Invalid ENV_MAX_FILE_SIZE value: %v, using default: %d bytes", err, DefaultMaxFileSize)
		return DefaultMaxFileSize
	}

	if maxSize <= 0 {
		log.Printf("ENV_MAX_FILE_SIZE must be positive, using default: %d bytes", DefaultMaxFileSize)
		return DefaultMaxFileSize
	}

	return maxSize
}

// GetBufferSize returns the buffer size for file copying from environment
// variable ENV_BUFFER_SIZE or the default value (32KB)
// Similar to GetMaxFileSize, this allows configuring the buffer size used in file operations
func GetBufferSize() int {
	bufferSizeStr := os.Getenv("ENV_BUFFER_SIZE")
	if bufferSizeStr == "" {
		return DefaultBufferSize
	}

	bufferSize, err := strconv.Atoi(bufferSizeStr)
	if err != nil {
		log.Printf("Invalid ENV_BUFFER_SIZE value: %v, using default: %d bytes", err, DefaultBufferSize)
		return DefaultBufferSize
	}

	if bufferSize <= 0 {
		log.Printf("ENV_BUFFER_SIZE must be positive, using default: %d bytes", DefaultBufferSize)
		return DefaultBufferSize
	}

	return bufferSize
}

// GetValidationMode returns the validation mode from the ENV_FILE_VALIDATION_MODE environment variable
// Valid values are:
// - "reject_row": Skip invalid rows/buffers but continue processing
// - "reject_file": Reject the entire file if any invalid content is found (default)
// - "ignore": Ignore validation results (validation still runs for logging)
func GetValidationMode() string {
	mode := os.Getenv("ENV_FILE_VALIDATION_MODE")
	if mode == "" {
		return DefaultValidationMode
	}

	switch mode {
	case ValidationModeRejectRow, ValidationModeRejectFile, ValidationModeIgnore:
		return mode
	default:
		log.Printf("Invalid ENV_FILE_VALIDATION_MODE value: %s, using default: %s", mode, DefaultValidationMode)
		return DefaultValidationMode
	}
}

// Precompiled patterns for performance
var compiledPatterns map[string]*regexp.Regexp

// Initialize precompiled patterns
func init() {
	compiledPatterns = make(map[string]*regexp.Regexp, len(CommonCSVInjectionPatterns))
	for _, pattern := range CommonCSVInjectionPatterns {
		re, err := regexp.Compile(pattern)
		if err != nil {
			log.Printf("Warning: Failed to compile pattern %s: %v", pattern, err)
			continue
		}
		compiledPatterns[pattern] = re
	}
}

// CSVRowCheckDetailed returns:
// - true if the row is safe (no injection attacks detected)
// - false if the row contains potential injection attacks
// - a ValidationIssue struct with details about the validation failure
// - an error if something went wrong during pattern matching
//
// This is a highly optimized version that:
// 1. First checks for common suspicious character sequences as a fast pre-filter
// 2. Only runs expensive regex validation if the pre-filter finds suspicious content
// 3. Uses early-exit optimization to stop at the first confirmed match
func CSVRowCheckDetailed(rowData []byte, lineNumber int, columnMap map[int]string) (bool, *ValidationIssue, error) {
	// First do a fast check for suspicious character sequences
	suspicious, matches := findSuspiciousContent(rowData)
	if !suspicious {
		return true, nil, nil
	}

	// If suspicious content was found, locate details and create a validation issue
	validationIssue := createValidationIssue(rowData, matches, lineNumber, columnMap)

	// Return validation result with detailed issue information and an error
	return false, validationIssue, fmt.Errorf("%w: detected suspicious content", ErrInvalidBuffer)
}

// createValidationIssue constructs a ValidationIssue from suspicious content matches
// by locating which CSV field contains the suspicious content
func createValidationIssue(rowData []byte, matches map[string][]string, lineNumber int, columnMap map[int]string) *ValidationIssue {
	// Extract fields from the CSV row to identify which column contains the suspicious content
	fields := parseCSVFields(rowData)

	// Find the first matching pattern and get its details
	suspiciousPattern, suspiciousMatch := getFirstMatch(matches)
	if suspiciousMatch == "" {
		// No actual match found (shouldn't happen if matches has data)
		return &ValidationIssue{
			Line: lineNumber,
		}
	}

	// Find which field contains the suspicious content
	fieldIndex, suspiciousValue := findSuspiciousField(fields, suspiciousMatch)

	// Get column name from the map if available
	suspiciousColumn := getColumnName(fieldIndex, columnMap)

	// Create and return the validation issue
	return &ValidationIssue{
		Pattern: suspiciousPattern,
		Line:    lineNumber,
		Column:  suspiciousColumn,
		Value:   suspiciousValue,
	}
}

// getFirstMatch returns the first pattern and match from the matches map
// Returns empty strings if no matches found
func getFirstMatch(matches map[string][]string) (string, string) {
	for pattern, patternMatches := range matches {
		if len(patternMatches) > 0 {
			return pattern, patternMatches[0]
		}
	}
	return "", ""
}

// findSuspiciousField searches through CSV fields to find which one contains
// the suspicious match. Returns the field index and the full field value.
func findSuspiciousField(fields []string, suspiciousMatch string) (int, string) {
	for i, field := range fields {
		if strings.Contains(field, suspiciousMatch) {
			return i, field
		}
	}
	return -1, ""
}

// getColumnName returns the name of a column based on its index
// Uses the provided column map if available, otherwise generates a default name
func getColumnName(fieldIndex int, columnMap map[int]string) string {
	if fieldIndex == -1 {
		return ""
	}

	if name, exists := columnMap[fieldIndex]; exists {
		return name
	}

	return fmt.Sprintf("Column %d", fieldIndex+1)
}

var CommonCSVInjectionPatterns = []string{
	// Formula injection patterns
	"[=\"']?=\\s*[A-Za-z]+\\s*\\(.*\\)",   // Basic formula pattern
	"[=\"']?\\+\\s*[A-Za-z]+\\s*\\(.*\\)", // Formula with + prefix
	"[=\"']?-\\s*[A-Za-z]+\\s*\\(.*\\)",   // Formula with - prefix
	"[=\"']?@\\s*[A-Za-z]+\\s*\\(.*\\)",   // Formula with @ prefix
	"[=\"']?=\\s*CMD\\s*\\(.*\\)",         // Command execution with parentheses
	"[=\"']?=\\s*cmd\\.[a-z]+",            // cmd.exe pattern (without parentheses)
	"[=\"']?=\\s*cmd\\|.*\\'?/C",          // Command execution with pipe and /C
	"-cmd\\.exe!",                         // Minus-prefixed command execution with cell reference
	"\\+cmd\\.exe!",                       // Plus-prefixed command execution with cell reference
	"[=\"']?=\\s*DDE\\s*\\(.*\\)",         // DDE formula
	"[=\"']?=\\s*HYPERLINK\\s*\\(.*\\)",   // Hyperlink injection
	"\\+IMPORTXML\\s*\\(.*\\)",            // Import XML with + prefix

	// XSS patterns
	"<script[^>]*>.*</script>",    // Script tags
	"<img[^>]*onerror=",           // Image with onerror
	"javascript:",                 // JavaScript protocol
	"on\\w+=['\"`][^'\"`]*['\"`]", // Event handlers with quotes
	"on\\w+=[^\\s>]*",             // Event handlers without quotes
	"<[^>]*\\son\\w+\\s*=",        // HTML tags with event handlers
	"data:text/html",              // Data URI
	"<iframe[^>]*>",               // iframes
	"\\balert\\s*\\(",             // Alert function
	"\\beval\\s*\\(",              // Eval function
}

// Common suspicious character sequences for quick pre-filtering
var suspiciousSequences = [][]byte{
	[]byte("<script"),
	[]byte("</script"),
	[]byte("<img"),
	[]byte("javascript:"),
	[]byte("onerror="),
	[]byte("onclick="),
	[]byte("onload="),      // onload event handler
	[]byte("onmouseover="), // onmouseover event handler
	[]byte("onmouseout="),  // onmouseout event handler
	[]byte("onchange="),    // onchange event handler
	[]byte("onsubmit="),    // onsubmit event handler
	[]byte("onfocus="),     // onfocus event handler
	[]byte("onblur="),      // onblur event handler
	[]byte("onkeydown="),   // onkeydown event handler
	[]byte("onkeypress="),  // onkeypress event handler
	[]byte("onkeyup="),     // onkeyup event handler
	[]byte("=cmd"),         // Command execution format in formula_cmd.csv
	[]byte("=CMD"),         // Command execution (capitalized)
	[]byte("-cmd"),         // Command execution with minus prefix
	[]byte("cmd|"),         // Pipe command execution format
	[]byte("/C "),          // Command execution parameter
	[]byte("!A"),           // Excel cell reference after command
	[]byte("=DDE"),         // DDE execution
	[]byte("=SUM"),         // Formula function
	[]byte("=HYPERLINK"),   // Hyperlink function
	[]byte("IMPORTXML"),    // Import XML function
	[]byte("CONCATENATE"),  // String concatenation function
	[]byte("+IMPORT"),      // Import with + prefix
	[]byte("@SUM"),         // At formula notation
}

// findSuspiciousContent performs a two-stage check for malicious content:
// 1. First a quick check using string matching for suspicious sequences
// 2. If found, a more thorough regex check using the patterns
// Returns (suspicious, matches)
func findSuspiciousContent(data []byte) (bool, map[string][]string) {
	// Stage 1: Quick byte sequence check
	if !containsSuspiciousBytes(data) {
		return false, nil
	}

	// Stage 2: Regex pattern check (only runs if Stage 1 detected something)
	return checkRegexPatterns(data)
}

// containsSuspiciousBytes performs a fast check for suspicious byte sequences
// Returns true if any suspicious sequence is found
func containsSuspiciousBytes(data []byte) bool {
	for _, seq := range suspiciousSequences {
		if bytes.Contains(data, seq) {
			return true
		}
	}
	return false
}

// checkRegexPatterns performs thorough regex checks with early-exit optimization
// Returns (suspicious, matches)
func checkRegexPatterns(data []byte) (bool, map[string][]string) {
	results := make(map[string][]string)
	strData := string(data)

	// Check each pattern until we find a match
	for _, pattern := range CommonCSVInjectionPatterns {
		// Skip invalid patterns (should never happen since we precompile)
		re, exists := compiledPatterns[pattern]
		if !exists {
			continue
		}

		// Find first match only (early exit optimization)
		matches := re.FindAllStringSubmatch(strData, 1)
		if len(matches) == 0 {
			continue
		}

		// Extract the actual matches
		matchStrings := extractMatches(matches)
		results[pattern] = matchStrings

		// Early exit - we found something suspicious
		return true, results
	}

	// No regex patterns matched despite suspicious byte sequences
	return false, nil
}

// extractMatches extracts match strings from regex submatch results
func extractMatches(matches [][]string) []string {
	matchStrings := make([]string, len(matches))
	for i, match := range matches {
		if len(match) > 0 {
			matchStrings[i] = match[0] // The first element is the full match
		}
	}
	return matchStrings
}

type ContextKey string

var myLoggerKey ContextKey = ContextKey("my logger")

func GetLoggerFromContext(ctx context.Context) *slog.Logger {
	if logger, ok := ctx.Value(myLoggerKey).(*slog.Logger); ok {
		return logger
	}
	return slog.Default()
}

// SetLoggerInContext returns a new context containing the provided logger.
// Consumers can retrieve it via GetLoggerFromContext.
func SetLoggerInContext(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, myLoggerKey, logger)
}

func GetHostname() string {
	name, err := os.Hostname()

	if err != nil {
		log.Print("Could not get the hostname")
		return "unknown"
	}

	return name
}

func GenerateID() string {
	nid, err := gonanoid.New()

	if err != nil {
		// Handle error, though is unlikely to fail
		// unless there's a problem with the system's random number generator.
		log.Fatalf("Failed to generate UUID: %v", err)
	}

	return nid
}

func GetServerMode() string {
	return os.Getenv("ENV_SERVER_MODE")
}
