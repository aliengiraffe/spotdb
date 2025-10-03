package api

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"
)

// CSV environment constants and variables

// CSV validation constants
const (
	// MaxSampleLines is the number of lines to sample for structure validation
	MaxSampleLines = 20
	// MinSampleLines is the minimum number of lines required for validation
	MinSampleLines = 3
	// MaxSampleSize is the maximum number of bytes to read for structure validation
	MaxSampleSize = 16 * 1024 // 16KB
	// MinColumnCount is the minimum number of columns required for a valid CSV
	MinColumnCount = 2
	// SampleSizeForDelimiter is the number of lines to read when detecting delimiter
	SampleSizeForDelimiter = 5

	// Error message constants
	ErrEmptyFile           = "empty file"
	ErrFailedResetPosition = "failed to reset file position: %v"
	ErrNoData              = "file contains no data"
	ErrInconsistentColumns = "inconsistent column count on line %d: got %d, expected %d"
	ErrDelimiterDetection  = "delimiter detection failed: %v"
	ErrReadSample          = "failed to read file sample: %v"
	ErrReadingSample       = "error reading sample: %v"
	ErrCSVFormatValidation = "CSV format validation failed: %v"

	// Line terminator constants
	WindowsLineEnding = "\r\n"
	UnixLineEnding    = "\n"
	MacLineEnding     = "\r"

	// Numeric digits for validation
	NumericDigits = "0123456789"
)

// Common CSV delimiters to try
var PossibleDelimiters = []rune{',', ';', '\t', '|'}

// AllowedCSVMimeTypes contains all MIME types accepted for CSV files
var AllowedCSVMimeTypes = map[string]bool{
	"text/csv":                    true,
	"text/plain":                  true,
	"application/csv":             true,
	"text/comma-separated-values": true,
	"application/vnd.ms-excel":    true, // Some systems use this for CSV
}

// CSVValidationResult contains the results of CSV validation
type CSVValidationResult struct {
	Valid          bool
	Delimiter      rune
	HasHeader      bool
	ColumnCount    int
	HasQuotes      bool
	SampleRows     int
	ErrorMessage   string
	LineTerminator string
}

// DetectDelimiterFromData analyzes byte data to determine the most likely delimiter
func DetectDelimiterFromData(data []byte) (rune, error) {
	// Read the data into lines
	var sampleLines []string
	reader := bytes.NewReader(data)
	scanner := bufio.NewScanner(reader)

	// Read only a few sample lines
	linesRead := 0
	for scanner.Scan() && linesRead < SampleSizeForDelimiter {
		line := scanner.Text()
		if line != "" {
			sampleLines = append(sampleLines, line)
			linesRead++
		}
	}

	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf(ErrReadingSample, err)
	}

	if len(sampleLines) == 0 {
		return 0, errors.New(ErrEmptyFile)
	}

	// Try each delimiter and score them
	bestDelimiter := ',' // Default to comma if we can't detect
	bestScore := -1

	for _, delimiter := range PossibleDelimiters {
		score, ok := scoreDelimiter(sampleLines, delimiter)
		if ok && score > bestScore {
			bestScore = score
			bestDelimiter = delimiter
		}
	}

	return bestDelimiter, nil
}

// scoreDelimiter scores a delimiter based on consistency and other factors
func scoreDelimiter(lines []string, delimiter rune) (int, bool) {
	if len(lines) == 0 {
		return 0, false
	}

	records, err := parseRecordsWithDelimiter(lines, delimiter)
	if err != nil || len(records) == 0 {
		return 0, false
	}

	// Must have multiple columns to be valid
	firstRowCount := len(records[0])
	if firstRowCount < MinColumnCount {
		return 0, false
	}

	// Check column consistency (most important)
	if !hasConsistentColumnCount(records, firstRowCount) {
		return 0, false // Reject inconsistent column counts
	}

	// Calculate score based on various criteria
	score := calculateDelimiterScore(records, delimiter, firstRowCount)

	return score, true
}

// parseRecordsWithDelimiter parses the input lines using the specified delimiter
func parseRecordsWithDelimiter(lines []string, delimiter rune) ([][]string, error) {
	// Create a CSV reader with the current delimiter
	reader := csv.NewReader(strings.NewReader(strings.Join(lines, "\n")))
	reader.Comma = delimiter
	reader.FieldsPerRecord = -1 // Don't enforce consistent column count yet
	reader.LazyQuotes = true    // Be more permissive for detection
	reader.TrimLeadingSpace = true

	// Parse all records with this delimiter
	return reader.ReadAll()
}

// hasConsistentColumnCount checks if all rows have the same number of columns
func hasConsistentColumnCount(records [][]string, expectedColumnCount int) bool {
	for _, record := range records[1:] {
		if len(record) != expectedColumnCount {
			return false
		}
	}
	return true
}

// calculateDelimiterScore computes a score for a delimiter based on various heuristics
func calculateDelimiterScore(records [][]string, delimiter rune, columnCount int) int {
	score := 15 // Base score for consistent columns

	// Bonus points for comma (most common)
	if delimiter == ',' {
		score += 5
	}

	// Bonus for having reasonable number of columns (3-20)
	if columnCount >= 3 && columnCount <= 20 {
		score += 3
	}

	// Bonus for not having empty fields
	emptyRatio := calculateEmptyFieldsRatio(records)
	if emptyRatio < 0.1 {
		score += 2
	}

	return score
}

// calculateEmptyFieldsRatio computes the ratio of empty fields to total fields
func calculateEmptyFieldsRatio(records [][]string) float64 {
	emptyFields := 0
	totalFields := 0

	for _, record := range records {
		for _, field := range record {
			totalFields++
			if field == "" {
				emptyFields++
			}
		}
	}

	if totalFields == 0 {
		return 1.0 // All empty if no fields
	}

	return float64(emptyFields) / float64(totalFields)
}

// ValidateCSVFileFromData performs validation on CSV data provided as bytes
func ValidateCSVFileFromData(data []byte) (*CSVValidationResult, error) {
	// Initialize the result
	result, err := initializeValidationResultFromData(data)
	if err != nil {
		return nil, err
	}

	// Sample records for validation from the data
	records, err := sampleCSVRecordsFromData(data, result.Delimiter, MaxSampleLines)
	if err != nil {
		result.Valid = false
		result.ErrorMessage = fmt.Sprintf(ErrCSVFormatValidation, err)
		return result, nil
	}

	// Check if data is empty
	if len(records) == 0 {
		result.Valid = false
		result.ErrorMessage = ErrNoData
		return result, nil
	}

	// Set basic metrics and validate structure
	result.ColumnCount = len(records[0])
	result.SampleRows = len(records)

	// Perform content validation
	validateCSVContent(records, result)

	return result, nil
}

// initializeValidationResultFromData sets up the validation result with basic data info
func initializeValidationResultFromData(data []byte) (*CSVValidationResult, error) {
	// First detect the delimiter
	delimiter, err := DetectDelimiterFromData(data)
	if err != nil {
		return nil, fmt.Errorf(ErrDelimiterDetection, err)
	}

	// Create a validation result with the detected delimiter
	result := &CSVValidationResult{
		Valid:     true,
		Delimiter: delimiter,
	}

	// Detect line terminator
	result.LineTerminator = detectLineTerminator(data)

	return result, nil
}

// sampleCSVRecordsFromData parses a limited number of CSV records for validation from bytes
func sampleCSVRecordsFromData(data []byte, delimiter rune, maxLines int) ([][]string, error) {
	// Limit data size if needed
	sampleSize := MaxSampleSize
	if len(data) > sampleSize {
		data = data[:sampleSize]
	}

	// Create a CSV reader on the data
	csvReader := csv.NewReader(bytes.NewReader(data))
	csvReader.Comma = delimiter
	csvReader.LazyQuotes = false // Strict mode for validation

	// Read sampled records
	var records [][]string
	for i := 0; i < maxLines; i++ {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return records, err
		}
		records = append(records, record)
	}

	return records, nil
}

// validateCSVContent performs various validations on the CSV content
func validateCSVContent(records [][]string, result *CSVValidationResult) {
	// Verify consistent column count
	validateColumnConsistency(records, result)

	// Check for quotes in the data
	result.HasQuotes = hasQuotedFields(records)

	// Detect if the file has a header (heuristic approach)
	result.HasHeader = detectHeader(records)
}

// validateColumnConsistency verifies all rows have the same number of columns
func validateColumnConsistency(records [][]string, result *CSVValidationResult) {
	for i, record := range records {
		if len(record) != result.ColumnCount {
			result.Valid = false
			result.ErrorMessage = fmt.Sprintf(ErrInconsistentColumns,
				i+1, len(record), result.ColumnCount)
			break
		}
	}
}

// hasQuotedFields checks if any field in the CSV contains quotes
func hasQuotedFields(records [][]string) bool {
	for _, record := range records {
		for _, field := range record {
			if strings.Contains(field, "\"") {
				return true
			}
		}
	}
	return false
}

// detectLineTerminator detects the line terminator used in the file
func detectLineTerminator(sample []byte) string {
	// Check for Windows line endings
	if bytes.Contains(sample, []byte(WindowsLineEnding)) {
		return WindowsLineEnding
	}

	// Check for Unix line endings
	if bytes.Contains(sample, []byte(UnixLineEnding)) {
		return UnixLineEnding
	}

	// Check for old Mac line endings
	if bytes.Contains(sample, []byte(MacLineEnding)) {
		return MacLineEnding
	}

	return UnixLineEnding // Default to Unix line endings
}

// detectHeader tries to determine if the first row is a header
func detectHeader(records [][]string) bool {
	if len(records) < 2 {
		return false
	}

	headerRow := records[0]
	dataRows := records[1:]

	// Statistical characteristics to analyze
	headerScore := 0

	// 1. Analyze character type distributions for the first row vs subsequent rows
	if isCharacterDistributionForHeader(headerRow, dataRows) {
		headerScore += 3
	}

	// 2. Length variance - headers typically have more similar lengths than data
	if hasConsistentLengths(headerRow) {
		headerScore++
	}

	// 3. Check capitalization patterns - headers are often capitalized
	capitalsInHeader := 0
	for _, cell := range headerRow {
		if containsCapitalizedWord(cell) {
			capitalsInHeader++
		}
	}
	if capitalsInHeader > len(headerRow)/3 {
		headerScore += 2
	}

	// 4. Check for numbers - typically fewer numbers in headers than in data
	numericRatio := compareNumericRatio(headerRow, dataRows)
	if numericRatio < 0.5 { // Headers have fewer numbers than data cells
		headerScore += 2
	}

	// 5. Analyze empty cells - headers shouldn't have many empty cells
	emptyInHeader := 0
	for _, cell := range headerRow {
		if strings.TrimSpace(cell) == "" {
			emptyInHeader++
		}
	}
	if emptyInHeader == 0 {
		headerScore++
	} else if emptyInHeader > len(headerRow)/3 {
		headerScore -= 2 // Too many empty cells, less likely to be a header
	}

	// 6. Compare word counts - headers often have fewer words than data cells
	if hasFewerWords(headerRow, dataRows) {
		headerScore++
	}

	// Make final determination
	return headerScore > 3 // Require more evidence to consider it a header
}

// isCharacterDistributionForHeader checks if the character type distribution
// suggests that the first row is a header by comparing character distributions
func isCharacterDistributionForHeader(headerRow []string, dataRows [][]string) bool {
	// Headers often have more non-numeric characters, letters, and special chars
	// than typical data rows
	headerAlphaRatio := calculateAlphaRatio(headerRow)

	if len(dataRows) == 0 {
		// If we only have one row, make a best guess
		return headerAlphaRatio > 0.7 // More than 70% alphabetic chars suggests a header
	}

	// Calculate the average alpha ratio in data rows
	var dataAlphaRatio float64
	for _, row := range dataRows {
		dataAlphaRatio += calculateAlphaRatio(row)
	}
	dataAlphaRatio /= float64(len(dataRows))

	// If header has more alphabetic content than data rows, it's likely a header
	return headerAlphaRatio > dataAlphaRatio
}

// calculateAlphaRatio calculates the ratio of alphabetic characters to total characters
func calculateAlphaRatio(row []string) float64 {
	totalChars := 0
	alphaChars := 0

	for _, cell := range row {
		totalChars += len(cell)
		for _, r := range cell {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
				alphaChars++
			}
		}
	}

	if totalChars == 0 {
		return 0
	}

	return float64(alphaChars) / float64(totalChars)
}

// hasConsistentLengths checks if the header cells have consistent lengths
func hasConsistentLengths(row []string) bool {
	if len(row) < 2 {
		return true
	}

	// Calculate the standard deviation of cell lengths
	var sum, sumSq float64
	for _, cell := range row {
		l := float64(len(cell))
		sum += l
		sumSq += l * l
	}

	mean := sum / float64(len(row))
	variance := (sumSq / float64(len(row))) - (mean * mean)

	// Low variance suggests consistent lengths, which is common for headers
	return variance < 25 // Threshold chosen empirically
}

// compareNumericRatio compares the ratio of numeric cells in the header vs data
func compareNumericRatio(headerRow []string, dataRows [][]string) float64 {
	numericInHeader := 0
	for _, cell := range headerRow {
		if containsDigits(cell) {
			numericInHeader++
		}
	}
	headerRatio := float64(numericInHeader) / float64(len(headerRow))

	if len(dataRows) == 0 {
		return headerRatio
	}

	numericInData := 0
	totalDataCells := 0
	for _, row := range dataRows {
		totalDataCells += len(row)
		for _, cell := range row {
			if containsDigits(cell) {
				numericInData++
			}
		}
	}

	if totalDataCells == 0 {
		return 1.0 // Avoid division by zero
	}

	dataRatio := float64(numericInData) / float64(totalDataCells)

	// Return the ratio of header's numeric density to data's numeric density
	if dataRatio == 0 {
		return 1.0 // Avoid division by zero
	}

	return headerRatio / dataRatio
}

// hasFewerWords checks if header cells typically have fewer words than data cells
func hasFewerWords(headerRow []string, dataRows [][]string) bool {
	headerWordCount := 0
	for _, cell := range headerRow {
		headerWordCount += len(strings.Fields(cell))
	}
	avgHeaderWords := float64(headerWordCount) / float64(len(headerRow))

	if len(dataRows) == 0 {
		return true // If no data rows, assume header has appropriate word count
	}

	dataWordCount := 0
	dataCellCount := 0
	for _, row := range dataRows {
		dataCellCount += len(row)
		for _, cell := range row {
			dataWordCount += len(strings.Fields(cell))
		}
	}

	if dataCellCount == 0 {
		return true
	}

	avgDataWords := float64(dataWordCount) / float64(dataCellCount)

	// Headers typically have fewer words per cell than data
	return avgHeaderWords <= avgDataWords
}

// containsDigits checks if a string contains numeric digits
func containsDigits(s string) bool {
	return strings.ContainsAny(s, NumericDigits)
}

// containsCapitalizedWord checks if the string contains any capitalized words
func containsCapitalizedWord(s string) bool {
	words := strings.Fields(s)
	for _, word := range words {
		if len(word) > 0 && word[0] >= 'A' && word[0] <= 'Z' {
			return true
		}
	}
	return false
}
