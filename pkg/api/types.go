package api

import (
	"mime/multipart"
)

// CSVRequest represents a request to upload a CSV file
type CSVRequest struct {
	TableName    string                `form:"table_name" binding:"required"`
	CSVFile      *multipart.FileHeader `form:"csv_file" binding:"required" swaggerignore:"true"`
	HasHeader    bool                  `form:"has_header" default:"false"`
	Override     bool                  `form:"override" default:"false"`
	Smart        bool                  `form:"smart" default:"true"`
	FileEncoding string                `form:"csv_file_encoding" default:"utf-8"`
}

// QueryRequest represents a database query request
type QueryRequest struct {
	Query string `json:"query" binding:"required"`
	Limit int    `json:"limit,omitempty"`
}

// ErrorResponse represents a standardized error response
type ErrorResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

// CSVErrorDetail contains detailed information about a CSV error
type CSVErrorDetail struct {
	Line         int    `json:"line"`
	Column       string `json:"column"`
	ExpectedType string `json:"expectedType"`
	FoundValue   string `json:"foundValue"`
	Suggestion   string `json:"suggestion,omitempty"`
}

// CSVError represents a single CSV error with code, message and details
type CSVError struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details CSVErrorDetail `json:"details"`
}

// CSVErrorResponse represents a detailed CSV error response
type CSVErrorResponse struct {
	Errors []CSVError `json:"errors"`
}

// TableColumn represents a single column in a table schema
type TableColumn struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// TableInfo represents a table with its schema information
type TableInfo struct {
	Name    string        `json:"name"`
	Columns []TableColumn `json:"columns"`
}

// TablesResponse represents the response for listing tables with schema
type TablesResponse struct {
	Tables []TableInfo `json:"tables"`
}

// CSVUploadResponse represents the response for a successful CSV upload
type CSVUploadResponse struct {
	Table    string                   `json:"table"`
	Columns  []map[string]interface{} `json:"columns"`
	RowCount int64                    `json:"row_count"`
	Import   map[string]interface{}   `json:"import"`
}

// SnapshotRequest represents a request to create a database snapshot
type SnapshotRequest struct {
	Bucket string `json:"bucket" binding:"required"`
	Key    string `json:"key" binding:"required"`
}

// SnapshotResponse represents the response for a successful snapshot creation
type SnapshotResponse struct {
	Status      string `json:"status"`
	SnapshotURI string `json:"snapshot_uri"`
	Filename    string `json:"filename"`
}
