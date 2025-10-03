package api

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aliengiraffe/spotdb/pkg/database"
	"github.com/aliengiraffe/spotdb/pkg/helpers"
	"github.com/aliengiraffe/spotdb/pkg/snapshot"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/go-playground/validator/v10"
)

// Server represents the HTTP API server
type Server struct {
	db     *database.DuckDB
	router *gin.Engine
}

// NewServer creates a new HTTP API server
func NewServer(db *database.DuckDB, log *slog.Logger) *http.Server {
	app := &Server{
		db: db,
	}

	app.setupRouter(log)

	return &http.Server{
		Addr:    ":8080",
		Handler: app.Router(),
	}
}

// setupRouter configures the HTTP router
func (s *Server) setupRouter(log *slog.Logger) {
	// Set Gin mode based on environment variable or default to release
	serverMode := os.Getenv("ENV_SERVER_MODE")
	if serverMode == "" {
		serverMode = gin.ReleaseMode
	}
	gin.SetMode(serverMode)

	// Create a new Gin router
	r := gin.New()
	// Configure middlewares
	s.setupMiddlewares(r, log)

	// Configure health check endpoints
	s.setupHealthEndpoints(r)

	// Configure API routes
	s.setupAPIRoutes(r)

	s.router = r
}

func getLoggerFromGinContext(c *gin.Context) *slog.Logger {
	return helpers.GetLoggerFromContext(c.Request.Context())
}

// setupMiddlewares adds all necessary middleware to the router
func (s *Server) setupMiddlewares(r *gin.Engine, log *slog.Logger) {
	// Should be at the top since it puts the log object into the context for the rest
	// of the request processing
	r.Use(ginLoggerMiddleware(log))

	// Add CORS middleware early in the chain
	r.Use(corsMiddleware())

	r.Use(gin.Recovery())

	// Set up custom defaults for form-based binding
	// This is needed to handle the default values for booleans in the CSV request
	if _, ok := binding.Validator.Engine().(*validator.Validate); ok {
		r.Use(s.formDefaultsMiddleware())
	}

	r.Use(apiKeyAuthMiddleware())
	// Add rate limiting middleware
	r.Use(rateLimitMiddleware(log))

}

// formDefaultsMiddleware sets default values for form fields

// Health check godoc
//
//	@Summary		Health check endpoint
//	@Description	Check if the service is up and running
//	@Tags			health
//	@Accept			plain
//	@Produce		plain
//	@Success		200	{string}	string	"OK"
//	@Router			/health [get]
func (s *Server) setupHealthEndpoints(r *gin.Engine) {
	v1 := r.Group("/api/v1")

	v1.GET("/healthcheck", func(c *gin.Context) {
		c.String(http.StatusOK, "OK")
	})
}

//	@title			Swagger Example API
//	@version		1.0
//	@description	A lightweight Docker container that runs DuckDB with a Go API for socket connections and CSV uploads. Provides HTTP endpoints for CSV file uploads, SQL queries, and table management.
//	@termsOfService	http://swagger.io/terms/

//	@contact.name	API Support
//	@contact.url	http://www.swagger.io/support
//	@contact.email	support@swagger.io

//	@host		localhost:8080
//	@BasePath	/api/v1

// @externalDocs.description	OpenAPI
// @externalDocs.url			https://swagger.io/resources/open-api/
func (s *Server) setupAPIRoutes(r *gin.Engine) {
	// Serve the web UI at /explorer
	r.GET("/explorer", func(c *gin.Context) {
		c.File("./static/index.html")
	})

	v1 := r.Group("/api/v1")

	{
		// Upload endpoint
		v1.POST("/upload", s.handleCSVUpload())

		// Query endpoint
		v1.POST("/query", s.handleQuery())

		// Tables endpoint
		v1.GET("/tables", s.handleListTables())

		// Snapshot endpoint
		v1.POST("/snapshot", s.handleCreateSnapshot())
	}
}

// Router returns the configured router
func (s *Server) Router() http.Handler {
	return s.router
}

// handleListTables godoc
//
//	@Summary		List database tables with schema
//	@Description	Get a list of all tables in the database with their column schemas
//	@Tags			tables
//	@Accept			json
//	@Produce		json
//	@Success		200	{object}	api.TablesResponse	"List of tables with schema"
//	@Failure		500	{object}	api.ErrorResponse	"Failed to list tables"
//	@Router			/tables [get]
func (s *Server) handleListTables() gin.HandlerFunc {
	return func(c *gin.Context) {
		l := getLoggerFromGinContext(c)

		// Get all tables
		tablesResult, err := s.db.ExecuteQuery(c.Request.Context(), "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")
		if err != nil {
			l.Error("Error listing tables", slog.Any("error", err))
			c.JSON(http.StatusInternalServerError, ErrorResponse{
				Status:  "error",
				Message: "Failed to list tables",
			})
			return
		}

		var tables []TableInfo

		// For each table, get its schema
		for _, row := range tablesResult.Results {
			tableName, ok := row["table_name"].(string)
			if !ok {
				continue
			}

			// Query for column information
			columnsQuery := fmt.Sprintf(`
				SELECT column_name, data_type, is_nullable
				FROM information_schema.columns
				WHERE table_schema = 'main' AND table_name = '%s'
				ORDER BY ordinal_position`, tableName)

			columnsResult, err := s.db.ExecuteQuery(c.Request.Context(), columnsQuery)
			if err != nil {
				l.Error("Error getting table schema", slog.Any("error", err), slog.String("table", tableName))
				continue
			}

			var columns []TableColumn
			for _, colRow := range columnsResult.Results {
				column := TableColumn{
					Name:     colRow["column_name"].(string),
					Type:     colRow["data_type"].(string),
					Nullable: colRow["is_nullable"].(string) == "YES",
				}
				columns = append(columns, column)
			}

			tables = append(tables, TableInfo{
				Name:    tableName,
				Columns: columns,
			})
		}

		// Write response as JSON
		c.JSON(http.StatusOK, TablesResponse{
			Tables: tables,
		})
	}
}

// handleQuery godoc
//
//	@Summary		Execute SQL query
//	@Description	Run a SQL query against the database
//	@Tags			query
//	@Accept			json
//	@Produce		json
//	@Param			benchmark	query		boolean					false	"Include benchmark metrics in response"
//	@Param			query		body		api.QueryRequest		true	"SQL query to execute"
//	@Success		200			{object}	map[string]interface{}	"Query results"
//	@Failure		400			{object}	api.ErrorResponse		"Bad request (invalid query)"
//	@Failure		500			{object}	api.ErrorResponse		"Internal server error"
//	@Router			/query [post]
func (s *Server) handleQuery() gin.HandlerFunc {
	return func(c *gin.Context) {
		log := getLoggerFromGinContext(c)

		log.Info("Query request received", slog.String("remote_addr", c.Request.RemoteAddr))

		// Determine if benchmarks should be included in the response
		includeBenchmarks := s.shouldIncludeBenchmarks(c)

		// Parse and validate the query request
		query, limit, err := s.parseQueryRequest(c)
		if err != nil {
			return // Error response already sent
		}

		// Apply limit if specified and not already in the query
		query = s.applyQueryLimit(query, limit)

		// Log query details
		log.Info("Executing query", slog.String("query", query))
		log.Info("Include benchmarks", slog.Bool("includeBenchmarks", includeBenchmarks))

		// Execute the query
		result, err := s.executeQuery(c, query)
		if err != nil {
			log.Error("Could not execute query", slog.Any("error", err))
			// TODO: This is strange, the API should convert the error
			return // Error response already sent
		}

		// Build and send the response
		s.sendQueryResponse(c, result, includeBenchmarks)
	}
}

// shouldIncludeBenchmarks determines if benchmark data should be included in the response
func (s *Server) shouldIncludeBenchmarks(c *gin.Context) bool {
	// Priority: query parameter > environment variable > default (false)
	includeBenchmarks := os.Getenv("ENABLE_QUERY_BENCHMARKS") == "true"

	// Check query parameter (overrides environment variable)
	queryParam := c.Query("benchmark")
	if queryParam != "" {
		switch queryParam {
		case "true":
			includeBenchmarks = true
		case "false":
			includeBenchmarks = false
		}
	}

	return includeBenchmarks
}

// parseQueryRequest binds and validates the query request
func (s *Server) parseQueryRequest(c *gin.Context) (string, int, error) {
	var payload QueryRequest
	log := getLoggerFromGinContext(c)

	if err := c.ShouldBindJSON(&payload); err != nil {
		log.Error("Error binding query request", slog.Any("error", err))
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Status:  "error",
			Message: "Invalid query request: " + err.Error(),
		})
		return "", 0, err
	}

	return payload.Query, payload.Limit, nil
}

// applyQueryLimit adds a LIMIT clause to the query if not already present
func (s *Server) applyQueryLimit(query string, limit int) string {
	if limit > 0 && !strings.Contains(strings.ToUpper(query), "LIMIT") {
		query = query + fmt.Sprintf(" LIMIT %d", limit)
		// TODO: solve this
		// log.Infof("Applied limit of %d to query", limit)
	}
	return query
}

// executeQuery executes the SQL query and handles errors
func (s *Server) executeQuery(c *gin.Context, query string) (*database.QueryResult, error) {

	l := getLoggerFromGinContext(c)

	result, err := s.db.ExecuteQuery(c.Request.Context(), query)

	if err != nil {
		l.Error("Error executing query", slog.Any("error", err))
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Status:  "error",
			Message: "Failed to execute query: " + err.Error(),
		})
		return nil, err
	}

	l.Info("Query executed successfully", slog.Duration("duration", result.Duration), slog.Int("resultCount", len(result.Results)))
	return result, nil
}

// sendQueryResponse builds and sends the query response to the client
func (s *Server) sendQueryResponse(c *gin.Context, result *database.QueryResult, includeBenchmarks bool) {
	// Extract column names from first result if available
	columns := s.extractColumnNames(result)

	// Prepare response
	response := gin.H{
		"status":      "success",
		"row_count":   len(result.Results),
		"columns":     columns,
		"results":     result.Results,
		"duration_ms": result.Duration.Milliseconds(),
	}

	// Add benchmarks if enabled
	if includeBenchmarks && result.BenchmarkMetrics != nil {
		response["benchmark"] = result.BenchmarkMetrics
	}

	c.JSON(http.StatusOK, response)
}

// extractColumnNames extracts and sorts column names from query results
func (s *Server) extractColumnNames(result *database.QueryResult) []string {
	var columns []string
	if len(result.Results) > 0 {
		for col := range result.Results[0] {
			columns = append(columns, col)
		}
		sort.Strings(columns) // Sort for consistent order
	}
	return columns
}

// handleCreateSnapshot godoc
//
//	@Summary		Create database snapshot
//	@Description	Create a snapshot of the current database state and upload to S3
//	@Tags			snapshot
//	@Accept			json
//	@Produce		json
//	@Param			request	body		api.SnapshotRequest		true	"Snapshot request with bucket and key"
//	@Success		200		{object}	api.SnapshotResponse	"Snapshot created successfully"
//	@Failure		400		{object}	api.ErrorResponse		"Bad request (invalid parameters)"
//	@Failure		500		{object}	api.ErrorResponse		"Internal server error"
//	@Router			/snapshot [post]
func (s *Server) handleCreateSnapshot() gin.HandlerFunc {
	return func(c *gin.Context) {
		log := getLoggerFromGinContext(c)

		log.Info("Snapshot creation request received", slog.String("remote_addr", c.Request.RemoteAddr))

		// Parse and validate the snapshot request
		var payload SnapshotRequest
		if err := c.ShouldBindJSON(&payload); err != nil {
			log.Error("Error binding snapshot request", slog.Any("error", err))
			c.JSON(http.StatusBadRequest, ErrorResponse{
				Status:  "error",
				Message: "Invalid snapshot request: " + err.Error(),
			})
			return
		}

		// Generate timestamp-based filename
		timestamp := time.Now().Format("2006-01-02T15-04-05")
		filename := fmt.Sprintf("snapshot-%s.db", timestamp)

		// Construct full S3 key path
		fullKey := filepath.Join(payload.Key, filename)

		log.Info("Creating snapshot",
			slog.String("bucket", payload.Bucket),
			slog.String("key", fullKey),
			slog.String("filename", filename))

		// Create temporary file for the snapshot
		tempDir := os.TempDir()
		tempSnapshotPath := filepath.Join(tempDir, filename)

		// Create snapshot
		if err := s.db.CreateSnapshot(c.Request.Context(), tempSnapshotPath); err != nil {
			log.Error("Failed to create snapshot", slog.Any("error", err))
			c.JSON(http.StatusInternalServerError, ErrorResponse{
				Status:  "error",
				Message: "Failed to create snapshot: " + err.Error(),
			})
			return
		}

		// Ensure cleanup of temp file
		defer func() {
			if err := os.Remove(tempSnapshotPath); err != nil {
				log.Error("Failed to remove temporary snapshot file", slog.Any("error", err))
			}
		}()

		// Create S3 client and upload
		s3Client, err := snapshot.NewS3Client(c.Request.Context())
		if err != nil {
			log.Error("Failed to create S3 client", slog.Any("error", err))
			c.JSON(http.StatusInternalServerError, ErrorResponse{
				Status:  "error",
				Message: "Failed to create S3 client: " + err.Error(),
			})
			return
		}

		// Upload snapshot to S3
		s3URI, err := s3Client.UploadSnapshot(c.Request.Context(), tempSnapshotPath, payload.Bucket, fullKey)
		if err != nil {
			log.Error("Failed to upload snapshot to S3", slog.Any("error", err))
			c.JSON(http.StatusInternalServerError, ErrorResponse{
				Status:  "error",
				Message: "Failed to upload snapshot to S3: " + err.Error(),
			})
			return
		}

		log.Info("Snapshot created and uploaded successfully", slog.String("s3URI", s3URI))

		// Return success response
		c.JSON(http.StatusOK, SnapshotResponse{
			Status:      "success",
			SnapshotURI: s3URI,
			Filename:    filename,
		})
	}
}
