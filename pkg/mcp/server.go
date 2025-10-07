package mcp

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/aliengiraffe/spotdb/pkg/database"
	"github.com/aliengiraffe/spotdb/pkg/helpers"
	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// A10eServer represents the MCP server for A10e
type A10eServer struct {
	db       *database.DuckDB
	insights []string
}

// NewA10eServer creates a new A10e MCP server with an in-memory DuckDB database
func NewA10eServer(db *database.DuckDB, log *slog.Logger) *A10eServer {
	// Test the connection
	if err := db.GetDB().Ping(); err != nil {
		log.Error("Failed to connect to DuckDB", slog.Any("error", err))
	}

	log.Info("Database connection established", slog.String("db_type", "in-memory DuckDB"))

	return &A10eServer{
		db:       db,
		insights: []string{},
	}
}

// ExecuteQuery executes a SQL query and returns results
func (s *A10eServer) ExecuteQuery(ctx context.Context, query string) (string, error) {
	// Handle special commands for the database
	query = strings.TrimSpace(query)
	queryUpper := strings.ToUpper(query)

	log := helpers.GetLoggerFromContext(ctx)

	log.Info("Executing query", slog.String("query", query))

	// Handle special commands
	if queryUpper == "LS" {
		return s.listTables(ctx)
	} else if strings.HasPrefix(queryUpper, "LOAD ") {
		// LOAD is a no-op in our implementation since tables are always available
		tableName := strings.TrimSpace(query[5:])
		return fmt.Sprintf("Table '%s' loaded successfully", tableName), nil
	} else if strings.HasPrefix(queryUpper, "DESCRIBE ") {
		tableName := strings.TrimSpace(query[9:])
		return s.describeTable(ctx, tableName)
	}

	// For SELECT queries, return formatted results
	if strings.HasPrefix(queryUpper, "SELECT") {
		return s.executeSelectQuery(ctx, query)
	}

	// For other queries (CREATE, INSERT, UPDATE, DELETE), just execute them
	_, err := s.db.ExecuteQuery(ctx, query)
	if err != nil {
		log.Error("Query execution failed", slog.Any("error", err))
		return "", fmt.Errorf("query execution error: %w", err)
	}

	// For non-SELECT queries, return success message
	log.Info("Query executed successfully", slog.String("type", "non-SELECT"))
	return "Query executed successfully.", nil
}

// listTables returns a list of all tables in the database
func (s *A10eServer) listTables(ctx context.Context) (string, error) {
	// Query to get all tables in DuckDB
	result, err := s.db.ExecuteQuery(ctx, "SHOW TABLES")
	if err != nil {
		return "", fmt.Errorf("error listing tables: %w", err)
	}

	var tables []string
	for _, row := range result.Results {
		// The column name might be "name" or "table_name" depending on DuckDB version
		var tableName string
		if name, ok := row["name"]; ok {
			tableName = fmt.Sprintf("%v", name)
		} else if name, ok := row["table_name"]; ok {
			tableName = fmt.Sprintf("%v", name)
		}

		if tableName != "" {
			tables = append(tables, tableName)
		}
	}

	if len(tables) == 0 {
		return "No tables found.", nil
	}

	var resultStr strings.Builder
	resultStr.WriteString("Tables:\n")
	for _, table := range tables {
		resultStr.WriteString(fmt.Sprintf("- %s\n", table))
	}

	return resultStr.String(), nil
}

// describeTable returns the schema of a table
func (s *A10eServer) describeTable(ctx context.Context, tableName string) (string, error) {
	// Use PRAGMA table_info to get column information
	query := fmt.Sprintf("PRAGMA table_info(%s)", tableName)

	result, err := s.db.ExecuteQuery(ctx, query)
	if err != nil {
		return "", fmt.Errorf("error describing table %s: %w", tableName, err)
	}

	var columns []struct {
		Name       string
		Type       string
		IsNullable string
	}

	for _, row := range result.Results {
		var col struct {
			Name       string
			Type       string
			IsNullable string
		}

		// Extract column information from the PRAGMA result
		// Column names in PRAGMA table_info: cid, name, type, notnull, dflt_value, pk
		col.Name = fmt.Sprintf("%v", row["name"])
		col.Type = fmt.Sprintf("%v", row["type"])

		// In DuckDB, notnull is 1 if NOT NULL, 0 if nullable
		notNull, _ := row["notnull"].(float64)
		if notNull == 1 {
			col.IsNullable = "NO"
		} else {
			col.IsNullable = "YES"
		}

		columns = append(columns, col)
	}

	if len(columns) == 0 {
		return fmt.Sprintf("Table '%s' not found or has no columns.", tableName), nil
	}

	var resultStr strings.Builder
	resultStr.WriteString("Column Name, Data Type, Nullable\n")
	for _, col := range columns {
		resultStr.WriteString(fmt.Sprintf("%s, %s, %s\n", col.Name, col.Type, col.IsNullable))
	}

	return resultStr.String(), nil
}

// executeSelectQuery executes a SELECT query and returns formatted results
func (s *A10eServer) executeSelectQuery(ctx context.Context, query string) (string, error) {
	result, err := s.db.ExecuteQuery(ctx, query)
	if err != nil {
		return "", fmt.Errorf("error executing SELECT query: %w", err)
	}

	if len(result.Results) == 0 {
		return "Query executed successfully, but no rows were returned.", nil
	}

	// Get column names from the first row
	var columns []string
	if len(result.Results) > 0 {
		for col := range result.Results[0] {
			columns = append(columns, col)
		}
	}

	if len(columns) == 0 {
		return "Query executed successfully, but no columns were returned.", nil
	}

	// Build the result table
	var resultStr strings.Builder

	// Create header row with column names
	var headerRow strings.Builder
	var separatorRow strings.Builder

	for _, col := range columns {
		headerRow.WriteString(fmt.Sprintf("| %-15s ", col))
		separatorRow.WriteString("+------------------")
	}
	headerRow.WriteString("|\n")
	separatorRow.WriteString("+\n")

	resultStr.WriteString(separatorRow.String())
	resultStr.WriteString(headerRow.String())
	resultStr.WriteString(separatorRow.String())

	// Add data rows
	rowCount := 0
	for _, row := range result.Results {
		rowCount++

		// Convert each value to string and add to result
		var rowStr strings.Builder
		for i, col := range columns {
			if i >= 10 { // Limit to 10 columns for display
				if len(columns) > 10 {
					rowStr.WriteString(fmt.Sprintf("| (%d more) ", len(columns)-10))
				}
				break
			}

			val := row[col]
			var valStr string
			if val == nil {
				valStr = "NULL"
			} else {
				valStr = fmt.Sprintf("%v", val)
			}

			// Truncate long values
			if len(valStr) > 15 {
				valStr = valStr[:12] + "..."
			}

			rowStr.WriteString(fmt.Sprintf("| %-15s ", valStr))
		}
		rowStr.WriteString("|\n")
		resultStr.WriteString(rowStr.String())

		// Limit to 100 rows for display
		if rowCount >= 100 {
			resultStr.WriteString("... (more rows)\n")
			break
		}
	}

	resultStr.WriteString(separatorRow.String())

	// Add row count summary
	switch rowCount {
	case 0:
		resultStr.WriteString("No rows returned.\n")
	case 1:
		resultStr.WriteString("1 row returned.\n")
	default:
		resultStr.WriteString(fmt.Sprintf("%d rows returned.\n", rowCount))
	}

	return resultStr.String(), nil
}

// SynthesizeMemo creates a formatted memo from insights
func (s *A10eServer) SynthesizeMemo() string {
	if len(s.insights) == 0 {
		return "No business insights have been discovered yet."
	}

	var insightsText strings.Builder
	for _, insight := range s.insights {
		insightsText.WriteString(fmt.Sprintf("- %s\n", insight))
	}

	memo := "📊 Business Intelligence Memo 📊\n\n"
	memo += "Key Insights Discovered:\n\n"
	memo += insightsText.String()

	if len(s.insights) > 1 {
		memo += "\nSummary:\n"
		memo += fmt.Sprintf("Analysis has revealed %d key business insights that suggest opportunities for strategic optimization and growth.", len(s.insights))
	}

	return memo
}

// InitMCP initializes the A10e MCP server by configuring its HTTP and SSE interfaces and registering resources, prompts, and tool handlers.
// It sets up a health check endpoint, conditionally applies API key authentication based on the environment, and uses the provided DuckDB
// connection for executing SQL queries throughout the server's operations. The function also starts a goroutine to run the SSE server
// and configures handlers for operational tools such as query execution and datasource management.
// Returns the HTTP server for graceful shutdown.
func InitMCP(ctx context.Context, db *database.DuckDB, log *slog.Logger) *http.Server {
	// Create A10e server instance
	log.Info("Initializing A10e MCP Server", slog.String("version", "1.0"))
	a10e := NewA10eServer(db, log)

	// Create handlers
	toolHandlers := NewToolHandlers(a10e)
	resourceHandlers := NewResourceHandlers(a10e)
	promptHandlers := NewPromptHandlers()

	// Create MCP server
	log.Info("Creating MCP server", slog.Bool("resource_capabilities", true))
	s := server.NewMCPServer(
		"A10e MCP Server",
		"1.0.0",
		server.WithResourceCapabilities(true, true),
		server.WithLogging(),
		server.WithRecovery(),
		server.WithToolHandlerMiddleware(loggingMiddleware(log)),
	)

	// Add resources
	log.Info("Registering resource", slog.String("name", "memo://insights"), slog.String("type", "business insights"))
	s.AddResource(mcp.NewResource(
		"memo://insights",
		"Business Insights Memo",
		mcp.WithResourceDescription("A living document of discovered business insights"),
		mcp.WithMIMEType("text/plain"),
	), resourceHandlers.HandleMemoInsights)

	// Add prompt
	log.Info("Registering prompt", slog.String("name", "a10e-demo"))
	s.AddPrompt(mcp.NewPrompt("a10e-demo",
		mcp.WithPromptDescription("A prompt to demonstrate what you can do with a A10e MCP Server + Claude"),
		mcp.WithArgument("topic",
			mcp.ArgumentDescription("Topic to seed the database with initial data"),
			mcp.RequiredArgument(),
		),
	), promptHandlers.HandleA10eDemo)

	// Add tools and add tool handlers
	log.Info("Adding tool handler", slog.String("tool", "read_query"))
	s.AddTool(mcp.NewTool("read_query",
		mcp.WithDescription("Execute a SELECT query on the A10e server. Make sure the datasource is loaded first."),
		mcp.WithString("query",
			mcp.Required(),
			mcp.Description("SELECT SQL query to execute"),
		),
	), toolHandlers.ReadQueryHandler)

	log.Info("Adding tool handler", slog.String("tool", "write_query"))
	s.AddTool(mcp.NewTool("write_query",
		mcp.WithDescription("Execute an INSERT, UPDATE, or DELETE query on the A10e server. Make sure the datasource is loaded first."),
		mcp.WithString("query",
			mcp.Required(),
			mcp.Description("SQL query to execute"),
		),
	), toolHandlers.WriteQueryHandler)

	log.Info("Adding tool handler", slog.String("tool", "create_datasource"))
	s.AddTool(mcp.NewTool("create_datasource",
		mcp.WithDescription("Create a new datasource in the A10e server"),
		mcp.WithString("query",
			mcp.Required(),
			mcp.Description("CREATE TABLE SQL statement"),
		),
	), toolHandlers.CreateDatasourceHandler)

	log.Info("Adding tool handler", slog.String("tool", "list_datasources"))
	s.AddTool(mcp.NewTool("list_datasources",
		mcp.WithDescription("List all data sources in the A10e server"),
	), toolHandlers.ListDatasourcesHandler)

	log.Info("Adding tool handler", slog.String("tool", "load_datasource"))
	s.AddTool(mcp.NewTool("load_datasource",
		mcp.WithDescription("Load a datasource into the A10e server"),
		mcp.WithString("datasource_name",
			mcp.Required(),
			mcp.Description("Name of the datasource to load"),
		),
	), toolHandlers.LoadDatasourceHandler)

	log.Info("Adding tool handler", slog.String("tool", "describe_datasource"))
	s.AddTool(mcp.NewTool("describe_datasource",
		mcp.WithDescription("Get the schema information for a specific datasource. Make sure the datasource is loaded first."),
		mcp.WithString("datasource_name",
			mcp.Required(),
			mcp.Description("Name of the datasource to describe"),
		),
	), toolHandlers.DescribeDatasourceHandler)

	log.Info("Adding tool handler", slog.String("tool", "append_insight"))
	appendInsightTool := mcp.NewTool("append_insight",
		mcp.WithDescription("Add a business insight to the memo"),
		mcp.WithString("insight",
			mcp.Required(),
			mcp.Description("Business insight discovered from data analysis"),
		),
	)
	// Special case for append_insight because it needs the MCP server reference
	// TODO: There might be a smarter way of doing this
	s.AddTool(appendInsightTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return toolHandlers.AppendInsightHandler(ctx, request, s)
	})

	// Create SSE server with HTTP server configuration and proxy-aware base path
	log.Info("Creating SSE server", slog.String("config_type", "HTTP"))
	sseServer := server.NewSSEServer(
		s,
		server.WithSSEContextFunc(setLoggerIntoContext(log)),
		server.WithDynamicBasePath(func(r *http.Request, sessionId string) string {
			// Use X-Forwarded-Prefix header set by nginx proxy
			forwardedPrefix := r.Header.Get("X-Forwarded-Prefix")

			if forwardedPrefix != "" {
				log.Info("Using proxy-aware base path from X-Forwarded-Prefix",
					slog.String("base_path", forwardedPrefix),
					slog.String("session_id", sessionId),
				)
				return forwardedPrefix
			}

			// For direct connections (no proxy), use empty base path
			return ""
		}),
	)

	// Create Streamable HTTP server
	log.Info("Creating Streamable HTTP server", slog.String("config_type", "Streamable HTTP"))
	streamableServer := server.NewStreamableHTTPServer(
		s,
		server.WithHTTPContextFunc(setLoggerIntoContext(log)),
		server.WithEndpointPath("/stream"),
	)

	// Create HTTP server
	mcpServer := &http.Server{
		Addr:    ":8081",
		Handler: a10e.routes(sseServer, streamableServer, log),
	}

	// Start server in goroutine
	go func() {
		log.Info("Starting MCP HTTP server", slog.String("port", "8081"))
		if err := mcpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("MCP server error", slog.Any("error", err))
		}
	}()

	return mcpServer
}
