package mcp

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/aliengiraffe/spotdb/pkg/helpers"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// ToolHandlers contains all MCP tool handlers for the A10e server
type ToolHandlers struct {
	a10e *A10eServer
}

// NewToolHandlers creates a new set of tool handlers for the A10e server
func NewToolHandlers(a10e *A10eServer) *ToolHandlers {
	return &ToolHandlers{
		a10e: a10e,
	}
}

// ReadQueryHandler handles read_query tool requests
func (h *ToolHandlers) ReadQueryHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	l := helpers.GetLoggerFromContext(ctx)

	query, err := request.RequireString("query")
	if err != nil || query == "" {
		l.Error("Invalid tool parameter", slog.String("tool", "read_query"), slog.String("parameter", "query"), slog.String("reason", "missing or invalid"))
		return mcp.NewToolResultError("Missing or invalid query parameter"), nil
	}
	l.Info("Executing read_query", slog.String("query", query))

	if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "SELECT") {
		l.Error("Invalid query type", slog.String("tool", "read_query"), slog.String("query", query), slog.String("reason", "non-SELECT query"))
		return mcp.NewToolResultError("Only SELECT queries are allowed for read_query"), nil
	}

	result, err := h.a10e.ExecuteQuery(ctx, query)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Query error: %v", err)), nil
	}

	return mcp.NewToolResultText(result), nil
}

// WriteQueryHandler handles write_query tool requests
func (h *ToolHandlers) WriteQueryHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	l := helpers.GetLoggerFromContext(ctx)

	query, err := request.RequireString("query")
	if err != nil || query == "" {
		l.Error("Invalid tool parameter", slog.String("tool", "write_query"), slog.String("parameter", "query"), slog.String("reason", "missing or invalid"))
		return mcp.NewToolResultError("Missing or invalid query parameter"), nil
	}
	l.Info("Executing write_query", slog.String("query", query))

	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "SELECT") {
		return mcp.NewToolResultError("SELECT queries are not allowed for write_query"), nil
	}

	result, err := h.a10e.ExecuteQuery(ctx, query)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Query error: %v", err)), nil
	}

	return mcp.NewToolResultText(result), nil
}

// CreateDatasourceHandler handles create_datasource tool requests
func (h *ToolHandlers) CreateDatasourceHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	l := helpers.GetLoggerFromContext(ctx)

	query, err := request.RequireString("query")
	if err != nil || query == "" {
		l.Error("Invalid tool parameter", slog.String("tool", "create_datasource"), slog.String("parameter", "query"), slog.String("reason", "missing or invalid"))
		return mcp.NewToolResultError("Missing or invalid query parameter"), nil
	}
	l.Info("Executing create_datasource", slog.String("query", query))

	if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "CREATE TABLE") {
		return mcp.NewToolResultError("Only CREATE TABLE statements are allowed"), nil
	}

	result, err := h.a10e.ExecuteQuery(ctx, query)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Query error: %v", err)), nil
	}

	return mcp.NewToolResultText(result), nil
}

// ListDatasourcesHandler handles list_datasources tool requests
func (h *ToolHandlers) ListDatasourcesHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	result, err := h.a10e.ExecuteQuery(ctx, "LS")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Query error: %v", err)), nil
	}

	return mcp.NewToolResultText(result), nil
}

// LoadDatasourceHandler handles load_datasource tool requests
func (h *ToolHandlers) LoadDatasourceHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	datasourceName, err := request.RequireString("datasource_name")
	if err != nil || datasourceName == "" {
		return mcp.NewToolResultError("Missing or invalid datasource_name parameter"), nil
	}

	result, err := h.a10e.ExecuteQuery(ctx, fmt.Sprintf("LOAD %s", datasourceName))
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Query error: %v", err)), nil
	}

	return mcp.NewToolResultText(result), nil
}

// DescribeDatasourceHandler handles describe_datasource tool requests
func (h *ToolHandlers) DescribeDatasourceHandler(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	datasourceName, err := request.RequireString("datasource_name")
	if err != nil || datasourceName == "" {
		return mcp.NewToolResultError("Missing or invalid datasource_name parameter"), nil
	}

	// First load the datasource
	_, err = h.a10e.ExecuteQuery(ctx, fmt.Sprintf("LOAD %s", datasourceName))
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Error loading datasource: %v", err)), nil
	}

	// Then describe it
	var result string
	result, err = h.a10e.ExecuteQuery(ctx, fmt.Sprintf("DESCRIBE %s", datasourceName))
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Query error: %v", err)), nil
	}
	return mcp.NewToolResultText(result), nil
}

// AppendInsightHandler handles append_insight tool requests
func (h *ToolHandlers) AppendInsightHandler(ctx context.Context, request mcp.CallToolRequest, mcpServer *server.MCPServer) (*mcp.CallToolResult, error) {
	l := helpers.GetLoggerFromContext(ctx)

	insight, err := request.RequireString("insight")
	if err != nil || insight == "" {
		return mcp.NewToolResultError("Missing or invalid insight parameter"), nil
	}

	// Add the insight to the list
	l.Info("Adding new business insight", slog.String("insight", insight))
	h.a10e.insights = append(h.a10e.insights, insight)

	// Notify clients that the memo resource has been updated
	if err := mcpServer.SendNotificationToClient(ctx, "resource/updated", map[string]any{
		"uri": "memo://insights",
	}); err != nil {
		l.Error("Failed to send resource updated notification", slog.Any("error", err))
	}

	return mcp.NewToolResultText("Insight added to memo"), nil
}
