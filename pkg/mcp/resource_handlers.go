package mcp

import (
	"context"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
)

// ResourceHandlers contains all MCP resource handlers for the A10e server
type ResourceHandlers struct {
	a10e *A10eServer
}

// NewResourceHandlers creates a new set of resource handlers for the A10e server
func NewResourceHandlers(a10e *A10eServer) *ResourceHandlers {
	return &ResourceHandlers{
		a10e: a10e,
	}
}

// HandleMemoInsights handles the memo://insights resource requests
func (r *ResourceHandlers) HandleMemoInsights(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	if request.Params.URI != "memo://insights" {
		return nil, fmt.Errorf("unknown resource: %s", request.Params.URI)
	}

	memo := r.a10e.SynthesizeMemo()

	return []mcp.ResourceContents{
		mcp.TextResourceContents{
			URI:      request.Params.URI,
			MIMEType: "text/plain",
			Text:     memo,
		},
	}, nil
}
