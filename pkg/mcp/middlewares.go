package mcp

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/aliengiraffe/spotdb/pkg/helpers"
	applog "github.com/aliengiraffe/spotdb/pkg/log"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// contextKey is a custom type for context keys to avoid collisions
type contextKey string

const (
	requestPathKey contextKey = "request_path"
)

// apiKeyMiddleware validates API key from the request header.
func apiKeyMiddleware(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			log := helpers.GetLoggerFromContext(r.Context())
			if !helpers.IsValidAPIKeyFromHeader(&r.Header) {
				log.Info("Unauthorized access attempt",
					slog.String("reason", "invalid or missing API key"),
					slog.String("remote_addr", r.RemoteAddr),
				)
				http.Error(w, "Unauthorized: invalid API key", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// setLoggerIntoContext attaches the logger and request path into SSE server context.
func setLoggerIntoContext(log *slog.Logger) func(context.Context, *http.Request) context.Context {
	return func(ctx context.Context, r *http.Request) context.Context {
		// Store the request path in context for transport detection
		ctx = context.WithValue(ctx, requestPathKey, r.URL.Path)
		return helpers.SetLoggerInContext(ctx, log)
	}
}

// loggingMiddleware wraps a ToolHandlerFunc to log MCP events.
func loggingMiddleware(log *slog.Logger) func(server.ToolHandlerFunc) server.ToolHandlerFunc {
	return func(next server.ToolHandlerFunc) server.ToolHandlerFunc {
		return func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			start := time.Now()
			response, err := next(ctx, request)

			// Determine transport type from request path stored in context
			transport := "unknown"
			if requestPath, ok := ctx.Value(requestPathKey).(string); ok {
				if strings.Contains(requestPath, "/stream") {
					transport = "stream"
				} else if strings.Contains(requestPath, "/message") {
					transport = "sse"
				}
			}

			applog.EndMCPEventLogging(log, "abc", transport, start)
			return response, err
		}
	}
}
