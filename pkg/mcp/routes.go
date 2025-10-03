package mcp

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/mark3labs/mcp-go/server"
)

// healthCheckHandler responds with a JSON status indicating the service is healthy
func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, `{"status": "healthy"}`)
}

// NewMux creates a new HTTP ServeMux with health check, SSE server, and Streamable HTTP endpoints registered.
func (a10e *A10eServer) routes(sseServer *server.SSEServer, streamableServer *server.StreamableHTTPServer, log *slog.Logger) *http.ServeMux {
	mux := http.NewServeMux()

	// Health check endpoint
	mux.HandleFunc("/health", healthCheckHandler)

	// SSE endpoints with generic middleware
	mux.Handle(sseServer.CompleteSsePath(), apiKeyMiddleware(log)(sseServer.SSEHandler()))
	mux.Handle(sseServer.CompleteMessagePath(), sseServer.MessageHandler())

	// Streamable HTTP endpoints with middleware
	mux.Handle("/stream", apiKeyMiddleware(log)(streamableServer))

	return mux
}
