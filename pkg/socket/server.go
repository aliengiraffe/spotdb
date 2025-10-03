package socket

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/aliengiraffe/spotdb/pkg/database"
	"github.com/aliengiraffe/spotdb/pkg/helpers"
	applog "github.com/aliengiraffe/spotdb/pkg/log"
	"github.com/gorilla/websocket"
)

// Server represents a socket server for DuckDB connections
type Server struct {
	db            *database.DuckDB
	address       string
	httpServer    *http.Server
	upgrader      websocket.Upgrader
	connections   map[net.Conn]struct{}
	wsConnections map[*websocket.Conn]struct{}
	mu            sync.Mutex
	wg            sync.WaitGroup
	authHandler   func(http.Handler) http.Handler
}

// WSEvent represents a client request over the socket
type WSEvent struct {
	Type  string `json:"type"`
	Query string `json:"query"`
}

// Response represents a server response over the socket
type Response struct {
	Status  string           `json:"status"`
	Results []map[string]any `json:"results,omitempty"`
	Error   string           `json:"error,omitempty"`
}

// NewServer creates a new socket server
func NewServer(db *database.DuckDB, address string) (*Server, error) {
	s := &Server{
		db:            db,
		address:       address,
		connections:   make(map[net.Conn]struct{}),
		wsConnections: make(map[*websocket.Conn]struct{}),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow connections from any origin
			},
		},
	}

	s.SetAuthHandler(apiKeyMiddleware)
	return s, nil
}

// SetAuthHandler sets an authentication middleware for the WebSocket server
func (s *Server) SetAuthHandler(handler func(http.Handler) http.Handler) {
	s.authHandler = handler
}

// Start starts the socket server
func (s *Server) Start(ctx context.Context, log *slog.Logger) error {
	// Create HTTP server for WebSocket connections
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleWebSocket)

	// Apply auth handler if set
	handler := http.Handler(mux)
	if s.authHandler != nil {
		handler = s.authHandler(mux)
	}
	// Wrap with logging middleware
	handler = loggingMiddleware(log)(handler)

	// Create the HTTP server
	s.mu.Lock()
	s.httpServer = &http.Server{
		Addr:    s.address,
		Handler: handler,
	}
	s.mu.Unlock()

	// Use a separate stopped channel to track server shutdown
	stopped := make(chan struct{})

	// Monitor context for cancellation
	go func() {
		<-ctx.Done()
		s.Stop(log) // Use the Stop method to ensure consistent shutdown
		close(stopped)
	}()

	log.Info("Starting WebSocket server", slog.String("address", s.address))

	// Start HTTP server
	err := s.httpServer.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("WebSocket server error: %w", err)
	}

	// Wait for shutdown to complete if context was cancelled
	select {
	case <-stopped:
		// Server was stopped via context cancellation
	default:
		// Server stopped for another reason
	}

	return nil
}

// Stop stops the socket server
func (s *Server) Stop(log *slog.Logger) {
	// Use a mutex to ensure thread safety
	s.mu.Lock()

	// Check if server is already in the process of being shut down
	// We'll use a temporary variable to track this rather than setting httpServer to nil
	if s.httpServer == nil {
		s.mu.Unlock()
		return
	}

	// Get a reference to the http server
	server := s.httpServer

	// Close all TCP connections
	for conn := range s.connections {
		helpers.CloseResources(conn, "TCP connection")
	}
	s.connections = make(map[net.Conn]struct{})

	// Close all WebSocket connections
	for conn := range s.wsConnections {
		helpers.CloseResources(conn, "WebSocket connection")
	}
	s.wsConnections = make(map[*websocket.Conn]struct{})
	s.mu.Unlock()

	// Create shutdown context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Shutdown the server
	if err := server.Shutdown(ctx); err != nil {
		log.Error("Error shutting down server", slog.Any("error", err))
	}

	// Wait for all handlers to finish
	s.wg.Wait()

	log.Info("Server stopped", slog.String("address", s.address))
}

// handleWebSocket handles a client connection via WebSocket
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Upgrade the HTTP connection to a WebSocket connection
	baseLogger := helpers.GetLoggerFromContext(r.Context())

	connectionID, _ := r.Context().Value(connIDKey).(string)

	rootLogger, extraLogger := applog.StartRequestLogging(baseLogger, "spotdb-ws")

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		extraLogger.Error("Error upgrading to WebSocket", slog.Any("error", err))
		return
	}

	remoteAddr := conn.RemoteAddr().String()
	extraLogger = extraLogger.With(slog.String("remote_address", remoteAddr))

	extraLogger.Info("WebSocket connection established")

	// Add connection to map
	s.mu.Lock()
	s.wsConnections[conn] = struct{}{}
	s.mu.Unlock()

	// Handle connection in a separate goroutine
	s.wg.Add(1)
	go func(conn *websocket.Conn) {
		defer s.wg.Done()
		defer helpers.CloseResources(conn, "WebSocket connection")
		defer func() {
			s.mu.Lock()
			delete(s.wsConnections, conn)
			s.mu.Unlock()
		}()

		requestCount := 0

		for {
			// Read raw WebSocket message
			_, msg, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					extraLogger.Error("Error reading WebSocket message", slog.Any("error", err))
				} else {
					extraLogger.Info("WebSocket connection closed by client", slog.Int("request_count", requestCount))
				}
				break
			}
			bytesReceived := int64(len(msg))

			// Parse JSON event
			var wsEvent WSEvent
			if err := json.Unmarshal(msg, &wsEvent); err != nil {
				extraLogger.Error("Error unmarshalling WebSocket message", slog.Any("error", err))
				break
			}

			requestCount++

			extraLogger.Info("WebSocket request received",
				slog.Int("request_number", requestCount),
				slog.String("type", wsEvent.Type),
				slog.String("query", wsEvent.Query),
			)

			start := time.Now()
			resp := s.processRequest(r.Context(), wsEvent)

			// Marshal and send response
			respMsg, err := json.Marshal(resp)
			if err != nil {
				extraLogger.Error("Error marshalling WebSocket response", slog.Any("error", err))
				break
			}
			bytesSent := int64(len(respMsg))
			if err := conn.WriteMessage(websocket.TextMessage, respMsg); err != nil {
				extraLogger.Error("Error writing WebSocket response",
					slog.String("remote_addr", remoteAddr),
					slog.Any("error", err))
				break
			}

			// Log event with byte metrics
			applog.EndWSEventLogging(rootLogger, "processRequest", "", remoteAddr, connectionID, bytesReceived, bytesSent, start)
		}
	}(conn)
}

// processRequest processes a client request and returns a response
func (s *Server) processRequest(ctx context.Context, req WSEvent) Response {
	// Process request
	var resp Response
	log := helpers.GetLoggerFromContext(ctx)
	switch req.Type {
	case "query":
		if req.Query == "" {
			log.Info("Error processing request",
				slog.String("reason", "empty query"),
			)
			resp = Response{
				Status: "error",
				Error:  "query cannot be empty",
			}
			return resp
		}

		result, err := s.db.ExecuteQuery(ctx, req.Query)
		if err != nil {
			log.Error("Error executing query",
				slog.Any("error", err))
			resp = Response{
				Status: "error",
				Error:  err.Error(),
			}
		} else {
			log.Info("Query processed successfully",
				slog.Duration("duration", result.Duration),
				slog.Int("result_count", len(result.Results)))
			resp = Response{
				Status:  "success",
				Results: result.Results,
			}
		}
	default:
		log.Info("Error processing request",
			slog.String("reason", "unknown request type"),
			slog.String("type", req.Type),
		)
		resp = Response{
			Status: "error",
			Error:  fmt.Sprintf("unknown request type: %s", req.Type),
		}
	}

	return resp
}
