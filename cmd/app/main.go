package app

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/aliengiraffe/spotdb/pkg/api"
	"github.com/aliengiraffe/spotdb/pkg/database"
	"github.com/aliengiraffe/spotdb/pkg/helpers"
	"github.com/aliengiraffe/spotdb/pkg/mcp"
	"github.com/aliengiraffe/spotdb/pkg/socket"
)

var (
	db         *database.DuckDB
	httpServer *http.Server
	mcpServer  *http.Server
	sockServer *socket.Server
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
)

// Run starts all components of the application
func Run(log *slog.Logger) error {
	// Create a cancelable context for graceful shutdown
	ctx, cancel = context.WithCancel(context.Background())

	// Initialize DuckDB
	var err error
	db, err = database.NewDuckDB(ctx)
	if err != nil {
		return err
	}

	log.Info("Database initialized successfully")

	// Start HTTP server for CSV uploads with API key middleware if needed
	httpServer = api.NewServer(db, log)

	// Initialize and start MCP server
	mcpServer = mcp.InitMCP(ctx, db, log)

	// Start HTTP server
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info("Starting HTTP server on :8080")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("HTTP server error", slog.Any("error", err))
		}
	}()

	// Get socket port from environment or use default
	socketPort := os.Getenv("SOCKET_PORT")
	if socketPort == "" {
		socketPort = "6033"
	}
	socketAddr := ":" + socketPort

	// Start socket server
	sockServer, err = socket.NewServer(db, socketAddr)
	if err != nil {
		return err
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info("Starting socket server", slog.String("socket_port", socketPort))
		if err := sockServer.Start(ctx, log); err != nil {
			log.Error("Socket server error", slog.Any("error", err))
		}
	}()

	// Wait a bit to ensure servers are started
	time.Sleep(100 * time.Millisecond)
	log.Info("All services started successfully")

	// Don't return - the calling function will handle signals
	<-ctx.Done()
	return nil
}

// Shutdown gracefully shuts down all components
func Shutdown(log *slog.Logger) {

	// First cancel the context to signal all components to shut down
	if cancel != nil {
		cancel()
	}

	// Shutdown HTTP server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if httpServer != nil {
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Error("HTTP server shutdown error", slog.Any("error", err))
		}
	}

	// Shutdown MCP server
	if mcpServer != nil {
		if err := mcpServer.Shutdown(shutdownCtx); err != nil {
			log.Error("MCP server shutdown error", slog.Any("error", err))
		}
	}

	// Shutdown socket server (handled via context cancel)
	if sockServer != nil {
		sockServer.Stop(log)
	}

	// Close database connection
	if db != nil {
		helpers.CloseResources(db, "database connection")
	}

	// Wait for all goroutines to finish
	wg.Wait()
}
