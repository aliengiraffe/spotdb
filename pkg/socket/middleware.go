package socket

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/aliengiraffe/spotdb/pkg/helpers"
)

type ConnectionID string

var connIDKey ConnectionID = ConnectionID("conn_id")

// apiKeyMiddleware validates the API key from the request header.
func apiKeyMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !helpers.IsValidAPIKeyFromHeader(&r.Header) {
			log := helpers.GetLoggerFromContext(r.Context())
			log.Info("Unauthorized access attempt",
				slog.String("reason", "invalid API key"),
				slog.String("remote_addr", r.RemoteAddr))
			http.Error(w, "Unauthorized: invalid API key", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Piggyback the baselogger to the context, its details are handled by the
// WebSocket event handler
func loggingMiddleware(baseLogger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Generate a unique connection ID
			connectionID := helpers.GenerateID()
			ctx := context.WithValue(r.Context(), connIDKey, connectionID)
			r = r.WithContext(helpers.SetLoggerInContext(ctx, baseLogger))
			next.ServeHTTP(w, r)
		})
	}
}
