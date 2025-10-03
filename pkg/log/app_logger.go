package log

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/aliengiraffe/spotdb/pkg/helpers"

	prettyconsole "github.com/thessem/zap-prettyconsole"
	"go.uber.org/zap"
	"go.uber.org/zap/exp/zapslog"
)

// StartRequestLogging initializes and returns a logger for the request with static fields.
// It sets up service, host, and trace groups based on the request context.
func StartRequestLogging(base *slog.Logger, serviceName string) (*slog.Logger, *slog.Logger) {
	rootLevel := base.With(
		slog.Group("service",
			slog.String("name", serviceName),
			slog.String("version", "0.0.1-beta"),
			slog.String("environment", helpers.GetServerMode()), // TODO: add this value dynamically
		),
		slog.Group("host",
			slog.String("hostname", helpers.GetHostname()),
		),
		slog.Group("trace",
			// TODO: generate real trace IDs
			slog.String("id", "123"),
			slog.String("span_id", "123"),
		),
	)

	return rootLevel, rootLevel.WithGroup("extra_data")
}

// EndHTTPRequestLogging enriches the per-request logger with dynamic fields and logs the completion.
// This version uses standard Go objects instead of gin.Context
func EndHTTPRequestLogging(
	reqLogger *slog.Logger,
	req *http.Request,
	statusCode int,
	errorMessage string,
	clientIP string,
	responseSize int,
	start time.Time,
) {
	duration, duration_ms := getDuration(start)

	// Build request path with query parameters if any
	path := req.URL.Path
	if raw := req.URL.RawQuery; raw != "" {
		path += "?" + raw
	}
	method := req.Method
	userAgent := req.UserAgent()
	bodySize := req.ContentLength

	// Enrich logger with dynamic details and log the request completion

	if errorMessage != "" {
		reqLogger.With("error",
			slog.String("message", errorMessage),
		)
	}

	reqLogger.With(
		slog.Group("event_details",
			slog.Group("http",
				slog.Group("request",
					slog.String("method", method),
					slog.String("url_path", path),
					slog.String("user_agent", userAgent),
					slog.Int64("bytes", bodySize),
					slog.Group("headers",
						slog.String("Content-Type", "foo"),
						slog.String("X-Request-ID", "foo"),
					),
				),
				slog.Group("response",
					slog.Int("status_code", statusCode),
					slog.Int("bytes", responseSize),
				),
			),
			slog.Group("network",
				slog.Group("client",
					slog.String("ip", clientIP),
				),
			),
			slog.Group("user",
				slog.String("id", "foo"),
			),
			slog.String("type", "http_request"),
			slog.Duration("duration", duration),
			slog.Int64("duration", duration_ms),
		),
	).Info("Request completed")
}

// EndWSEventLogging enriches the per-request logger with WebSocket event details
func EndWSEventLogging(
	reqLogger *slog.Logger,
	eventName string,
	errorMessage string,
	clientIP string,
	connectionId string,
	bytesReceived int64,
	bytesSent int64,
	start time.Time,
) {

	duration, duration_ms := getDuration(start)

	if errorMessage != "" {
		reqLogger.With("error",
			slog.String("message", errorMessage),
		)
	}

	reqLogger.With(
		slog.Group("event_details",
			slog.Group("socket",
				slog.String("event_name", eventName),
				slog.String("transport", "websocket"),
				slog.String("connection_id", connectionId),
			),
			slog.Group("network",
				slog.Group("client",
					slog.String("ip", clientIP),
				),
				slog.Int64("bytes_received", bytesReceived),
				slog.Int64("bytes_sent", bytesSent),
			),
			slog.Group("user",
				slog.String("id", "foo"),
			),
			slog.String("type", "ws_event"),
			slog.Duration("duration", duration),
			slog.Int64("duration_ms", duration_ms),
		),
	).Info("WebSocket response sent")
}

// EndMCPEventLogging enriches the per-request logger with MCP event details
func EndMCPEventLogging(
	reqLogger *slog.Logger,
	connectionId string,
	transport string,
	start time.Time,
) {

	duration, duration_ms := getDuration(start)

	reqLogger.With(
		slog.Group("event_details",
			slog.Group("mcp",
				slog.String("tool_called", "mytool"),
				slog.String("transport", transport),
				slog.String("connection_id", connectionId),
			),
			slog.String("type", "mcp_event"),
			slog.Duration("duration", duration),
			slog.Int64("duration_ms", duration_ms),
		),
	).Info("MCP response sent")
}

func getDuration(start time.Time) (time.Duration, int64) {
	duration := time.Since(start)

	return duration, duration.Milliseconds()
}

// GetLogger returns an slog.Logger, colorizing log levels in non-release environments
// according to the ENV_SERVER_MODE environment variable.
func GetLogger(serviceName string) *slog.Logger {
	var l *zap.Logger
	// Determine environment from ENV_SERVER_MODE (empty default if unset)
	environment := helpers.GetServerMode()

	// Set production config by default
	config := zap.NewProductionConfig()

	// When running tests, switch to no-op logger
	if environment == "test" {
		config = zap.NewDevelopmentConfig()
		// Disable all outputs
		config.OutputPaths = []string{}
		config.ErrorOutputPaths = []string{}
	}

	// When not in release environment
	if environment != "release" {
		config = prettyconsole.NewConfig()
	}

	l, _ = config.Build(zap.AddCaller())

	// Include call site (file and line number) in log output
	slogHandler := zapslog.NewHandler(l.Core(), zapslog.WithCaller(true))

	return slog.New(slogHandler)

}
