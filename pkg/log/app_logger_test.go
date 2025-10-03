package log_test

import (
	"bytes"
	"log/slog"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	logpkg "github.com/aliengiraffe/spotdb/pkg/log"
)

// newTestLogger creates an slog.Logger that writes JSON to the given buffer.
func newTestLogger(buf *bytes.Buffer) *slog.Logger {
	handler := slog.NewJSONHandler(buf, &slog.HandlerOptions{AddSource: false, Level: slog.LevelDebug})
	return slog.New(handler)
}

func TestStartRequestLogging(t *testing.T) {
	buf := &bytes.Buffer{}
	base := newTestLogger(buf)
	root, _ := logpkg.StartRequestLogging(base, "svc-test")
	root.Info("hello world")
	out := buf.String()
	if !strings.Contains(out, `"service"`) {
		t.Errorf("missing service group: %s", out)
	}
	if !strings.Contains(out, `"name":"svc-test"`) {
		t.Errorf("missing service name: %s", out)
	}
	if !strings.Contains(out, `"msg":"hello world"`) {
		t.Errorf("missing hello world message: %s", out)
	}
}

func TestEndHTTPRequestLogging_NoError_NoQuery(t *testing.T) {
	buf := &bytes.Buffer{}
	base := newTestLogger(buf)
	_, extra := logpkg.StartRequestLogging(base, "svc-http")
	req := httptest.NewRequest("POST", "http://example.com/foo", nil)
	req.ContentLength = 123
	start := time.Now().Add(-50 * time.Millisecond)
	logpkg.EndHTTPRequestLogging(extra, req, 200, "", "127.0.0.1", 456, start)
	out := buf.String()
	if !strings.Contains(out, "Request completed") {
		t.Errorf("missing Request completed message: %s", out)
	}
	if !strings.Contains(out, `"status_code":200`) {
		t.Errorf("missing status_code: %s", out)
	}
	if strings.Contains(out, "?") {
		// should not include query string
		if strings.Contains(out, "/foo?") {
			t.Errorf("unexpected query in path: %s", out)
		}
	}
}

func TestEndHTTPRequestLogging_ErrorAndQuery(t *testing.T) {
	buf := &bytes.Buffer{}
	base := newTestLogger(buf)
	_, extra := logpkg.StartRequestLogging(base, "svc-http-err")
	req := httptest.NewRequest("GET", "http://example.com/bar?x=1&y=2", nil)
	start := time.Now().Add(-1 * time.Second)
	logpkg.EndHTTPRequestLogging(extra, req, 500, "oops", "10.0.0.1", 789, start)
	out := buf.String()
	if !strings.Contains(out, "Request completed") {
		t.Errorf("missing Request completed message: %s", out)
	}
	if !strings.Contains(out, `"url_path":"/bar?x=1&y=2"`) {
		t.Errorf("missing full url_path: %s", out)
	}
	if !strings.Contains(out, `"status_code":500`) {
		t.Errorf("missing status_code 500: %s", out)
	}
}

func TestEndWSEventLogging_NoError(t *testing.T) {
	buf := &bytes.Buffer{}
	base := newTestLogger(buf)
	_, extra := logpkg.StartRequestLogging(base, "svc-ws")
	start := time.Now().Add(-10 * time.Millisecond)
	logpkg.EndWSEventLogging(extra, "myEvent", "", "192.168.0.1", "conn1", 100, 200, start)
	out := buf.String()
	if !strings.Contains(out, "WebSocket response sent") {
		t.Errorf("missing WebSocket response sent: %s", out)
	}
	if !strings.Contains(out, `"event_name":"myEvent"`) {
		t.Errorf("missing event_name: %s", out)
	}
}

func TestEndWSEventLogging_Error(t *testing.T) {
	buf := &bytes.Buffer{}
	base := newTestLogger(buf)
	_, extra := logpkg.StartRequestLogging(base, "svc-ws-err")
	start := time.Now().Add(-10 * time.Millisecond)
	logpkg.EndWSEventLogging(extra, "evt", "errMsg", "192.168.0.2", "conn2", 1000, 2000, start)
	out := buf.String()
	if !strings.Contains(out, "WebSocket response sent") {
		t.Errorf("missing WebSocket response sent: %s", out)
	}
}

func TestEndMCPEventLogging(t *testing.T) {
	buf := &bytes.Buffer{}
	base := newTestLogger(buf)
	_, extra := logpkg.StartRequestLogging(base, "svc-mcp")
	start := time.Now().Add(-5 * time.Millisecond)
	logpkg.EndMCPEventLogging(extra, "conn-id", "sse", start)
	out := buf.String()
	if !strings.Contains(out, "MCP response sent") {
		t.Errorf("missing MCP response sent: %s", out)
	}
	if !strings.Contains(out, `"tool_called":"mytool"`) {
		t.Errorf("missing tool_called: %s", out)
	}
	if !strings.Contains(out, `"transport":"sse"`) {
		t.Errorf("missing transport: %s", out)
	}
}

func TestGetLogger(t *testing.T) {
	// Test logger in "test" environment
	os.Setenv("ENV_SERVER_MODE", "test")
	defer os.Unsetenv("ENV_SERVER_MODE")
	l1 := logpkg.GetLogger("svc")
	if l1 == nil {
		t.Error("expected logger for test env, got nil")
	}
	// Test logger in "release" environment
	os.Setenv("ENV_SERVER_MODE", "release")
	l2 := logpkg.GetLogger("svc")
	if l2 == nil {
		t.Error("expected logger for release env, got nil")
	}
}
