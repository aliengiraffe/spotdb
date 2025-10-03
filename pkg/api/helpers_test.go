package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aliengiraffe/spotdb/pkg/helpers"
)

func init() {
	helpers.SilenceLogOutput()
}

func TestWriteError(t *testing.T) {
	tests := []struct {
		name       string
		message    string
		code       int
		expectBody ErrorResponse
	}{
		{
			name:    "basic error",
			message: "something went wrong",
			code:    http.StatusBadRequest,
			expectBody: ErrorResponse{
				Status:  "error",
				Message: "something went wrong",
			},
		},
		{
			name:    "server error",
			message: "internal error",
			code:    http.StatusInternalServerError,
			expectBody: ErrorResponse{
				Status:  "error",
				Message: "internal error",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			writeError(w, tc.message, tc.code)

			if w.Code != tc.code {
				t.Errorf("expected status code %d, got %d", tc.code, w.Code)
			}

			if ct := w.Header().Get("Content-Type"); ct != "application/json" {
				t.Errorf("expected Content-Type application/json, got %s", ct)
			}

			var got ErrorResponse
			if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
				t.Fatalf("failed to decode response: %v", err)
			}

			if got != tc.expectBody {
				t.Errorf("expected body %+v, got %+v", tc.expectBody, got)
			}
		})
	}
}

func TestReadJSON(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		expectError bool
	}{
		{
			name: "valid json",
			body: `{"status":"ok","message":"test"}`},
		{
			name:        "invalid json",
			body:        `{invalid}`,
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(tc.body))
			var v ErrorResponse
			err := readJSON(req, &v)

			if tc.expectError && err == nil {
				t.Error("expected error, got nil")
			}
			if !tc.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestWriteJSON(t *testing.T) {
	tests := []struct {
		name    string
		payload interface{}
	}{
		{
			name: "error response",
			payload: ErrorResponse{
				Status:  "error",
				Message: "test error",
			},
		},
		{
			name: "simple map",
			payload: map[string]string{
				"key": "value",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			err := writeJSON(w, tc.payload)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Verify we can decode it back
			var got interface{}
			if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
				t.Fatalf("failed to decode response: %v", err)
			}
		})
	}
}
