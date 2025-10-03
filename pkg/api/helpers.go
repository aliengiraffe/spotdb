package api

import (
	"encoding/json"
	"log"
	"net/http"
)

// writeError sends a JSON error response with the appropriate status code
// This is kept for backward compatibility with any code not yet migrated to Gin
func writeError(w http.ResponseWriter, message string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := writeJSON(w, ErrorResponse{
		Status:  "error",
		Message: message,
	}); err != nil {
		log.Printf("Error writing JSON error response: %v", err)
	}
}

// writeJSON writes a JSON response
// This is kept for backward compatibility with any code not yet migrated to Gin
func writeJSON(w http.ResponseWriter, v interface{}) error {
	enc := json.NewEncoder(w)
	// Disable HTML escaping to ensure special characters like <, >, and & are not converted to their HTML entity equivalents
	enc.SetEscapeHTML(false)
	return enc.Encode(v)
}

// readJSON reads a JSON request
// This is kept for backward compatibility with any code not yet migrated to Gin
func readJSON(r *http.Request, v interface{}) error {
	return json.NewDecoder(r.Body).Decode(v)
}
