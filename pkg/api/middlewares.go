package api

import (
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	ratelimit "github.com/JGLTechnologies/gin-rate-limit"
	"github.com/aliengiraffe/spotdb/pkg/helpers"
	applog "github.com/aliengiraffe/spotdb/pkg/log"
	"github.com/gin-gonic/gin"
)

// keyFunc extracts the client IP for rate limiting.
func keyFunc(c *gin.Context) string {
	return c.ClientIP()
}

// errorHandler responds to rate limit violations.
func errorHandler(c *gin.Context, info ratelimit.Info) {
	waitTime := time.Until(info.ResetTime).String()
	logger := getLoggerFromGinContext(c)

	logger.Info("Rate limit exceeded",
		slog.String("clientIP", c.ClientIP()),
		slog.String("waitTime", waitTime),
	)

	c.String(http.StatusTooManyRequests, "Too many requests. Try again in "+waitTime)
}

// apiKeyAuthMiddleware validates API key from headers.
func apiKeyAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		log := getLoggerFromGinContext(c)

		if !helpers.IsValidAPIKeyFromHeader(&c.Request.Header) {
			log.Info("Unauthorized access attempt",
				slog.String("reason", "invalid or missing API key"),
				slog.String("remote_addr", c.Request.RemoteAddr),
				slog.String("path", c.Request.URL.Path),
			)

			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error":   "Unauthorized",
				"message": "Invalid or missing API Key",
			})
			return
		}

		c.Next()
	}
}

// rateLimitMiddleware creates a Gin rate limiter.
func rateLimitMiddleware(log *slog.Logger) gin.HandlerFunc {
	rpsEnv := os.Getenv("ENV_RATE_LIMIT_RPS")
	if rpsEnv == "0" || gin.Mode() != gin.ReleaseMode {
		log.Info("Rate limiting disabled",
			slog.String("mode", gin.Mode()),
			slog.String("ENV_RATE_LIMIT_RPS", rpsEnv),
		)
		return func(c *gin.Context) { c.Next() }
	}

	rateLimit := uint(5)
	if rpsEnv != "" {
		if parsed, err := strconv.ParseUint(rpsEnv, 10, 32); err == nil && parsed > 0 {
			rateLimit = uint(parsed)
		} else if err != nil {
			log.Error("Invalid ENV_RATE_LIMIT_RPS value",
				slog.String("ENV_RATE_LIMIT_RPS", rpsEnv),
				slog.Any("errorMessage", err),
			)
		}
	}

	log.Info("Rate limiting enabled", slog.Int("requests_per_second", int(rateLimit)))
	store := ratelimit.InMemoryStore(&ratelimit.InMemoryOptions{Rate: time.Second, Limit: rateLimit})
	return ratelimit.RateLimiter(store, &ratelimit.Options{ErrorHandler: errorHandler, KeyFunc: keyFunc})
}

// EndHTTPRequestLoggingGin finalizes request logging after processing.
func EndHTTPRequestLoggingGin(reqLogger *slog.Logger, c *gin.Context, start time.Time) {
	statusCode := c.Writer.Status()
	errMsg := c.Errors.ByType(gin.ErrorTypePrivate).String()
	clientIP := c.ClientIP()
	size := c.Writer.Size()

	applog.EndHTTPRequestLogging(
		reqLogger,
		c.Request,
		statusCode,
		errMsg,
		clientIP,
		size,
		start,
	)
}

// ginLoggerMiddleware attaches a per-request logger.
func ginLoggerMiddleware(baseLogger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		root, extra := applog.StartRequestLogging(baseLogger, "spotdb-api")
		c.Request = c.Request.WithContext(
			helpers.SetLoggerInContext(c.Request.Context(), extra),
		)

		start := time.Now()
		c.Next()

		EndHTTPRequestLoggingGin(root, c, start)
	}
}

// corsMiddleware handles CORS headers for the API.
func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		origin := c.Request.Header.Get("Origin")
		if origin == "" {
			origin = "*"
		}

		c.Header("Access-Control-Allow-Origin", origin)
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, PATCH")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With, X-Api-Key")
		c.Header("Access-Control-Allow-Credentials", "true")
		c.Header("Access-Control-Max-Age", "86400")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}

// formDefaultsMiddleware sets default form values for CSV uploads.
func (s *Server) formDefaultsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.Method == "POST" && c.ContentType() == "multipart/form-data" {
			if c.PostForm("has_header") == "" {
				c.Request.Form.Set("has_header", "false")
			}
			if c.PostForm("override") == "" {
				c.Request.Form.Set("override", "false")
			}
			if c.PostForm("smart") == "" {
				c.Request.Form.Set("smart", "true")
			}
		}
		c.Next()
	}
}
