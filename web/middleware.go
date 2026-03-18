package web

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// contextKey is an unexported type for context keys in this package.
type contextKey string

// nonceKey is the context key for the CSP nonce.
const nonceKey contextKey = "cspNonce"

// GetNonce returns the CSP nonce from the request context.
func GetNonce(ctx context.Context) string {
	if v, ok := ctx.Value(nonceKey).(string); ok {
		return v
	}
	return ""
}

// generateNonce creates a cryptographically random base64-encoded nonce.
func generateNonce() string {
	b := make([]byte, 16)
	rand.Read(b)
	return base64.StdEncoding.EncodeToString(b)
}

// SecurityHeaders returns middleware that adds security headers to responses.
func SecurityHeaders(tlsEnabled bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Frame-Options", "DENY")
			w.Header().Set("X-Content-Type-Options", "nosniff")
			w.Header().Set("X-XSS-Protection", "1; mode=block")
			w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")

			// Nonce-based CSP — eliminates need for unsafe-inline on scripts
			nonce := generateNonce()
			w.Header().Set("Content-Security-Policy",
				"default-src 'self'; "+
					"script-src 'self' 'nonce-"+nonce+"'; "+
					"style-src 'self' 'unsafe-inline'; "+
					"img-src 'self' data:; "+
					"frame-ancestors 'none'")

			if tlsEnabled || r.TLS != nil {
				w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
			}

			// Store nonce in context for templates
			ctx := context.WithValue(r.Context(), nonceKey, nonce)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RateLimiterConfig holds rate limiting configuration.
type RateLimiterConfig struct {
	// RequestsPerSecond is the rate limit per IP.
	RequestsPerSecond float64
	// Burst is the maximum burst size.
	Burst int
	// Enabled controls whether rate limiting is active.
	Enabled bool
	// TrustProxy controls whether X-Forwarded-For/X-Real-IP headers are trusted.
	// When false, only r.RemoteAddr is used for client IP detection.
	TrustProxy bool
}

// RateLimiter provides per-IP rate limiting.
type RateLimiter struct {
	visitors   map[string]*visitorInfo
	mu         sync.RWMutex
	rate       rate.Limit
	burst      int
	enabled    bool
	trustProxy bool
}

type visitorInfo struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// NewRateLimiter creates a new rate limiter.
func NewRateLimiter(cfg RateLimiterConfig) *RateLimiter {
	return &RateLimiter{
		visitors:   make(map[string]*visitorInfo),
		rate:       rate.Limit(cfg.RequestsPerSecond),
		burst:      cfg.Burst,
		enabled:    cfg.Enabled,
		trustProxy: cfg.TrustProxy,
	}
}

// getLimiter returns the rate limiter for the given IP.
func (rl *RateLimiter) getLimiter(ip string) *rate.Limiter {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	v, exists := rl.visitors[ip]
	if !exists {
		limiter := rate.NewLimiter(rl.rate, rl.burst)
		rl.visitors[ip] = &visitorInfo{limiter: limiter, lastSeen: time.Now()}
		return limiter
	}

	v.lastSeen = time.Now()
	return v.limiter
}

// StartCleanup spawns a goroutine that periodically evicts stale visitor entries.
// It stops when the context is cancelled.
func (rl *RateLimiter) StartCleanup(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				rl.cleanupVisitors()
			}
		}
	}()
}

// cleanupVisitors evicts visitor entries not seen in the last 5 minutes.
func (rl *RateLimiter) cleanupVisitors() {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	cutoff := time.Now().Add(-5 * time.Minute)
	for ip, v := range rl.visitors {
		if v.lastSeen.Before(cutoff) {
			delete(rl.visitors, ip)
		}
	}
}

// Middleware returns HTTP middleware that enforces rate limiting.
func (rl *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !rl.enabled {
			next.ServeHTTP(w, r)
			return
		}

		ip := getClientIP(r, rl.trustProxy)
		if !rl.getLimiter(ip).Allow() {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// getClientIP extracts the client IP from the request.
// When trustProxy is true, X-Forwarded-For and X-Real-IP headers are checked.
// When false, only r.RemoteAddr is used.
func getClientIP(r *http.Request, trustProxy bool) string {
	if trustProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			if idx := strings.Index(xff, ","); idx != -1 {
				return strings.TrimSpace(xff[:idx])
			}
			return strings.TrimSpace(xff)
		}
		if xri := r.Header.Get("X-Real-IP"); xri != "" {
			return strings.TrimSpace(xri)
		}
	}

	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}

// ChainMiddleware chains multiple middleware together.
func ChainMiddleware(h http.Handler, middlewares ...func(http.Handler) http.Handler) http.Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		h = middlewares[i](h)
	}
	return h
}
