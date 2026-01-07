package web

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSecurityHeaders_Basic(t *testing.T) {
	handler := SecurityHeaders(false)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	// Check all security headers are set
	tests := []struct {
		header   string
		expected string
	}{
		{"X-Frame-Options", "DENY"},
		{"X-Content-Type-Options", "nosniff"},
		{"X-XSS-Protection", "1; mode=block"},
		{"Referrer-Policy", "strict-origin-when-cross-origin"},
	}

	for _, tt := range tests {
		if got := rec.Header().Get(tt.header); got != tt.expected {
			t.Errorf("Header %s = %q, expected %q", tt.header, got, tt.expected)
		}
	}

	// CSP should be set
	csp := rec.Header().Get("Content-Security-Policy")
	if csp == "" {
		t.Error("Expected Content-Security-Policy header to be set")
	}

	// HSTS should NOT be set when TLS is disabled
	if hsts := rec.Header().Get("Strict-Transport-Security"); hsts != "" {
		t.Errorf("Expected no HSTS header when TLS disabled, got %q", hsts)
	}
}

func TestSecurityHeaders_WithTLS(t *testing.T) {
	handler := SecurityHeaders(true)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	// HSTS should be set when TLS is enabled
	hsts := rec.Header().Get("Strict-Transport-Security")
	if hsts == "" {
		t.Error("Expected HSTS header when TLS enabled")
	}
	if hsts != "max-age=31536000; includeSubDomains" {
		t.Errorf("HSTS header = %q, expected max-age with includeSubDomains", hsts)
	}
}

func TestRateLimiter_Disabled(t *testing.T) {
	rl := NewRateLimiter(RateLimiterConfig{
		Enabled:           false,
		RequestsPerSecond: 1,
		Burst:             1,
	})

	handler := rl.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Should allow all requests when disabled
	for i := 0; i < 10; i++ {
		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = "192.168.1.1:12345"
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("Request %d: expected 200, got %d", i, rec.Code)
		}
	}
}

func TestRateLimiter_Enabled(t *testing.T) {
	rl := NewRateLimiter(RateLimiterConfig{
		Enabled:           true,
		RequestsPerSecond: 1,
		Burst:             2, // Allow 2 requests initially
	})

	handler := rl.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// First 2 requests should succeed (burst)
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = "192.168.1.1:12345"
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("Request %d: expected 200, got %d", i, rec.Code)
		}
	}

	// Third request should be rate limited
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "192.168.1.1:12345"
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("Expected 429 Too Many Requests, got %d", rec.Code)
	}

	// Should have Retry-After header
	if rec.Header().Get("Retry-After") == "" {
		t.Error("Expected Retry-After header on rate limited response")
	}
}

func TestRateLimiter_PerIP(t *testing.T) {
	rl := NewRateLimiter(RateLimiterConfig{
		Enabled:           true,
		RequestsPerSecond: 1,
		Burst:             1,
	})

	handler := rl.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// First IP uses its quota
	req1 := httptest.NewRequest("GET", "/", nil)
	req1.RemoteAddr = "192.168.1.1:12345"
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Errorf("First IP first request: expected 200, got %d", rec1.Code)
	}

	// Second IP should still be allowed (different limiter)
	req2 := httptest.NewRequest("GET", "/", nil)
	req2.RemoteAddr = "192.168.1.2:12345"
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Errorf("Second IP first request: expected 200, got %d", rec2.Code)
	}

	// First IP should now be rate limited
	req3 := httptest.NewRequest("GET", "/", nil)
	req3.RemoteAddr = "192.168.1.1:12345"
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)

	if rec3.Code != http.StatusTooManyRequests {
		t.Errorf("First IP second request: expected 429, got %d", rec3.Code)
	}
}

func TestGetClientIP_RemoteAddr(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "192.168.1.100:12345"

	ip := getClientIP(req)
	if ip != "192.168.1.100" {
		t.Errorf("Expected 192.168.1.100, got %s", ip)
	}
}

func TestGetClientIP_XForwardedFor(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "10.0.0.1:12345"
	req.Header.Set("X-Forwarded-For", "203.0.113.50")

	ip := getClientIP(req)
	if ip != "203.0.113.50" {
		t.Errorf("Expected 203.0.113.50, got %s", ip)
	}
}

func TestGetClientIP_XForwardedForChain(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "10.0.0.1:12345"
	req.Header.Set("X-Forwarded-For", "203.0.113.50, 70.41.3.18, 150.172.238.178")

	ip := getClientIP(req)
	if ip != "203.0.113.50" {
		t.Errorf("Expected first IP 203.0.113.50, got %s", ip)
	}
}

func TestGetClientIP_XRealIP(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "10.0.0.1:12345"
	req.Header.Set("X-Real-IP", "203.0.113.75")

	ip := getClientIP(req)
	if ip != "203.0.113.75" {
		t.Errorf("Expected 203.0.113.75, got %s", ip)
	}
}

func TestGetClientIP_XForwardedForPrecedence(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "10.0.0.1:12345"
	req.Header.Set("X-Forwarded-For", "203.0.113.50")
	req.Header.Set("X-Real-IP", "203.0.113.75")

	// X-Forwarded-For should take precedence
	ip := getClientIP(req)
	if ip != "203.0.113.50" {
		t.Errorf("Expected X-Forwarded-For IP 203.0.113.50, got %s", ip)
	}
}

func TestGetClientIP_NoPort(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "192.168.1.100" // No port

	ip := getClientIP(req)
	if ip != "192.168.1.100" {
		t.Errorf("Expected 192.168.1.100, got %s", ip)
	}
}

func TestChainMiddleware_Empty(t *testing.T) {
	called := false
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})

	chained := ChainMiddleware(handler)

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()

	chained.ServeHTTP(rec, req)

	if !called {
		t.Error("Expected handler to be called")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", rec.Code)
	}
}

func TestChainMiddleware_Single(t *testing.T) {
	order := []string{}

	middleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			order = append(order, "middleware")
			next.ServeHTTP(w, r)
		})
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		order = append(order, "handler")
		w.WriteHeader(http.StatusOK)
	})

	chained := ChainMiddleware(handler, middleware)

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()

	chained.ServeHTTP(rec, req)

	if len(order) != 2 {
		t.Fatalf("Expected 2 calls, got %d", len(order))
	}
	if order[0] != "middleware" || order[1] != "handler" {
		t.Errorf("Expected [middleware, handler], got %v", order)
	}
}

func TestChainMiddleware_Multiple(t *testing.T) {
	order := []string{}

	middleware1 := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			order = append(order, "m1")
			next.ServeHTTP(w, r)
		})
	}

	middleware2 := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			order = append(order, "m2")
			next.ServeHTTP(w, r)
		})
	}

	middleware3 := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			order = append(order, "m3")
			next.ServeHTTP(w, r)
		})
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		order = append(order, "handler")
		w.WriteHeader(http.StatusOK)
	})

	// Chain order: m1 -> m2 -> m3 -> handler
	chained := ChainMiddleware(handler, middleware1, middleware2, middleware3)

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()

	chained.ServeHTTP(rec, req)

	expected := []string{"m1", "m2", "m3", "handler"}
	if len(order) != len(expected) {
		t.Fatalf("Expected %d calls, got %d", len(expected), len(order))
	}
	for i, v := range expected {
		if order[i] != v {
			t.Errorf("Expected order[%d] = %s, got %s", i, v, order[i])
		}
	}
}

func TestNewRateLimiter(t *testing.T) {
	cfg := RateLimiterConfig{
		Enabled:           true,
		RequestsPerSecond: 10.5,
		Burst:             20,
	}

	rl := NewRateLimiter(cfg)

	if rl == nil {
		t.Fatal("Expected non-nil rate limiter")
	}
	if !rl.enabled {
		t.Error("Expected enabled to be true")
	}
	if rl.burst != 20 {
		t.Errorf("Expected burst 20, got %d", rl.burst)
	}
	if rl.visitors == nil {
		t.Error("Expected visitors map to be initialized")
	}
}
