package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMiddleware_AuthDisabled(t *testing.T) {
	cfg := Config{
		Enabled: false,
	}

	handler := Middleware(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestMiddleware_PublicPath(t *testing.T) {
	cfg := Config{
		Enabled:     true,
		PublicPaths: []string{"/health"},
	}

	handler := Middleware(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200 for public path, got %d", rec.Code)
	}
}

func TestMiddleware_NoCredentials(t *testing.T) {
	passwordHash, _ := HashPassword("secret")
	cfg := Config{
		Enabled:      true,
		Username:     "admin",
		PasswordHash: passwordHash,
	}

	handler := Middleware(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected status 401, got %d", rec.Code)
	}

	if rec.Header().Get("WWW-Authenticate") == "" {
		t.Error("expected WWW-Authenticate header")
	}
}

func TestMiddleware_ValidBasicAuth(t *testing.T) {
	passwordHash, _ := HashPassword("secret")
	cfg := Config{
		Enabled:      true,
		Username:     "admin",
		PasswordHash: passwordHash,
	}

	handler := Middleware(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest("GET", "/", nil)
	req.SetBasicAuth("admin", "secret")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200 with valid credentials, got %d", rec.Code)
	}
}

func TestMiddleware_InvalidPassword(t *testing.T) {
	passwordHash, _ := HashPassword("secret")
	cfg := Config{
		Enabled:      true,
		Username:     "admin",
		PasswordHash: passwordHash,
	}

	handler := Middleware(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/", nil)
	req.SetBasicAuth("admin", "wrongpassword")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected status 401 with invalid password, got %d", rec.Code)
	}
}

func TestMiddleware_InvalidUsername(t *testing.T) {
	passwordHash, _ := HashPassword("secret")
	cfg := Config{
		Enabled:      true,
		Username:     "admin",
		PasswordHash: passwordHash,
	}

	handler := Middleware(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/", nil)
	req.SetBasicAuth("wronguser", "secret")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected status 401 with invalid username, got %d", rec.Code)
	}
}

func TestMiddleware_ValidAPIKey(t *testing.T) {
	cfg := Config{
		Enabled: true,
		APIKeys: []string{"test-api-key-123"},
	}

	handler := Middleware(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-API-Key", "test-api-key-123")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200 with valid API key, got %d", rec.Code)
	}
}

func TestMiddleware_InvalidAPIKey(t *testing.T) {
	cfg := Config{
		Enabled: true,
		APIKeys: []string{"test-api-key-123"},
	}

	handler := Middleware(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-API-Key", "wrong-api-key")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected status 401 with invalid API key, got %d", rec.Code)
	}
}

func TestParseAPIKeys(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"", nil},
		{"key1", []string{"key1"}},
		{"key1,key2", []string{"key1", "key2"}},
		{"key1, key2, key3", []string{"key1", "key2", "key3"}},
		{" key1 , key2 ", []string{"key1", "key2"}},
	}

	for _, tt := range tests {
		result := ParseAPIKeys(tt.input)
		if len(result) != len(tt.expected) {
			t.Errorf("ParseAPIKeys(%q) = %v, expected %v", tt.input, result, tt.expected)
			continue
		}
		for i := range result {
			if result[i] != tt.expected[i] {
				t.Errorf("ParseAPIKeys(%q)[%d] = %q, expected %q", tt.input, i, result[i], tt.expected[i])
			}
		}
	}
}

func TestParsePublicPaths(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"", []string{"/health"}}, // default
		{"/health,/metrics", []string{"/health", "/metrics"}},
		{" /health , /ready ", []string{"/health", "/ready"}},
	}

	for _, tt := range tests {
		result := ParsePublicPaths(tt.input)
		if len(result) != len(tt.expected) {
			t.Errorf("ParsePublicPaths(%q) = %v, expected %v", tt.input, result, tt.expected)
			continue
		}
		for i := range result {
			if result[i] != tt.expected[i] {
				t.Errorf("ParsePublicPaths(%q)[%d] = %q, expected %q", tt.input, i, result[i], tt.expected[i])
			}
		}
	}
}

func TestHashPassword(t *testing.T) {
	hash, err := HashPassword("testpassword")
	if err != nil {
		t.Fatalf("HashPassword failed: %v", err)
	}

	if len(hash) == 0 {
		t.Error("expected non-empty hash")
	}

	// Hash should be different each time (due to salt)
	hash2, _ := HashPassword("testpassword")
	if string(hash) == string(hash2) {
		t.Error("expected different hashes due to salt")
	}
}
