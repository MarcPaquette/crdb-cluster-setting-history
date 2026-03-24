package auth

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
)

// Shared bcrypt hash for "secret" — avoids repeating the expensive hash in every test.
var testPasswordHash, _ = HashPassword("secret")

func testBasicAuthConfig() Config {
	return Config{
		Enabled:      true,
		Username:     "admin",
		PasswordHash: testPasswordHash,
	}
}

func TestMiddleware(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		config         Config
		setupRequest   func(*http.Request)
		expectedStatus int
	}{
		{
			name:           "auth disabled passes through",
			config:         Config{Enabled: false},
			expectedStatus: http.StatusOK,
		},
		{
			name:           "public path bypasses auth",
			config:         Config{Enabled: true, PublicPaths: []string{"/health"}},
			setupRequest:   func(r *http.Request) { r.URL.Path = "/health" },
			expectedStatus: http.StatusOK,
		},
		{
			name:           "no credentials returns 401",
			config:         testBasicAuthConfig(),
			expectedStatus: http.StatusUnauthorized,
		},
		{
			name:   "valid basic auth",
			config: testBasicAuthConfig(),
			setupRequest: func(r *http.Request) {
				r.SetBasicAuth("admin", "secret")
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:   "invalid password",
			config: testBasicAuthConfig(),
			setupRequest: func(r *http.Request) {
				r.SetBasicAuth("admin", "wrongpassword")
			},
			expectedStatus: http.StatusUnauthorized,
		},
		{
			name:   "invalid username",
			config: testBasicAuthConfig(),
			setupRequest: func(r *http.Request) {
				r.SetBasicAuth("wronguser", "secret")
			},
			expectedStatus: http.StatusUnauthorized,
		},
		{
			name:   "valid API key",
			config: Config{Enabled: true, APIKeys: []string{"test-api-key-123"}},
			setupRequest: func(r *http.Request) {
				r.Header.Set("X-API-Key", "test-api-key-123")
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:   "invalid API key",
			config: Config{Enabled: true, APIKeys: []string{"test-api-key-123"}},
			setupRequest: func(r *http.Request) {
				r.Header.Set("X-API-Key", "wrong-api-key")
			},
			expectedStatus: http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler := Middleware(tt.config)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.setupRequest != nil {
				tt.setupRequest(req)
			}
			rec := httptest.NewRecorder()

			handler.ServeHTTP(rec, req)

			if rec.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, rec.Code)
			}
		})
	}
}

func TestMiddleware_NoCredentials_WWWAuthenticate(t *testing.T) {
	t.Parallel()

	handler := Middleware(testBasicAuthConfig())(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Header().Get("WWW-Authenticate") == "" {
		t.Error("expected WWW-Authenticate header")
	}
}

func TestParseAPIKeys(t *testing.T) {
	t.Parallel()
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
		if got := ParseAPIKeys(tt.input); !slices.Equal(got, tt.expected) {
			t.Errorf("ParseAPIKeys(%q) = %v, expected %v", tt.input, got, tt.expected)
		}
	}
}

func TestParsePublicPaths(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input    string
		expected []string
	}{
		{"", []string{"/health"}},
		{"/health,/metrics", []string{"/health", "/metrics"}},
		{" /health , /ready ", []string{"/health", "/ready"}},
	}

	for _, tt := range tests {
		if got := ParsePublicPaths(tt.input); !slices.Equal(got, tt.expected) {
			t.Errorf("ParsePublicPaths(%q) = %v, expected %v", tt.input, got, tt.expected)
		}
	}
}

func TestHashPassword(t *testing.T) {
	t.Parallel()
	hash, err := HashPassword("testpassword")
	if err != nil {
		t.Fatalf("HashPassword failed: %v", err)
	}

	if len(hash) == 0 {
		t.Error("expected non-empty hash")
	}

	hash2, _ := HashPassword("testpassword")
	if string(hash) == string(hash2) {
		t.Error("expected different hashes due to salt")
	}
}
