package auth

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"
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

func TestCreateAndValidateSessionToken(t *testing.T) {
	t.Parallel()
	cfg := NewSessionConfig(false)

	token := CreateSessionToken("admin", cfg)
	if token == "" {
		t.Fatal("expected non-empty token")
	}

	username, valid := ValidateSessionToken(token, cfg)
	if !valid {
		t.Fatal("expected valid token")
	}
	if username != "admin" {
		t.Errorf("expected username 'admin', got %q", username)
	}
}

func TestSessionTokenExpiry(t *testing.T) {
	t.Parallel()
	cfg := NewSessionConfig(false)
	cfg.MaxAge = -1 * time.Second // already expired

	token := CreateSessionToken("admin", cfg)
	_, valid := ValidateSessionToken(token, cfg)
	if valid {
		t.Error("expected expired token to be invalid")
	}
}

func TestSessionTokenTampering(t *testing.T) {
	t.Parallel()
	cfg := NewSessionConfig(false)

	token := CreateSessionToken("admin", cfg)
	// Tamper with the token by flipping a character
	tampered := []byte(token)
	if tampered[0] == 'A' {
		tampered[0] = 'B'
	} else {
		tampered[0] = 'A'
	}

	_, valid := ValidateSessionToken(string(tampered), cfg)
	if valid {
		t.Error("expected tampered token to be invalid")
	}
}

func TestSessionTokenWrongSecret(t *testing.T) {
	t.Parallel()
	cfg1 := NewSessionConfig(false)
	cfg2 := NewSessionConfig(false)

	token := CreateSessionToken("admin", cfg1)
	_, valid := ValidateSessionToken(token, cfg2)
	if valid {
		t.Error("expected token validated with wrong secret to be invalid")
	}
}

func TestSetSessionCookie(t *testing.T) {
	t.Parallel()
	cfg := NewSessionConfig(false)

	w := httptest.NewRecorder()
	SetSessionCookie(w, "admin", cfg)

	cookies := w.Result().Cookies()
	if len(cookies) == 0 {
		t.Fatal("expected session cookie to be set")
	}
	cookie := cookies[0]
	if cookie.Name != "session" {
		t.Errorf("expected cookie name 'session', got %q", cookie.Name)
	}
	if !cookie.HttpOnly {
		t.Error("expected HttpOnly cookie")
	}

	// Validate the cookie value is a valid token
	username, valid := ValidateSessionToken(cookie.Value, cfg)
	if !valid {
		t.Error("expected cookie value to be a valid token")
	}
	if username != "admin" {
		t.Errorf("expected username 'admin', got %q", username)
	}
}

func TestClearSessionCookie(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	ClearSessionCookie(w)

	cookies := w.Result().Cookies()
	if len(cookies) == 0 {
		t.Fatal("expected cookie to be set")
	}
	if cookies[0].MaxAge != -1 {
		t.Errorf("expected MaxAge -1, got %d", cookies[0].MaxAge)
	}
}

func TestMiddleware_ValidSessionCookie(t *testing.T) {
	t.Parallel()
	cfg := testBasicAuthConfig()
	cfg.Session = NewSessionConfig(false)

	token := CreateSessionToken("admin", cfg.Session)

	handler := Middleware(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(&http.Cookie{Name: "session", Value: token})
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 with valid session cookie, got %d", rec.Code)
	}
}

func TestMiddleware_ExpiredSessionCookie(t *testing.T) {
	t.Parallel()
	cfg := testBasicAuthConfig()
	cfg.Session = NewSessionConfig(false)
	cfg.Session.MaxAge = -1 * time.Second

	token := CreateSessionToken("admin", cfg.Session)

	handler := Middleware(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.AddCookie(&http.Cookie{Name: "session", Value: token})
	req.Header.Set("Accept", "text/html")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusSeeOther {
		t.Errorf("expected redirect (303) with expired cookie, got %d", rec.Code)
	}
}

func TestMiddleware_BrowserRedirectToLogin(t *testing.T) {
	t.Parallel()
	cfg := testBasicAuthConfig()

	handler := Middleware(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept", "text/html,application/xhtml+xml")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusSeeOther {
		t.Errorf("expected redirect (303) for browser request, got %d", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "/login" {
		t.Errorf("expected redirect to /login, got %q", loc)
	}
}

func TestMiddleware_APIStill401(t *testing.T) {
	t.Parallel()
	cfg := testBasicAuthConfig()

	handler := Middleware(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/clusters", nil)
	req.Header.Set("Accept", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for API request, got %d", rec.Code)
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
