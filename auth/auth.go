package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// SessionConfig holds session token configuration.
type SessionConfig struct {
	Secret []byte
	MaxAge time.Duration
	Secure bool
}

const (
	sessionCookieName  = "session"
	defaultSessionMaxAge = 24 * time.Hour
)

// NewSessionConfig creates a new SessionConfig with a random 32-byte secret.
func NewSessionConfig(secure bool) SessionConfig {
	secret := make([]byte, 32)
	rand.Read(secret)
	return SessionConfig{
		Secret: secret,
		MaxAge: defaultSessionMaxAge,
		Secure: secure,
	}
}

// CreateSessionToken creates an HMAC-signed session token for the given username.
func CreateSessionToken(username string, cfg SessionConfig) string {
	expiry := time.Now().Add(cfg.MaxAge).Unix()
	payload := fmt.Sprintf("%s|%d", username, expiry)
	mac := hmac.New(sha256.New, cfg.Secret)
	mac.Write([]byte(payload))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	token := fmt.Sprintf("%s|%s", payload, sig)
	return base64.RawURLEncoding.EncodeToString([]byte(token))
}

// ValidateSessionToken validates a session token and returns the username if valid.
func ValidateSessionToken(token string, cfg SessionConfig) (string, bool) {
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", false
	}

	parts := strings.SplitN(string(decoded), "|", 3)
	if len(parts) != 3 {
		return "", false
	}

	username, expiryStr, sigStr := parts[0], parts[1], parts[2]

	expiry, err := strconv.ParseInt(expiryStr, 10, 64)
	if err != nil {
		return "", false
	}
	if time.Now().Unix() > expiry {
		return "", false
	}

	payload := fmt.Sprintf("%s|%s", username, expiryStr)
	mac := hmac.New(sha256.New, cfg.Secret)
	mac.Write([]byte(payload))
	expectedSig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(sigStr), []byte(expectedSig)) {
		return "", false
	}

	return username, true
}

// SetSessionCookie sets a session cookie on the response.
func SetSessionCookie(w http.ResponseWriter, username string, cfg SessionConfig) {
	token := CreateSessionToken(username, cfg)
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		MaxAge:   int(cfg.MaxAge.Seconds()),
		HttpOnly: true,
		Secure:   cfg.Secure,
		SameSite: http.SameSiteStrictMode,
	})
}

// ClearSessionCookie removes the session cookie.
func ClearSessionCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
	})
}

// Config holds authentication configuration.
type Config struct {
	Enabled      bool
	Username     string
	PasswordHash []byte
	APIKeys      []string
	PublicPaths  []string
	Session      SessionConfig
}

// HashPassword creates a bcrypt hash of the given password.
func HashPassword(password string) ([]byte, error) {
	return bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
}

// Middleware returns an HTTP middleware that enforces authentication.
func Middleware(cfg Config) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !cfg.Enabled {
				next.ServeHTTP(w, r)
				return
			}

			for _, path := range cfg.PublicPaths {
				if r.URL.Path == path {
					next.ServeHTTP(w, r)
					return
				}
			}

			if apiKey := r.Header.Get("X-API-Key"); apiKey != "" {
				for _, validKey := range cfg.APIKeys {
					if subtle.ConstantTimeCompare([]byte(apiKey), []byte(validKey)) == 1 {
						next.ServeHTTP(w, r)
						return
					}
				}
			}

			// Check session cookie
			if cookie, err := r.Cookie(sessionCookieName); err == nil {
				if _, valid := ValidateSessionToken(cookie.Value, cfg.Session); valid {
					next.ServeHTTP(w, r)
					return
				}
			}

			username, password, ok := r.BasicAuth()
			if ok && CheckCredentials(username, password, cfg) {
				next.ServeHTTP(w, r)
				return
			}

			// Browser requests get redirected to login page
			if isBrowserRequest(r) {
				http.Redirect(w, r, "/login", http.StatusSeeOther)
				return
			}

			w.Header().Set("WWW-Authenticate", `Basic realm="CockroachDB Cluster History"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		})
	}
}

// isBrowserRequest returns true if the request appears to come from a browser.
func isBrowserRequest(r *http.Request) bool {
	accept := r.Header.Get("Accept")
	return strings.Contains(accept, "text/html")
}

// CheckCredentials validates username and password against the config.
func CheckCredentials(username, password string, cfg Config) bool {
	usernameMatch := subtle.ConstantTimeCompare([]byte(username), []byte(cfg.Username)) == 1
	passwordMatch := bcrypt.CompareHashAndPassword(cfg.PasswordHash, []byte(password)) == nil
	return usernameMatch && passwordMatch
}

// parseCommaSeparated parses a comma-separated string into a slice.
// If the input is empty, it returns the provided default value.
func parseCommaSeparated(s string, defaultValue []string) []string {
	if s == "" {
		return defaultValue
	}
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}

// ParseAPIKeys parses a comma-separated list of API keys.
func ParseAPIKeys(keys string) []string {
	return parseCommaSeparated(keys, nil)
}

// ParsePublicPaths parses a comma-separated list of public paths.
func ParsePublicPaths(paths string) []string {
	return parseCommaSeparated(paths, []string{"/health"})
}
