package auth

import (
	"crypto/subtle"
	"net/http"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

// Config holds authentication configuration.
type Config struct {
	// Enabled controls whether authentication is required.
	Enabled bool

	// Username for HTTP Basic Auth.
	Username string

	// PasswordHash is the bcrypt hash of the password.
	// If empty and Password is set, it will be hashed at startup.
	PasswordHash []byte

	// APIKeys is a list of valid API keys for X-API-Key header auth.
	APIKeys []string

	// PublicPaths are paths that don't require authentication (e.g., /health).
	PublicPaths []string
}

// HashPassword creates a bcrypt hash of the given password.
func HashPassword(password string) ([]byte, error) {
	return bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
}

// Middleware returns an HTTP middleware that enforces authentication.
func Middleware(cfg Config) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// If auth is disabled, pass through
			if !cfg.Enabled {
				next.ServeHTTP(w, r)
				return
			}

			// Check if path is public
			for _, path := range cfg.PublicPaths {
				if r.URL.Path == path {
					next.ServeHTTP(w, r)
					return
				}
			}

			// Check API key header first
			if apiKey := r.Header.Get("X-API-Key"); apiKey != "" {
				for _, validKey := range cfg.APIKeys {
					if subtle.ConstantTimeCompare([]byte(apiKey), []byte(validKey)) == 1 {
						next.ServeHTTP(w, r)
						return
					}
				}
			}

			// Check HTTP Basic Auth
			username, password, ok := r.BasicAuth()
			if ok && checkCredentials(username, password, cfg) {
				next.ServeHTTP(w, r)
				return
			}

			// Authentication failed
			w.Header().Set("WWW-Authenticate", `Basic realm="CockroachDB Cluster History"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		})
	}
}

// checkCredentials validates username and password against the config.
func checkCredentials(username, password string, cfg Config) bool {
	// Check username with constant-time comparison
	usernameMatch := subtle.ConstantTimeCompare([]byte(username), []byte(cfg.Username)) == 1

	// Check password against bcrypt hash
	passwordMatch := bcrypt.CompareHashAndPassword(cfg.PasswordHash, []byte(password)) == nil

	return usernameMatch && passwordMatch
}

// ParseAPIKeys parses a comma-separated list of API keys.
func ParseAPIKeys(keys string) []string {
	if keys == "" {
		return nil
	}
	parts := strings.Split(keys, ",")
	result := make([]string, 0, len(parts))
	for _, key := range parts {
		key = strings.TrimSpace(key)
		if key != "" {
			result = append(result, key)
		}
	}
	return result
}

// ParsePublicPaths parses a comma-separated list of public paths.
func ParsePublicPaths(paths string) []string {
	if paths == "" {
		return []string{"/health"}
	}
	parts := strings.Split(paths, ",")
	result := make([]string, 0, len(parts))
	for _, path := range parts {
		path = strings.TrimSpace(path)
		if path != "" {
			result = append(result, path)
		}
	}
	return result
}
