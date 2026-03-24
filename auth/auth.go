package auth

import (
	"crypto/subtle"
	"net/http"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

// Config holds authentication configuration.
type Config struct {
	Enabled      bool
	Username     string
	PasswordHash []byte
	APIKeys      []string
	PublicPaths  []string
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

			username, password, ok := r.BasicAuth()
			if ok && checkCredentials(username, password, cfg) {
				next.ServeHTTP(w, r)
				return
			}

			w.Header().Set("WWW-Authenticate", `Basic realm="CockroachDB Cluster History"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		})
	}
}

func checkCredentials(username, password string, cfg Config) bool {
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
