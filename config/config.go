// Package config provides configuration loading and validation for multi-cluster monitoring.
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// ClusterConfig defines a single cluster to monitor.
type ClusterConfig struct {
	Name        string `yaml:"name"`         // Display name (e.g., "Production", "Staging")
	ID          string `yaml:"id"`           // Unique identifier (slug, e.g., "prod", "staging")
	DatabaseURL string `yaml:"database_url"` // Connection string to monitored cluster
}

// Config is the root configuration structure.
type Config struct {
	HistoryDatabaseURL string          `yaml:"history_database_url"`
	Clusters           []ClusterConfig `yaml:"clusters"`
	PollInterval       Duration        `yaml:"poll_interval"`
	Retention          Duration        `yaml:"retention"`
	HTTPPort           string          `yaml:"http_port"`
}

// Duration is a wrapper around time.Duration that supports YAML unmarshaling.
type Duration time.Duration

// UnmarshalYAML implements yaml.Unmarshaler for Duration.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	if s == "" {
		*d = 0
		return nil
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// Duration returns the time.Duration value.
func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}

// Load reads configuration from a YAML file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Apply defaults
	if cfg.HTTPPort == "" {
		cfg.HTTPPort = "8080"
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = Duration(15 * time.Minute)
	}

	return &cfg, nil
}

// LoadFromEnv creates a configuration from environment variables.
// This provides backward compatibility with single-cluster deployments.
func LoadFromEnv() (*Config, error) {
	sourceURL := os.Getenv("DATABASE_URL")
	historyURL := os.Getenv("HISTORY_DATABASE_URL")

	if sourceURL == "" {
		return nil, errors.New("DATABASE_URL environment variable is required")
	}
	if historyURL == "" {
		return nil, errors.New("HISTORY_DATABASE_URL environment variable is required")
	}

	cfg := &Config{
		HistoryDatabaseURL: historyURL,
		Clusters: []ClusterConfig{{
			Name:        "Default",
			ID:          "default",
			DatabaseURL: sourceURL,
		}},
		PollInterval: Duration(parseDurationEnv("POLL_INTERVAL", 15*time.Minute)),
		Retention:    Duration(parseDurationEnv("RETENTION", 0)),
		HTTPPort:     getEnvDefault("HTTP_PORT", "8080"),
	}

	return cfg, nil
}

// LoadAuto tries to load configuration from a file, falling back to environment variables.
// It checks for CLUSTERS_CONFIG env var, then clusters.yaml, then falls back to env vars.
func LoadAuto() (*Config, error) {
	// Check for explicit config file path
	configPath := os.Getenv("CLUSTERS_CONFIG")
	if configPath != "" {
		return Load(configPath)
	}

	// Check for default config file
	if _, err := os.Stat("clusters.yaml"); err == nil {
		return Load("clusters.yaml")
	}

	// Fall back to environment variables
	return LoadFromEnv()
}

// Validate checks the configuration for errors.
func (c *Config) Validate() error {
	if c.HistoryDatabaseURL == "" {
		return errors.New("history_database_url is required")
	}

	if len(c.Clusters) == 0 {
		return errors.New("at least one cluster must be configured")
	}

	// Check for duplicate IDs
	seenIDs := make(map[string]bool)
	for i, cluster := range c.Clusters {
		if cluster.ID == "" {
			return fmt.Errorf("cluster[%d]: id is required", i)
		}
		if cluster.Name == "" {
			return fmt.Errorf("cluster[%d]: name is required", i)
		}
		if cluster.DatabaseURL == "" {
			return fmt.Errorf("cluster[%d] (%s): database_url is required", i, cluster.ID)
		}

		// Validate ID format (alphanumeric, hyphens, underscores)
		if !isValidID(cluster.ID) {
			return fmt.Errorf("cluster[%d]: id %q contains invalid characters (use only alphanumeric, hyphens, underscores)", i, cluster.ID)
		}

		if seenIDs[cluster.ID] {
			return fmt.Errorf("duplicate cluster id: %s", cluster.ID)
		}
		seenIDs[cluster.ID] = true
	}

	if c.PollInterval.Duration() < time.Second {
		return errors.New("poll_interval must be at least 1 second")
	}

	return nil
}

// GetCluster returns a cluster configuration by ID.
func (c *Config) GetCluster(id string) (*ClusterConfig, bool) {
	for i := range c.Clusters {
		if c.Clusters[i].ID == id {
			return &c.Clusters[i], true
		}
	}
	return nil, false
}

// ClusterIDs returns a list of all cluster IDs.
func (c *Config) ClusterIDs() []string {
	ids := make([]string, len(c.Clusters))
	for i, cluster := range c.Clusters {
		ids[i] = cluster.ID
	}
	return ids
}

// isValidID checks if a string is a valid cluster ID.
func isValidID(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_') {
			return false
		}
	}
	return true
}

// getEnvDefault returns an environment variable value or a default.
func getEnvDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

// parseDurationEnv parses a duration from an environment variable.
func parseDurationEnv(key string, defaultValue time.Duration) time.Duration {
	s := os.Getenv(key)
	if s == "" {
		return defaultValue
	}
	s = strings.TrimSpace(s)
	d, err := time.ParseDuration(s)
	if err != nil {
		return defaultValue
	}
	return d
}
