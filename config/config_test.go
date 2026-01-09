package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoad(t *testing.T) {
	// Create a temporary config file
	content := `
history_database_url: "postgresql://history@localhost:26257/history?sslmode=disable"
poll_interval: 5m
retention: 720h
http_port: "9090"

clusters:
  - name: "Production"
    id: "prod"
    database_url: "postgresql://readonly@prod:26257/defaultdb?sslmode=require"
  - name: "Staging"
    id: "staging"
    database_url: "postgresql://readonly@staging:26257/defaultdb?sslmode=disable"
`
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "clusters.yaml")
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write temp config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.HistoryDatabaseURL != "postgresql://history@localhost:26257/history?sslmode=disable" {
		t.Errorf("HistoryDatabaseURL = %q, want postgresql://history@localhost:26257/history?sslmode=disable", cfg.HistoryDatabaseURL)
	}
	if cfg.PollInterval.Duration() != 5*time.Minute {
		t.Errorf("PollInterval = %v, want 5m", cfg.PollInterval.Duration())
	}
	if cfg.Retention.Duration() != 720*time.Hour {
		t.Errorf("Retention = %v, want 720h", cfg.Retention.Duration())
	}
	if cfg.HTTPPort != "9090" {
		t.Errorf("HTTPPort = %q, want 9090", cfg.HTTPPort)
	}
	if len(cfg.Clusters) != 2 {
		t.Fatalf("len(Clusters) = %d, want 2", len(cfg.Clusters))
	}
	if cfg.Clusters[0].Name != "Production" {
		t.Errorf("Clusters[0].Name = %q, want Production", cfg.Clusters[0].Name)
	}
	if cfg.Clusters[0].ID != "prod" {
		t.Errorf("Clusters[0].ID = %q, want prod", cfg.Clusters[0].ID)
	}
	if cfg.Clusters[1].ID != "staging" {
		t.Errorf("Clusters[1].ID = %q, want staging", cfg.Clusters[1].ID)
	}
}

func TestLoadDefaults(t *testing.T) {
	// Config with minimal settings
	content := `
history_database_url: "postgresql://localhost/history"
clusters:
  - name: "Test"
    id: "test"
    database_url: "postgresql://localhost/test"
`
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "clusters.yaml")
	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write temp config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// Check defaults are applied
	if cfg.HTTPPort != "8080" {
		t.Errorf("HTTPPort = %q, want default 8080", cfg.HTTPPort)
	}
	if cfg.PollInterval.Duration() != 15*time.Minute {
		t.Errorf("PollInterval = %v, want default 15m", cfg.PollInterval.Duration())
	}
}

func TestLoadFromEnv(t *testing.T) {
	// Set environment variables
	os.Setenv("DATABASE_URL", "postgresql://root@localhost:26257/defaultdb")
	os.Setenv("HISTORY_DATABASE_URL", "postgresql://history@localhost:26257/history")
	os.Setenv("POLL_INTERVAL", "10m")
	os.Setenv("HTTP_PORT", "8888")
	defer func() {
		os.Unsetenv("DATABASE_URL")
		os.Unsetenv("HISTORY_DATABASE_URL")
		os.Unsetenv("POLL_INTERVAL")
		os.Unsetenv("HTTP_PORT")
	}()

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() failed: %v", err)
	}

	if cfg.HistoryDatabaseURL != "postgresql://history@localhost:26257/history" {
		t.Errorf("HistoryDatabaseURL = %q, want postgresql://history@localhost:26257/history", cfg.HistoryDatabaseURL)
	}
	if len(cfg.Clusters) != 1 {
		t.Fatalf("len(Clusters) = %d, want 1", len(cfg.Clusters))
	}
	if cfg.Clusters[0].ID != "default" {
		t.Errorf("Clusters[0].ID = %q, want default", cfg.Clusters[0].ID)
	}
	if cfg.Clusters[0].DatabaseURL != "postgresql://root@localhost:26257/defaultdb" {
		t.Errorf("Clusters[0].DatabaseURL = %q, want postgresql://root@localhost:26257/defaultdb", cfg.Clusters[0].DatabaseURL)
	}
	if cfg.PollInterval.Duration() != 10*time.Minute {
		t.Errorf("PollInterval = %v, want 10m", cfg.PollInterval.Duration())
	}
	if cfg.HTTPPort != "8888" {
		t.Errorf("HTTPPort = %q, want 8888", cfg.HTTPPort)
	}
}

func TestLoadFromEnvMissingVars(t *testing.T) {
	os.Unsetenv("DATABASE_URL")
	os.Unsetenv("HISTORY_DATABASE_URL")

	_, err := LoadFromEnv()
	if err == nil {
		t.Error("LoadFromEnv() should fail when DATABASE_URL is missing")
	}

	os.Setenv("DATABASE_URL", "postgresql://localhost/test")
	defer os.Unsetenv("DATABASE_URL")

	_, err = LoadFromEnv()
	if err == nil {
		t.Error("LoadFromEnv() should fail when HISTORY_DATABASE_URL is missing")
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			config: Config{
				HistoryDatabaseURL: "postgresql://localhost/history",
				Clusters: []ClusterConfig{
					{Name: "Test", ID: "test", DatabaseURL: "postgresql://localhost/test"},
				},
				PollInterval: Duration(5 * time.Minute),
			},
			wantErr: false,
		},
		{
			name: "missing history url",
			config: Config{
				Clusters: []ClusterConfig{
					{Name: "Test", ID: "test", DatabaseURL: "postgresql://localhost/test"},
				},
				PollInterval: Duration(5 * time.Minute),
			},
			wantErr: true,
			errMsg:  "history_database_url is required",
		},
		{
			name: "no clusters",
			config: Config{
				HistoryDatabaseURL: "postgresql://localhost/history",
				Clusters:           []ClusterConfig{},
				PollInterval:       Duration(5 * time.Minute),
			},
			wantErr: true,
			errMsg:  "at least one cluster",
		},
		{
			name: "missing cluster id",
			config: Config{
				HistoryDatabaseURL: "postgresql://localhost/history",
				Clusters: []ClusterConfig{
					{Name: "Test", ID: "", DatabaseURL: "postgresql://localhost/test"},
				},
				PollInterval: Duration(5 * time.Minute),
			},
			wantErr: true,
			errMsg:  "id is required",
		},
		{
			name: "missing cluster name",
			config: Config{
				HistoryDatabaseURL: "postgresql://localhost/history",
				Clusters: []ClusterConfig{
					{Name: "", ID: "test", DatabaseURL: "postgresql://localhost/test"},
				},
				PollInterval: Duration(5 * time.Minute),
			},
			wantErr: true,
			errMsg:  "name is required",
		},
		{
			name: "missing cluster database_url",
			config: Config{
				HistoryDatabaseURL: "postgresql://localhost/history",
				Clusters: []ClusterConfig{
					{Name: "Test", ID: "test", DatabaseURL: ""},
				},
				PollInterval: Duration(5 * time.Minute),
			},
			wantErr: true,
			errMsg:  "database_url is required",
		},
		{
			name: "duplicate cluster ids",
			config: Config{
				HistoryDatabaseURL: "postgresql://localhost/history",
				Clusters: []ClusterConfig{
					{Name: "Test1", ID: "test", DatabaseURL: "postgresql://localhost/test1"},
					{Name: "Test2", ID: "test", DatabaseURL: "postgresql://localhost/test2"},
				},
				PollInterval: Duration(5 * time.Minute),
			},
			wantErr: true,
			errMsg:  "duplicate cluster id",
		},
		{
			name: "invalid cluster id characters",
			config: Config{
				HistoryDatabaseURL: "postgresql://localhost/history",
				Clusters: []ClusterConfig{
					{Name: "Test", ID: "test cluster", DatabaseURL: "postgresql://localhost/test"},
				},
				PollInterval: Duration(5 * time.Minute),
			},
			wantErr: true,
			errMsg:  "invalid characters",
		},
		{
			name: "poll interval too short",
			config: Config{
				HistoryDatabaseURL: "postgresql://localhost/history",
				Clusters: []ClusterConfig{
					{Name: "Test", ID: "test", DatabaseURL: "postgresql://localhost/test"},
				},
				PollInterval: Duration(500 * time.Millisecond),
			},
			wantErr: true,
			errMsg:  "at least 1 second",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("Validate() should have failed")
				} else if tt.errMsg != "" && !contains(err.Error(), tt.errMsg) {
					t.Errorf("Validate() error = %q, want error containing %q", err.Error(), tt.errMsg)
				}
			} else if err != nil {
				t.Errorf("Validate() unexpected error: %v", err)
			}
		})
	}
}

func TestGetCluster(t *testing.T) {
	cfg := &Config{
		Clusters: []ClusterConfig{
			{Name: "Production", ID: "prod", DatabaseURL: "postgresql://prod"},
			{Name: "Staging", ID: "staging", DatabaseURL: "postgresql://staging"},
		},
	}

	// Find existing cluster
	cluster, found := cfg.GetCluster("prod")
	if !found {
		t.Error("GetCluster(prod) should find cluster")
	}
	if cluster.Name != "Production" {
		t.Errorf("GetCluster(prod).Name = %q, want Production", cluster.Name)
	}

	// Find non-existent cluster
	_, found = cfg.GetCluster("nonexistent")
	if found {
		t.Error("GetCluster(nonexistent) should not find cluster")
	}
}

func TestClusterIDs(t *testing.T) {
	cfg := &Config{
		Clusters: []ClusterConfig{
			{Name: "Production", ID: "prod", DatabaseURL: "postgresql://prod"},
			{Name: "Staging", ID: "staging", DatabaseURL: "postgresql://staging"},
		},
	}

	ids := cfg.ClusterIDs()
	if len(ids) != 2 {
		t.Fatalf("len(ClusterIDs()) = %d, want 2", len(ids))
	}
	if ids[0] != "prod" {
		t.Errorf("ClusterIDs()[0] = %q, want prod", ids[0])
	}
	if ids[1] != "staging" {
		t.Errorf("ClusterIDs()[1] = %q, want staging", ids[1])
	}
}

func TestIsValidID(t *testing.T) {
	tests := []struct {
		id    string
		valid bool
	}{
		{"prod", true},
		{"staging", true},
		{"prod-us-east", true},
		{"prod_us_east", true},
		{"Prod123", true},
		{"123", true},
		{"prod us", false},        // space
		{"prod.us", false},        // dot
		{"prod/us", false},        // slash
		{"prod@us", false},        // at sign
		{"", false},               // empty
	}

	for _, tt := range tests {
		t.Run(tt.id, func(t *testing.T) {
			if got := isValidID(tt.id); got != tt.valid {
				t.Errorf("isValidID(%q) = %v, want %v", tt.id, got, tt.valid)
			}
		})
	}
}

func TestDurationUnmarshal(t *testing.T) {
	content := `
poll_interval: 30s
retention: 24h
`
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "test.yaml")

	// Add required fields
	fullContent := `
history_database_url: "postgresql://localhost/history"
clusters:
  - name: "Test"
    id: "test"
    database_url: "postgresql://localhost/test"
` + content

	if err := os.WriteFile(configPath, []byte(fullContent), 0644); err != nil {
		t.Fatalf("Failed to write temp config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.PollInterval.Duration() != 30*time.Second {
		t.Errorf("PollInterval = %v, want 30s", cfg.PollInterval.Duration())
	}
	if cfg.Retention.Duration() != 24*time.Hour {
		t.Errorf("Retention = %v, want 24h", cfg.Retention.Duration())
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsAt(s, substr, 0))
}

func containsAt(s, substr string, start int) bool {
	for i := start; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
