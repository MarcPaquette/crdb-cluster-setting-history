package collector

import (
	"context"
	"os"
	"testing"
	"time"

	"crdb-cluster-history/config"
	"crdb-cluster-history/storage"
)

func getTestDBs(t *testing.T) (string, string) {
	sourceURL := os.Getenv("DATABASE_URL")
	if sourceURL == "" {
		t.Skip("DATABASE_URL not set")
	}
	historyURL := os.Getenv("HISTORY_DATABASE_URL")
	if historyURL == "" {
		t.Skip("HISTORY_DATABASE_URL not set")
	}
	return sourceURL, historyURL
}

func TestNewManager(t *testing.T) {
	sourceURL, historyURL := getTestDBs(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := storage.New(ctx, historyURL)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	cfg := &config.Config{
		HistoryDatabaseURL: historyURL,
		PollInterval:       config.Duration(1 * time.Hour),
		Clusters: []config.ClusterConfig{
			{Name: "Test1", ID: "test1", DatabaseURL: sourceURL},
			{Name: "Test2", ID: "test2", DatabaseURL: sourceURL},
		},
	}

	manager, err := NewManager(ctx, cfg, store)
	if err != nil {
		t.Fatalf("NewManager() failed: %v", err)
	}
	defer manager.Close()

	// Verify collectors were created
	ids := manager.ClusterIDs()
	if len(ids) != 2 {
		t.Errorf("ClusterIDs() = %d, want 2", len(ids))
	}

	// Verify GetCollector works
	coll, ok := manager.GetCollector("test1")
	if !ok {
		t.Error("GetCollector(test1) should find collector")
	}
	if coll == nil {
		t.Error("GetCollector(test1) returned nil collector")
	}

	_, ok = manager.GetCollector("nonexistent")
	if ok {
		t.Error("GetCollector(nonexistent) should not find collector")
	}
}

func TestNewManagerInvalidURL(t *testing.T) {
	_, historyURL := getTestDBs(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := storage.New(ctx, historyURL)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	cfg := &config.Config{
		HistoryDatabaseURL: historyURL,
		PollInterval:       config.Duration(1 * time.Hour),
		Clusters: []config.ClusterConfig{
			{Name: "Bad", ID: "bad", DatabaseURL: "postgresql://invalid:9999/db"},
		},
	}

	_, err = NewManager(ctx, cfg, store)
	if err == nil {
		t.Error("NewManager() should fail with invalid database URL")
	}
}

func TestManagerCollect(t *testing.T) {
	sourceURL, historyURL := getTestDBs(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := storage.New(ctx, historyURL)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	cfg := &config.Config{
		HistoryDatabaseURL: historyURL,
		PollInterval:       config.Duration(1 * time.Hour),
		Clusters: []config.ClusterConfig{
			{Name: "Test", ID: "manager-test", DatabaseURL: sourceURL},
		},
	}

	manager, err := NewManager(ctx, cfg, store)
	if err != nil {
		t.Fatalf("NewManager() failed: %v", err)
	}
	defer manager.Close()

	// Trigger manual collection
	err = manager.Collect(ctx)
	if err != nil {
		t.Errorf("Collect() failed: %v", err)
	}

	// Verify data was collected
	snapshot, err := store.GetLatestSnapshot(ctx, "manager-test")
	if err != nil {
		t.Fatalf("GetLatestSnapshot() failed: %v", err)
	}
	if len(snapshot) == 0 {
		t.Error("Expected snapshot to have settings after Collect()")
	}
}

func TestManagerClusterIDs(t *testing.T) {
	sourceURL, historyURL := getTestDBs(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := storage.New(ctx, historyURL)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	cfg := &config.Config{
		HistoryDatabaseURL: historyURL,
		PollInterval:       config.Duration(1 * time.Hour),
		Clusters: []config.ClusterConfig{
			{Name: "Alpha", ID: "alpha", DatabaseURL: sourceURL},
			{Name: "Beta", ID: "beta", DatabaseURL: sourceURL},
			{Name: "Gamma", ID: "gamma", DatabaseURL: sourceURL},
		},
	}

	manager, err := NewManager(ctx, cfg, store)
	if err != nil {
		t.Fatalf("NewManager() failed: %v", err)
	}
	defer manager.Close()

	ids := manager.ClusterIDs()
	if len(ids) != 3 {
		t.Errorf("ClusterIDs() = %d, want 3", len(ids))
	}

	// Check all IDs are present (order may vary)
	idMap := make(map[string]bool)
	for _, id := range ids {
		idMap[id] = true
	}
	for _, expected := range []string{"alpha", "beta", "gamma"} {
		if !idMap[expected] {
			t.Errorf("ClusterIDs() missing %q", expected)
		}
	}
}
