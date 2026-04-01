package collector

import (
	"context"
	"testing"
	"time"

	"crdb-cluster-history/config"
	"crdb-cluster-history/storage"
)

func setupManagerTest(t *testing.T, clusters []config.ClusterConfig) (context.Context, *Manager) {
	t.Helper()
	_, historyURL := getTestURLs(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	store, err := storage.New(ctx, historyURL)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	cfg := &config.Config{
		HistoryDatabaseURL: historyURL,
		PollInterval:       config.Duration(1 * time.Hour),
		Clusters:           clusters,
	}

	manager, err := NewManager(ctx, cfg, store)
	if err != nil {
		t.Fatalf("NewManager() failed: %v", err)
	}
	t.Cleanup(func() { manager.Close() })

	return ctx, manager
}

func TestNewManager(t *testing.T) {
	sourceURL, _ := getTestURLs(t)

	_, manager := setupManagerTest(t, []config.ClusterConfig{
		{Name: "Test1", ID: "test1", DatabaseURL: sourceURL},
		{Name: "Test2", ID: "test2", DatabaseURL: sourceURL},
	})

	ids := manager.ClusterIDs()
	if len(ids) != 2 {
		t.Errorf("ClusterIDs() = %d, want 2", len(ids))
	}

	coll, ok := manager.GetCollector("test1")
	if !ok {
		t.Fatal("GetCollector(test1) should find collector")
	}
	if coll == nil {
		t.Fatal("GetCollector(test1) returned nil collector")
	}

	_, ok = manager.GetCollector("nonexistent")
	if ok {
		t.Error("GetCollector(nonexistent) should not find collector")
	}
}

func TestNewManagerInvalidURL(t *testing.T) {
	_, historyURL := getTestURLs(t)

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
	sourceURL, _ := getTestURLs(t)

	ctx, manager := setupManagerTest(t, []config.ClusterConfig{
		{Name: "Test", ID: "manager-test", DatabaseURL: sourceURL},
	})

	err := manager.Collect(ctx)
	if err != nil {
		t.Fatalf("Collect() failed: %v", err)
	}
}

func TestManagerClusterIDs(t *testing.T) {
	sourceURL, _ := getTestURLs(t)

	_, manager := setupManagerTest(t, []config.ClusterConfig{
		{Name: "Alpha", ID: "alpha", DatabaseURL: sourceURL},
		{Name: "Beta", ID: "beta", DatabaseURL: sourceURL},
		{Name: "Gamma", ID: "gamma", DatabaseURL: sourceURL},
	})

	ids := manager.ClusterIDs()
	if len(ids) != 3 {
		t.Errorf("ClusterIDs() = %d, want 3", len(ids))
	}

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
