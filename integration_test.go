package main

import (
	"context"
	"os"
	"testing"
	"time"

	"crdb-cluster-history/cmd"
	"crdb-cluster-history/collector"
	"crdb-cluster-history/storage"
)

// testClusterID is used for all tests
const testClusterID = "test-cluster"

func TestFullIntegration(t *testing.T) {
	adminURL := os.Getenv("DATABASE_URL")
	if adminURL == "" {
		t.Skip("DATABASE_URL not set, skipping integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Use a unique database name for testing
	dbName := "cluster_history_test"
	username := "history_test_user"

	// Step 1: Initialize database and user
	t.Log("Step 1: Initializing database and user...")
	initCfg := cmd.InitConfig{
		AdminURL:     adminURL,
		DatabaseName: dbName,
		Username:     username,
		Password:     "", // Insecure mode
	}

	if err := cmd.RunInit(ctx, initCfg); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// Step 2: Connect to the history database
	t.Log("Step 2: Connecting to history database...")
	historyURL := "postgresql://" + username + "@localhost:26257/" + dbName + "?sslmode=disable"
	store, err := storage.New(ctx, historyURL)
	if err != nil {
		t.Fatalf("Failed to connect to history database: %v", err)
	}
	defer store.Close()

	// Step 3: Run collector once (directly, without timing dependency)
	t.Log("Step 3: Running collector...")
	coll, err := collector.New(ctx, testClusterID, adminURL, store, time.Hour)
	if err != nil {
		t.Fatalf("Failed to create collector: %v", err)
	}
	defer coll.Close()

	// Call Collect directly instead of relying on Start with sleep
	if err := coll.Collect(ctx); err != nil {
		t.Fatalf("First collection failed: %v", err)
	}

	// Step 4: Verify data was stored
	t.Log("Step 4: Verifying stored data...")

	// Verify we can get the latest snapshot
	snapshot, err := store.GetLatestSnapshot(ctx, testClusterID)
	if err != nil {
		t.Fatalf("Failed to get latest snapshot: %v", err)
	}
	if len(snapshot) == 0 {
		t.Fatal("Expected snapshot to have settings after collection")
	}
	t.Logf("First snapshot contains %d settings", len(snapshot))

	// Step 5: Run a second collection to generate changes
	t.Log("Step 5: Running second collection to verify change detection...")
	if err := coll.Collect(ctx); err != nil {
		t.Fatalf("Second collection failed: %v", err)
	}

	// Get changes - there may or may not be changes depending on cluster state
	changes, err := store.GetChanges(ctx, testClusterID, 10)
	if err != nil {
		t.Fatalf("Failed to get changes: %v", err)
	}
	t.Logf("Found %d changes after two collections", len(changes))

	// Sample some settings
	count := 0
	for variable, setting := range snapshot {
		if count >= 3 {
			break
		}
		t.Logf("  %s = %s", variable, setting.Value)
		count++
	}

	t.Log("Integration test passed!")
}
