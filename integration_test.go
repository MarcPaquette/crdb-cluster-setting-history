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

	// Step 3: Run collector once
	t.Log("Step 3: Running collector...")
	coll, err := collector.New(ctx, adminURL, store, time.Hour) // Interval doesn't matter, we call collect directly
	if err != nil {
		t.Fatalf("Failed to create collector: %v", err)
	}
	defer coll.Close()

	// We need to trigger a collection - let's start and immediately cancel
	collCtx, collCancel := context.WithTimeout(ctx, 5*time.Second)
	go coll.Start(collCtx)
	time.Sleep(2 * time.Second) // Wait for first collection
	collCancel()

	// Step 4: Verify data was stored
	t.Log("Step 4: Verifying stored data...")
	changes, err := store.GetChanges(ctx, 10)
	if err != nil {
		t.Fatalf("Failed to get changes: %v", err)
	}

	// First run won't have changes (nothing to compare to)
	t.Logf("Found %d changes after first collection (expected 0)", len(changes))

	// Verify we can get the latest snapshot
	snapshot, err := store.GetLatestSnapshot(ctx)
	if err != nil {
		t.Fatalf("Failed to get latest snapshot: %v", err)
	}

	if snapshot == nil || len(snapshot) == 0 {
		t.Fatal("Expected snapshot to have settings")
	}

	t.Logf("Snapshot contains %d settings", len(snapshot))

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
