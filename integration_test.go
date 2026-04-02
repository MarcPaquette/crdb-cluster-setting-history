package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"crdb-cluster-history/cmd"
	"crdb-cluster-history/collector"
	"crdb-cluster-history/storage"

	"github.com/jackc/pgx/v5"
)

func TestFullIntegration(t *testing.T) {
	testClusterID := fmt.Sprintf("integ-%d", time.Now().UnixNano())
	adminURL := os.Getenv("DATABASE_URL")
	if adminURL == "" {
		t.Skip("DATABASE_URL not set, skipping integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dbName := "cluster_history_test"
	username := "history_test_user"

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()

		conn, err := pgx.Connect(cleanupCtx, adminURL)
		if err != nil {
			t.Logf("Cleanup: failed to connect for cleanup: %v", err)
			return
		}
		defer conn.Close(cleanupCtx)

		_, err = conn.Exec(cleanupCtx, "DROP DATABASE IF EXISTS "+pgx.Identifier{dbName}.Sanitize()+" CASCADE")
		if err != nil {
			t.Logf("Cleanup: failed to drop database: %v", err)
		}

		conn.Exec(cleanupCtx, "ALTER DEFAULT PRIVILEGES FOR ROLE root REVOKE ALL ON TABLES FROM "+pgx.Identifier{username}.Sanitize())
		_, err = conn.Exec(cleanupCtx, "DROP USER IF EXISTS "+pgx.Identifier{username}.Sanitize())
		if err != nil {
			t.Logf("Cleanup: failed to drop user: %v", err)
		}

		t.Log("Cleanup: test database and user removed")
	})

	t.Log("Step 1: Initializing database and user...")
	initCfg := cmd.InitConfig{
		AdminURL:     adminURL,
		DatabaseName: dbName,
		Username:     username,
		Password:     "",
	}

	if err := cmd.RunInit(ctx, initCfg); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	t.Log("Step 2: Connecting to history database...")
	historyURL := "postgresql://" + username + "@localhost:26257/" + dbName + "?sslmode=disable"
	store, err := storage.New(ctx, historyURL)
	if err != nil {
		t.Fatalf("Failed to connect to history database: %v", err)
	}
	defer store.Close()

	t.Log("Step 3: Running collector...")
	coll, err := collector.New(ctx, testClusterID, adminURL, store, time.Hour)
	if err != nil {
		t.Fatalf("Failed to create collector: %v", err)
	}
	defer coll.Close()

	if err := coll.Collect(ctx); err != nil {
		t.Fatalf("First collection failed: %v", err)
	}

	t.Log("Step 4: Verifying stored data...")
	snapshot, err := store.GetLatestSnapshot(ctx, testClusterID)
	if err != nil {
		t.Fatalf("Failed to get latest snapshot: %v", err)
	}
	if len(snapshot) == 0 {
		t.Fatal("Expected snapshot to have settings after collection")
	}
	t.Logf("First snapshot contains %d settings", len(snapshot))

	t.Log("Step 5: Running second collection...")
	if err := coll.Collect(ctx); err != nil {
		t.Fatalf("Second collection failed: %v", err)
	}

	changes, err := store.GetChanges(ctx, testClusterID, 10)
	if err != nil {
		t.Fatalf("Failed to get changes: %v", err)
	}
	t.Logf("Found %d changes after two collections", len(changes))

	count := 0
	for variable, setting := range snapshot {
		if count >= 3 {
			break
		}
		t.Logf("  %s = %s", variable, setting.Value)
		count++
	}
}
