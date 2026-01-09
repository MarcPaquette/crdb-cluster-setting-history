package collector

import (
	"context"
	"os"
	"testing"
	"time"

	"crdb-cluster-history/storage"

	"github.com/jackc/pgx/v5"
)

// testClusterID is used for all tests
const testClusterID = "test-cluster"

func TestShowClusterSettingsColumns(t *testing.T) {
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		t.Skip("DATABASE_URL not set, skipping integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, "SHOW CLUSTER SETTINGS")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	// Check column names
	fieldDescs := rows.FieldDescriptions()
	t.Logf("SHOW CLUSTER SETTINGS returns %d columns:", len(fieldDescs))
	for i, fd := range fieldDescs {
		t.Logf("  [%d] %s", i, fd.Name)
	}

	expectedCols := []string{"variable", "value", "setting_type", "description", "default_value", "origin"}
	if len(fieldDescs) != len(expectedCols) {
		t.Errorf("Expected %d columns, got %d", len(expectedCols), len(fieldDescs))
	}

	for i, expected := range expectedCols {
		if i < len(fieldDescs) && string(fieldDescs[i].Name) != expected {
			t.Errorf("Column %d: expected %q, got %q", i, expected, fieldDescs[i].Name)
		}
	}

	// Test scanning a row
	if rows.Next() {
		var variable, value, settingType, description, defaultValue, origin string
		err := rows.Scan(&variable, &value, &settingType, &description, &defaultValue, &origin)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		t.Logf("Sample row: variable=%s, value=%s, type=%s", variable, value, settingType)
	}
}

func getTestURLs(t *testing.T) (string, string) {
	sourceURL := os.Getenv("DATABASE_URL")
	historyURL := os.Getenv("HISTORY_DATABASE_URL")
	if sourceURL == "" || historyURL == "" {
		t.Skip("DATABASE_URL and HISTORY_DATABASE_URL must be set")
	}
	return sourceURL, historyURL
}

func TestNew(t *testing.T) {
	sourceURL, historyURL := getTestURLs(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := storage.New(ctx, historyURL)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	coll, err := New(ctx, testClusterID, sourceURL, store, 15*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create collector: %v", err)
	}
	defer coll.Close()

	if coll == nil {
		t.Fatal("Expected non-nil collector")
	}
	if coll.interval != 15*time.Minute {
		t.Error("Interval not set correctly")
	}
}

func TestCollect(t *testing.T) {
	sourceURL, historyURL := getTestURLs(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := storage.New(ctx, historyURL)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	coll, err := New(ctx, testClusterID, sourceURL, store, 15*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create collector: %v", err)
	}
	defer coll.Close()

	// Call collect directly
	err = coll.collect(ctx)
	if err != nil {
		t.Fatalf("collect() failed: %v", err)
	}

	// Verify data was stored
	snapshot, err := store.GetLatestSnapshot(ctx, testClusterID)
	if err != nil {
		t.Fatalf("Failed to get snapshot: %v", err)
	}

	if len(snapshot) == 0 {
		t.Error("Expected snapshot to have settings after collect()")
	}

	t.Logf("Collected %d settings", len(snapshot))
}

func TestStart(t *testing.T) {
	sourceURL, historyURL := getTestURLs(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store, err := storage.New(ctx, historyURL)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Use a short interval
	coll, err := New(ctx, testClusterID, sourceURL, store, 1*time.Second)
	if err != nil {
		t.Fatalf("Failed to create collector: %v", err)
	}
	defer coll.Close()

	// Start in a goroutine
	done := make(chan struct{})
	go func() {
		coll.Start(ctx)
		close(done)
	}()

	// Wait for context to timeout
	<-done

	// Verify data was collected
	snapshot, err := store.GetLatestSnapshot(ctx, testClusterID)
	if err != nil && ctx.Err() == nil {
		t.Fatalf("Failed to get snapshot: %v", err)
	}

	if len(snapshot) > 0 {
		t.Logf("Start() collected %d settings", len(snapshot))
	}
}

func TestNewWithInvalidURL(t *testing.T) {
	_, historyURL := getTestURLs(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := storage.New(ctx, historyURL)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Use invalid source URL - should fail at pool creation
	_, err = New(ctx, testClusterID, "postgresql://invalid:5432/db?connect_timeout=1", store, 15*time.Minute)
	if err == nil {
		t.Error("Expected error with invalid URL")
	}
}

func TestWithRetention(t *testing.T) {
	sourceURL, historyURL := getTestURLs(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := storage.New(ctx, historyURL)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	coll, err := New(ctx, testClusterID, sourceURL, store, 15*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create collector: %v", err)
	}
	defer coll.Close()

	// Test chaining
	result := coll.WithRetention(24 * time.Hour)
	if result != coll {
		t.Error("WithRetention should return the same collector for chaining")
	}

	if coll.retention != 24*time.Hour {
		t.Errorf("Expected retention 24h, got %v", coll.retention)
	}
}

func TestCollectAndCleanup(t *testing.T) {
	sourceURL, historyURL := getTestURLs(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := storage.New(ctx, historyURL)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	coll, err := New(ctx, testClusterID, sourceURL, store, 15*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create collector: %v", err)
	}
	defer coll.Close()

	// Set a very short retention to trigger cleanup
	coll.WithRetention(1 * time.Nanosecond)

	// Run collectAndCleanup - this exercises both collect and cleanup paths
	coll.collectAndCleanup(ctx)

	// Verify collection happened (cleanup may have removed old data)
	snapshot, err := store.GetLatestSnapshot(ctx, testClusterID)
	if err != nil {
		t.Fatalf("Failed to get snapshot: %v", err)
	}

	// May or may not have data depending on timing, but shouldn't error
	t.Logf("After collectAndCleanup: %d settings in snapshot", len(snapshot))
}

func TestCleanupWithRetention(t *testing.T) {
	sourceURL, historyURL := getTestURLs(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := storage.New(ctx, historyURL)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	coll, err := New(ctx, testClusterID, sourceURL, store, 15*time.Minute)
	if err != nil {
		t.Fatalf("Failed to create collector: %v", err)
	}
	defer coll.Close()

	// First collect some data
	err = coll.collect(ctx)
	if err != nil {
		t.Fatalf("collect() failed: %v", err)
	}

	// Set retention and run cleanup
	coll.WithRetention(1 * time.Nanosecond)
	err = coll.cleanup(ctx)
	if err != nil {
		t.Fatalf("cleanup() failed: %v", err)
	}

	// Cleanup should have run without error
	t.Log("Cleanup completed successfully")
}
