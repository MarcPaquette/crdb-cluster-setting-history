package collector

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"crdb-cluster-history/storage"

	"github.com/jackc/pgx/v5"
)

func uniqueClusterID(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("coll-%s-%d", t.Name(), time.Now().UnixNano())
}

func TestExtractShortVersion(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input    string
		expected string
	}{
		{"CockroachDB CCL v25.4.2 (go1.22.0)", "v25.4.2"},
		{"CockroachDB v24.1.0-alpha.1", "v24.1.0"},
		{"v1.0.0", "v1.0.0"},
		{"no version here", "no version here"}, // fallback to full string
		{"", ""},                                // empty stays empty
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			got := extractShortVersion(tt.input)
			if got != tt.expected {
				t.Errorf("extractShortVersion(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

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

func setupCollectorTest(t *testing.T, timeout, interval time.Duration) (context.Context, *storage.Store, *Collector, string) {
	t.Helper()
	sourceURL, historyURL := getTestURLs(t)
	clusterID := uniqueClusterID(t)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)

	store, err := storage.New(ctx, historyURL)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	coll, err := New(ctx, clusterID, sourceURL, store, interval)
	if err != nil {
		t.Fatalf("Failed to create collector: %v", err)
	}
	t.Cleanup(func() { coll.Close() })

	return ctx, store, coll, clusterID
}

func TestNew(t *testing.T) {
	_, _, coll, _ := setupCollectorTest(t, 10*time.Second, 15*time.Minute)

	if coll.interval != 15*time.Minute {
		t.Error("Interval not set correctly")
	}
}

func TestCollect(t *testing.T) {
	ctx, store, coll, clusterID := setupCollectorTest(t, 30*time.Second, 15*time.Minute)

	err := coll.collect(ctx)
	if err != nil {
		t.Fatalf("collect() failed: %v", err)
	}

	snapshot, err := store.GetLatestSnapshot(ctx, clusterID)
	if err != nil {
		t.Fatalf("Failed to get snapshot: %v", err)
	}

	if len(snapshot) == 0 {
		t.Error("Expected snapshot to have settings after collect()")
	}

	t.Logf("Collected %d settings", len(snapshot))
}

func TestStart(t *testing.T) {
	ctx, store, coll, clusterID := setupCollectorTest(t, 5*time.Second, 1*time.Second)

	done := make(chan struct{})
	go func() {
		coll.Start(ctx)
		close(done)
	}()
	<-done

	// Use a fresh context since the original expired after the goroutine finished
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer verifyCancel()

	snapshot, err := store.GetLatestSnapshot(verifyCtx, clusterID)
	if err != nil {
		t.Fatalf("Failed to get snapshot: %v", err)
	}

	if len(snapshot) > 0 {
		t.Logf("Start() collected %d settings", len(snapshot))
	}
}

func TestNewWithInvalidURL(t *testing.T) {
	_, historyURL := getTestURLs(t)
	clusterID := uniqueClusterID(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	store, err := storage.New(ctx, historyURL)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	_, err = New(ctx, clusterID, "postgresql://invalid:5432/db?connect_timeout=1", store, 15*time.Minute)
	if err == nil {
		t.Error("Expected error with invalid URL")
	}
}

func TestWithRetention(t *testing.T) {
	_, _, coll, _ := setupCollectorTest(t, 10*time.Second, 15*time.Minute)

	result := coll.WithRetention(24 * time.Hour)
	if result != coll {
		t.Error("WithRetention should return the same collector for chaining")
	}

	if coll.retention != 24*time.Hour {
		t.Errorf("Expected retention 24h, got %v", coll.retention)
	}
}

func TestCollectAndCleanup(t *testing.T) {
	ctx, store, coll, clusterID := setupCollectorTest(t, 30*time.Second, 15*time.Minute)

	// Short retention to trigger cleanup path
	coll.WithRetention(1 * time.Nanosecond)
	coll.collectAndCleanup(ctx)

	snapshot, err := store.GetLatestSnapshot(ctx, clusterID)
	if err != nil {
		t.Fatalf("Failed to get snapshot: %v", err)
	}

	t.Logf("After collectAndCleanup: %d settings in snapshot", len(snapshot))
}

func TestSourceClusterIDOnlyAttemptedOnce(t *testing.T) {
	ctx, store, coll, clusterID := setupCollectorTest(t, 30*time.Second, 15*time.Minute)

	// Before first collect, flag should be false
	if coll.sourceClusterIDDone {
		t.Fatal("Expected sourceClusterIDDone to be false before first collect")
	}

	// First collect — should successfully retrieve source cluster ID
	err := coll.collect(ctx)
	if err != nil {
		t.Fatalf("first collect() failed: %v", err)
	}

	// After first collect, flag should be true
	if !coll.sourceClusterIDDone {
		t.Error("Expected sourceClusterIDDone to be true after first collect")
	}

	// Verify source cluster ID was actually set
	sourceID, err := store.GetSourceClusterID(ctx, clusterID)
	if err != nil {
		t.Fatalf("GetSourceClusterID failed: %v", err)
	}
	if sourceID == "" {
		t.Error("Expected source cluster ID to be set after first collect")
	}

	// Second collect — should NOT re-attempt
	err = coll.collect(ctx)
	if err != nil {
		t.Fatalf("second collect() failed: %v", err)
	}

	// Flag should still be true
	if !coll.sourceClusterIDDone {
		t.Error("Expected sourceClusterIDDone to remain true after second collect")
	}
}

func TestCleanupWithRetention(t *testing.T) {
	ctx, _, coll, _ := setupCollectorTest(t, 30*time.Second, 15*time.Minute)

	err := coll.collect(ctx)
	if err != nil {
		t.Fatalf("collect() failed: %v", err)
	}

	coll.WithRetention(1 * time.Nanosecond)
	err = coll.cleanup(ctx)
	if err != nil {
		t.Fatalf("cleanup() failed: %v", err)
	}
}
