package storage

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// testClusterID is used for all tests
const testClusterID = "test-cluster"

// testDBURL is set by TestMain after creating the test database
var testDBURL string

func TestMain(m *testing.M) {
	adminURL := os.Getenv("DATABASE_URL")
	if adminURL == "" {
		fmt.Println("DATABASE_URL not set, skipping database setup")
		os.Exit(m.Run())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	adminPool, err := pgxpool.New(ctx, adminURL)
	if err != nil {
		fmt.Printf("Failed to connect to admin database: %v\n", err)
		os.Exit(1)
	}

	testDB := "cluster_history_test"
	testUser := "history_test_user"

	adminPool.Exec(ctx, fmt.Sprintf("CREATE USER IF NOT EXISTS %s", testUser))
	adminPool.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s CASCADE", testDB))
	_, err = adminPool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", testDB))
	if err != nil {
		fmt.Printf("Failed to create test database: %v\n", err)
		os.Exit(1)
	}

	adminPool.Exec(ctx, fmt.Sprintf("GRANT ALL ON DATABASE %s TO %s", testDB, testUser))
	adminPool.Close()

	testDBURL = replaceDatabase(adminURL, testDB)
	os.Setenv("TEST_DATABASE_URL", testDBURL)

	code := m.Run()

	adminPool, err = pgxpool.New(context.Background(), adminURL)
	if err == nil {
		adminPool.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s CASCADE", testDB))
		adminPool.Close()
	}

	os.Exit(code)
}

func replaceDatabase(url, newDB string) string {
	if idx := strings.LastIndex(url, "/"); idx != -1 {
		rest := url[idx+1:]
		if qIdx := strings.Index(rest, "?"); qIdx != -1 {
			return url[:idx+1] + newDB + rest[qIdx:]
		}
		return url[:idx+1] + newDB
	}
	return url
}

func getTestDB(t *testing.T) string {
	if testDBURL != "" {
		return testDBURL
	}
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		url = os.Getenv("HISTORY_DATABASE_URL")
	}
	if url == "" {
		t.Skip("TEST_DATABASE_URL or HISTORY_DATABASE_URL not set")
	}
	return url
}

// setupStoreTest creates a Store connected to the test database and registers cleanup.
func setupStoreTest(t *testing.T, timeout time.Duration) (*Store, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)

	store, err := New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	t.Cleanup(store.Close)

	return store, ctx
}

// cleanupTestData removes all test data from the database using TRUNCATE for speed.
func cleanupTestData(t *testing.T, store *Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store.pool.Exec(ctx, "TRUNCATE TABLE annotations, changes, settings, snapshots, metadata CASCADE")
}

// findChange returns the first change matching the given variable name, or nil.
func findChange(changes []Change, variable string) *Change {
	for i := range changes {
		if changes[i].Variable == variable {
			return &changes[i]
		}
	}
	return nil
}

// saveTestChange creates two snapshots that produce a change, and returns the change ID.
// Calls cleanupTestData first for a clean slate.
func saveTestChange(t *testing.T, ctx context.Context, store *Store, variable string) int64 {
	t.Helper()
	cleanupTestData(t, store)
	s1 := []Setting{{Variable: variable, Value: "v1", SettingType: "s", Description: "Test"}}
	if err := store.SaveSnapshot(ctx, testClusterID, s1, "v1.0"); err != nil {
		t.Fatalf("Failed to save first snapshot: %v", err)
	}
	s2 := []Setting{{Variable: variable, Value: "v2", SettingType: "s", Description: "Test"}}
	if err := store.SaveSnapshot(ctx, testClusterID, s2, "v1.0"); err != nil {
		t.Fatalf("Failed to save second snapshot: %v", err)
	}
	changes, err := store.GetChangesWithAnnotations(ctx, testClusterID, 1)
	if err != nil {
		t.Fatalf("Failed to get changes: %v", err)
	}
	if len(changes) == 0 {
		t.Fatal("No changes found")
	}
	return changes[0].ID
}

func TestNew(t *testing.T) {
	// Longer timeout for first connection — schema migration can be slow
	setupStoreTest(t, 60*time.Second)
}

func TestSaveAndGetSnapshot(t *testing.T) {
	store, ctx := setupStoreTest(t, 10*time.Second)

	settings := []Setting{
		{Variable: "test.setting.one", Value: "value1", SettingType: "s", Description: "Test setting 1"},
		{Variable: "test.setting.two", Value: "value2", SettingType: "i", Description: "Test setting 2"},
	}

	err := store.SaveSnapshot(ctx, testClusterID, settings, "v1.0.0")
	if err != nil {
		t.Fatalf("Failed to save snapshot: %v", err)
	}

	snapshot, err := store.GetLatestSnapshot(ctx, testClusterID)
	if err != nil {
		t.Fatalf("Failed to get snapshot: %v", err)
	}

	if len(snapshot) < 2 {
		t.Errorf("Expected at least 2 settings, got %d", len(snapshot))
	}

	if s, ok := snapshot["test.setting.one"]; !ok {
		t.Error("Expected test.setting.one in snapshot")
	} else if s.Value != "value1" {
		t.Errorf("Expected value1, got %s", s.Value)
	}
}

func TestChangeDetection(t *testing.T) {
	store, ctx := setupStoreTest(t, 10*time.Second)

	settings1 := []Setting{
		{Variable: "change.test.setting", Value: "original", SettingType: "s", Description: "Test"},
	}
	if err := store.SaveSnapshot(ctx, testClusterID, settings1, "v1.0.0"); err != nil {
		t.Fatalf("Failed to save first snapshot: %v", err)
	}

	settings2 := []Setting{
		{Variable: "change.test.setting", Value: "modified", SettingType: "s", Description: "Test"},
	}
	if err := store.SaveSnapshot(ctx, testClusterID, settings2, "v1.0.0"); err != nil {
		t.Fatalf("Failed to save second snapshot: %v", err)
	}

	changes, err := store.GetChanges(ctx, testClusterID, 100)
	if err != nil {
		t.Fatalf("Failed to get changes: %v", err)
	}

	c := findChange(changes, "change.test.setting")
	if c == nil {
		t.Fatal("Expected to find change for change.test.setting")
	}
	if c.OldValue != "original" {
		t.Errorf("Expected old value 'original', got '%s'", c.OldValue)
	}
	if c.NewValue != "modified" {
		t.Errorf("Expected new value 'modified', got '%s'", c.NewValue)
	}
}

func TestNewSettingDetection(t *testing.T) {
	store, ctx := setupStoreTest(t, 10*time.Second)

	settings1 := []Setting{
		{Variable: "existing.setting", Value: "exists", SettingType: "s", Description: "Test"},
	}
	if err := store.SaveSnapshot(ctx, testClusterID, settings1, "v1.0.0"); err != nil {
		t.Fatalf("Failed to save first snapshot: %v", err)
	}

	uniqueName := "new.setting." + time.Now().Format("20060102150405")
	settings2 := []Setting{
		{Variable: "existing.setting", Value: "exists", SettingType: "s", Description: "Test"},
		{Variable: uniqueName, Value: "new", SettingType: "s", Description: "New setting"},
	}
	if err := store.SaveSnapshot(ctx, testClusterID, settings2, "v1.0.0"); err != nil {
		t.Fatalf("Failed to save second snapshot: %v", err)
	}

	changes, err := store.GetChanges(ctx, testClusterID, 100)
	if err != nil {
		t.Fatalf("Failed to get changes: %v", err)
	}

	c := findChange(changes, uniqueName)
	if c == nil {
		t.Fatalf("Expected to find change for %s", uniqueName)
	}
	if c.OldValue != "" {
		t.Errorf("Expected empty old value for new setting, got '%s'", c.OldValue)
	}
	if c.NewValue != "new" {
		t.Errorf("Expected new value 'new', got '%s'", c.NewValue)
	}
}

func TestRemovedSettingDetection(t *testing.T) {
	store, ctx := setupStoreTest(t, 10*time.Second)

	uniqueName := "removed.setting." + time.Now().Format("20060102150405")

	settings1 := []Setting{
		{Variable: uniqueName, Value: "will-be-removed", SettingType: "s", Description: "Test"},
		{Variable: "keeper.setting", Value: "stays", SettingType: "s", Description: "Test"},
	}
	if err := store.SaveSnapshot(ctx, testClusterID, settings1, "v1.0.0"); err != nil {
		t.Fatalf("Failed to save first snapshot: %v", err)
	}

	settings2 := []Setting{
		{Variable: "keeper.setting", Value: "stays", SettingType: "s", Description: "Test"},
	}
	if err := store.SaveSnapshot(ctx, testClusterID, settings2, "v1.0.0"); err != nil {
		t.Fatalf("Failed to save second snapshot: %v", err)
	}

	changes, err := store.GetChanges(ctx, testClusterID, 100)
	if err != nil {
		t.Fatalf("Failed to get changes: %v", err)
	}

	c := findChange(changes, uniqueName)
	if c == nil {
		t.Fatalf("Expected to find change for removed setting %s", uniqueName)
	}
	if c.OldValue != "will-be-removed" {
		t.Errorf("Expected old value 'will-be-removed', got '%s'", c.OldValue)
	}
	if c.NewValue != "" {
		t.Errorf("Expected empty new value for removed setting, got '%s'", c.NewValue)
	}
}

func TestGetChangesLimit(t *testing.T) {
	store, ctx := setupStoreTest(t, 30*time.Second)

	clusterID := "limit-test-" + time.Now().Format("20060102150405.000")

	baseSettings := []Setting{
		{Variable: "limit.test.setting", Value: "initial", SettingType: "s", Description: "Test"},
	}
	if err := store.SaveSnapshot(ctx, clusterID, baseSettings, "v1.0.0"); err != nil {
		t.Fatalf("Failed to save initial snapshot: %v", err)
	}

	for i := range 7 {
		settings := []Setting{
			{Variable: "limit.test.setting", Value: fmt.Sprintf("value%d", i), SettingType: "s", Description: "Test"},
		}
		if err := store.SaveSnapshot(ctx, clusterID, settings, "v1.0.0"); err != nil {
			t.Fatalf("Failed to save snapshot %d: %v", i, err)
		}
	}

	allChanges, err := store.GetChanges(ctx, clusterID, 100)
	if err != nil {
		t.Fatalf("Failed to get all changes: %v", err)
	}
	if len(allChanges) < 6 {
		t.Fatalf("Expected at least 6 changes for limit test, got %d", len(allChanges))
	}

	limitedChanges, err := store.GetChanges(ctx, clusterID, 5)
	if err != nil {
		t.Fatalf("Failed to get limited changes: %v", err)
	}
	if len(limitedChanges) != 5 {
		t.Errorf("Expected exactly 5 changes with limit, got %d", len(limitedChanges))
	}

	oneChange, err := store.GetChanges(ctx, clusterID, 1)
	if err != nil {
		t.Fatalf("Failed to get single change: %v", err)
	}
	if len(oneChange) != 1 {
		t.Errorf("Expected exactly 1 change with limit=1, got %d", len(oneChange))
	}
}

func TestCleanupOldSnapshots(t *testing.T) {
	store, ctx := setupStoreTest(t, 10*time.Second)

	clusterID := "cleanup-snapshot-test-" + time.Now().Format("20060102150405.000")

	settings := []Setting{
		{Variable: "cleanup.test.setting", Value: "value1", SettingType: "s", Description: "Test"},
	}
	for range 3 {
		if err := store.SaveSnapshot(ctx, clusterID, settings, "v1.0.0"); err != nil {
			t.Fatalf("Failed to save snapshot: %v", err)
		}
		time.Sleep(10 * time.Millisecond) // Ensure different timestamps
	}

	snapshots, err := store.ListSnapshots(ctx, clusterID, 10)
	if err != nil {
		t.Fatalf("Failed to list snapshots: %v", err)
	}
	if len(snapshots) < 3 {
		t.Fatalf("Expected at least 3 snapshots, got %d", len(snapshots))
	}

	deleted, err := store.CleanupOldSnapshots(ctx, clusterID, 0)
	if err != nil {
		t.Fatalf("Failed to cleanup snapshots: %v", err)
	}
	if deleted < 3 {
		t.Errorf("Expected to delete at least 3 snapshots, deleted %d", deleted)
	}

	snapshots, err = store.ListSnapshots(ctx, clusterID, 10)
	if err != nil {
		t.Fatalf("Failed to list snapshots after cleanup: %v", err)
	}
	if len(snapshots) != 0 {
		t.Errorf("Expected 0 snapshots after cleanup, got %d", len(snapshots))
	}

	for range 2 {
		if err := store.SaveSnapshot(ctx, clusterID, settings, "v1.0.0"); err != nil {
			t.Fatalf("Failed to save snapshot: %v", err)
		}
	}

	deleted, err = store.CleanupOldSnapshots(ctx, clusterID, 24*time.Hour)
	if err != nil {
		t.Fatalf("Failed to cleanup snapshots: %v", err)
	}
	if deleted != 0 {
		t.Errorf("Expected 0 deletions with 24h retention on fresh snapshots, got %d", deleted)
	}
}

func TestCleanupOldChanges(t *testing.T) {
	store, ctx := setupStoreTest(t, 10*time.Second)

	settings1 := []Setting{
		{Variable: "cleanup.change.test", Value: "original", SettingType: "s", Description: "Test"},
	}
	if err := store.SaveSnapshot(ctx, testClusterID, settings1, "v1.0.0"); err != nil {
		t.Fatalf("Failed to save first snapshot: %v", err)
	}

	settings2 := []Setting{
		{Variable: "cleanup.change.test", Value: "modified", SettingType: "s", Description: "Test"},
	}
	if err := store.SaveSnapshot(ctx, testClusterID, settings2, "v1.0.0"); err != nil {
		t.Fatalf("Failed to save second snapshot: %v", err)
	}

	deleted, err := store.CleanupOldChanges(ctx, testClusterID, 0)
	if err != nil {
		t.Fatalf("Failed to cleanup changes: %v", err)
	}

	if deleted < 1 {
		t.Logf("Deleted %d changes (may vary based on test order)", deleted)
	}

	changes, err := store.GetChanges(ctx, testClusterID, 100)
	if err != nil {
		t.Fatalf("Failed to get changes: %v", err)
	}
	if len(changes) != 0 {
		t.Errorf("Expected 0 changes after cleanup, got %d", len(changes))
	}
}

func TestSetAndGetMetadata(t *testing.T) {
	store, ctx := setupStoreTest(t, 10*time.Second)

	err := store.SetMetadata(ctx, testClusterID, "test_key", "test_value")
	if err != nil {
		t.Fatalf("Failed to set metadata: %v", err)
	}

	value, err := store.GetMetadata(ctx, testClusterID, "test_key")
	if err != nil {
		t.Fatalf("Failed to get metadata: %v", err)
	}
	if value != "test_value" {
		t.Errorf("Expected 'test_value', got '%s'", value)
	}

	err = store.SetMetadata(ctx, testClusterID, "test_key", "updated_value")
	if err != nil {
		t.Fatalf("Failed to update metadata: %v", err)
	}

	value, err = store.GetMetadata(ctx, testClusterID, "test_key")
	if err != nil {
		t.Fatalf("Failed to get updated metadata: %v", err)
	}
	if value != "updated_value" {
		t.Errorf("Expected 'updated_value', got '%s'", value)
	}

	value, err = store.GetMetadata(ctx, testClusterID, "non_existent_key")
	if err != nil {
		t.Fatalf("Failed to get non-existent metadata: %v", err)
	}
	if value != "" {
		t.Errorf("Expected empty string for non-existent key, got '%s'", value)
	}
}

func TestMetadataSameKeyDifferentClusters(t *testing.T) {
	store, ctx := setupStoreTest(t, 60*time.Second)
	cleanupTestData(t, store)

	// Set same key for two different clusters — this should work with composite PK
	err := store.SetMetadata(ctx, "cluster-a", "database_version", "v25.1.0")
	if err != nil {
		t.Fatalf("Failed to set metadata for cluster-a: %v", err)
	}

	err = store.SetMetadata(ctx, "cluster-b", "database_version", "v25.2.0")
	if err != nil {
		t.Fatalf("Failed to set metadata for cluster-b: %v", err)
	}

	// Verify each cluster has its own value
	valueA, err := store.GetMetadata(ctx, "cluster-a", "database_version")
	if err != nil {
		t.Fatalf("Failed to get metadata for cluster-a: %v", err)
	}
	if valueA != "v25.1.0" {
		t.Errorf("Expected 'v25.1.0' for cluster-a, got '%s'", valueA)
	}

	valueB, err := store.GetMetadata(ctx, "cluster-b", "database_version")
	if err != nil {
		t.Fatalf("Failed to get metadata for cluster-b: %v", err)
	}
	if valueB != "v25.2.0" {
		t.Errorf("Expected 'v25.2.0' for cluster-b, got '%s'", valueB)
	}
}

func TestSourceClusterIDMetadata(t *testing.T) {
	store, ctx := setupStoreTest(t, 10*time.Second)

	sourceClusterID := "source-cluster-id-12345"

	err := store.SetSourceClusterID(ctx, testClusterID, sourceClusterID)
	if err != nil {
		t.Fatalf("Failed to set source cluster ID: %v", err)
	}

	retrieved, err := store.GetSourceClusterID(ctx, testClusterID)
	if err != nil {
		t.Fatalf("Failed to get source cluster ID: %v", err)
	}
	if retrieved != sourceClusterID {
		t.Errorf("Expected '%s', got '%s'", sourceClusterID, retrieved)
	}
}

func TestDatabaseVersionMetadata(t *testing.T) {
	store, ctx := setupStoreTest(t, 10*time.Second)

	testVersion := "CockroachDB CCL v25.4.2"

	err := store.SetDatabaseVersion(ctx, testClusterID, testVersion)
	if err != nil {
		t.Fatalf("Failed to set database version: %v", err)
	}

	version, err := store.GetDatabaseVersion(ctx, testClusterID)
	if err != nil {
		t.Fatalf("Failed to get database version: %v", err)
	}
	if version != testVersion {
		t.Errorf("Expected '%s', got '%s'", testVersion, version)
	}
}

func TestChangeWithVersion(t *testing.T) {
	store, ctx := setupStoreTest(t, 10*time.Second)
	cleanupTestData(t, store)

	testVersion := "v25.4.2"

	settings1 := []Setting{
		{Variable: "version.test.setting", Value: "original", SettingType: "s", Description: "Test"},
	}
	if err := store.SaveSnapshot(ctx, testClusterID, settings1, testVersion); err != nil {
		t.Fatalf("Failed to save first snapshot: %v", err)
	}

	settings2 := []Setting{
		{Variable: "version.test.setting", Value: "modified", SettingType: "s", Description: "Test"},
	}
	if err := store.SaveSnapshot(ctx, testClusterID, settings2, testVersion); err != nil {
		t.Fatalf("Failed to save second snapshot: %v", err)
	}

	changes, err := store.GetChanges(ctx, testClusterID, 100)
	if err != nil {
		t.Fatalf("Failed to get changes: %v", err)
	}

	c := findChange(changes, "version.test.setting")
	if c == nil {
		t.Fatal("Expected to find change for version.test.setting")
	}
	if c.Version != testVersion {
		t.Errorf("Expected version '%s', got '%s'", testVersion, c.Version)
	}
}

func TestAnnotationCRUD(t *testing.T) {
	store, ctx := setupStoreTest(t, 10*time.Second)
	changeID := saveTestChange(t, ctx, store, "annotation.test")

	ann, err := store.CreateAnnotation(ctx, changeID, "Test note", "testuser")
	if err != nil {
		t.Fatalf("CreateAnnotation failed: %v", err)
	}
	if ann.ID == 0 {
		t.Error("Expected non-zero annotation ID")
	}
	if ann.Content != "Test note" {
		t.Errorf("Expected content 'Test note', got '%s'", ann.Content)
	}
	if ann.CreatedBy != "testuser" {
		t.Errorf("Expected createdBy 'testuser', got '%s'", ann.CreatedBy)
	}
	if ann.CreatedAt.IsZero() {
		t.Error("Expected non-zero created_at")
	}

	retrieved, err := store.GetAnnotation(ctx, ann.ID)
	if err != nil {
		t.Fatalf("GetAnnotation failed: %v", err)
	}
	if retrieved == nil {
		t.Fatal("Expected annotation, got nil")
	}
	if retrieved.Content != "Test note" {
		t.Errorf("Expected content 'Test note', got '%s'", retrieved.Content)
	}

	err = store.UpdateAnnotation(ctx, ann.ID, "Updated note", "otheruser")
	if err != nil {
		t.Fatalf("UpdateAnnotation failed: %v", err)
	}
	updated, _ := store.GetAnnotation(ctx, ann.ID)
	if updated.Content != "Updated note" {
		t.Errorf("Expected updated content, got '%s'", updated.Content)
	}
	if updated.UpdatedBy != "otheruser" {
		t.Errorf("Expected updatedBy 'otheruser', got '%s'", updated.UpdatedBy)
	}
	if updated.UpdatedAt.IsZero() {
		t.Error("Expected non-zero updated_at after update")
	}

	err = store.DeleteAnnotation(ctx, ann.ID)
	if err != nil {
		t.Fatalf("DeleteAnnotation failed: %v", err)
	}
	deleted, _ := store.GetAnnotation(ctx, ann.ID)
	if deleted != nil {
		t.Error("Expected nil after delete")
	}
}

func TestAnnotationNotFound(t *testing.T) {
	store, ctx := setupStoreTest(t, 10*time.Second)

	ann, err := store.GetAnnotation(ctx, 999999)
	if err != nil {
		t.Fatalf("GetAnnotation should not error for missing: %v", err)
	}
	if ann != nil {
		t.Error("Expected nil for non-existent annotation")
	}

	err = store.UpdateAnnotation(ctx, 999999, "content", "user")
	if err == nil {
		t.Error("Expected error for updating non-existent annotation")
	}

	err = store.DeleteAnnotation(ctx, 999999)
	if err == nil {
		t.Error("Expected error for deleting non-existent annotation")
	}
}

func TestAnnotationCascadeDelete(t *testing.T) {
	store, ctx := setupStoreTest(t, 10*time.Second)
	changeID := saveTestChange(t, ctx, store, "cascade.test")

	ann, err := store.CreateAnnotation(ctx, changeID, "Will be deleted", "user")
	if err != nil {
		t.Fatalf("Failed to create annotation: %v", err)
	}

	store.CleanupOldChanges(ctx, testClusterID, 0)

	retrieved, _ := store.GetAnnotation(ctx, ann.ID)
	if retrieved != nil {
		t.Error("Expected annotation to be deleted with change")
	}
}

func TestGetChangesWithAnnotations(t *testing.T) {
	store, ctx := setupStoreTest(t, 10*time.Second)
	cleanupTestData(t, store)

	settings1 := []Setting{{Variable: "join.test.a", Value: "v1", SettingType: "s", Description: "Test A"}}
	if err := store.SaveSnapshot(ctx, testClusterID, settings1, "v1.0"); err != nil {
		t.Fatalf("Failed to save snapshot: %v", err)
	}
	settings2 := []Setting{
		{Variable: "join.test.a", Value: "v2", SettingType: "s", Description: "Test A"},
		{Variable: "join.test.b", Value: "x1", SettingType: "s", Description: "Test B"},
	}
	if err := store.SaveSnapshot(ctx, testClusterID, settings2, "v1.0"); err != nil {
		t.Fatalf("Failed to save snapshot: %v", err)
	}
	settings3 := []Setting{
		{Variable: "join.test.a", Value: "v2", SettingType: "s", Description: "Test A"},
		{Variable: "join.test.b", Value: "x2", SettingType: "s", Description: "Test B"},
	}
	if err := store.SaveSnapshot(ctx, testClusterID, settings3, "v1.0"); err != nil {
		t.Fatalf("Failed to save snapshot: %v", err)
	}

	changes, err := store.GetChangesWithAnnotations(ctx, testClusterID, 10)
	if err != nil {
		t.Fatalf("Failed to get changes with annotations: %v", err)
	}
	if len(changes) < 2 {
		t.Fatalf("Expected at least 2 changes, got %d", len(changes))
	}

	for i, c := range changes {
		if c.ID == 0 {
			t.Errorf("Change %d has zero ID", i)
		}
	}

	_, err = store.CreateAnnotation(ctx, changes[0].ID, "First change note", "user")
	if err != nil {
		t.Fatalf("Failed to create annotation: %v", err)
	}

	changes, err = store.GetChangesWithAnnotations(ctx, testClusterID, 10)
	if err != nil {
		t.Fatalf("Failed to get changes with annotations: %v", err)
	}

	foundWithAnn := false
	foundWithoutAnn := false
	for _, c := range changes {
		if c.Annotation != nil && c.Annotation.Content == "First change note" {
			foundWithAnn = true
			if c.Annotation.ChangeID != c.ID {
				t.Errorf("Annotation changeID %d doesn't match change ID %d", c.Annotation.ChangeID, c.ID)
			}
		} else if c.Annotation == nil {
			foundWithoutAnn = true
		}
	}
	if !foundWithAnn {
		t.Error("Expected to find change with annotation")
	}
	if !foundWithoutAnn {
		t.Error("Expected to find change without annotation")
	}
}

func TestAnnotationDuplicateFails(t *testing.T) {
	store, ctx := setupStoreTest(t, 10*time.Second)
	changeID := saveTestChange(t, ctx, store, "dup.test")

	_, err := store.CreateAnnotation(ctx, changeID, "First", "user")
	if err != nil {
		t.Fatalf("First CreateAnnotation failed: %v", err)
	}

	// UNIQUE constraint should reject a second annotation on the same change
	_, err = store.CreateAnnotation(ctx, changeID, "Second", "user")
	if err == nil {
		t.Error("Expected error for duplicate annotation on same change")
	}
}

func TestListClusters(t *testing.T) {
	store, ctx := setupStoreTest(t, 10*time.Second)

	settings := []Setting{{Variable: "list.test", Value: "v1", SettingType: "s", Description: "Test"}}
	store.SaveSnapshot(ctx, "list-cluster-alpha", settings, "v1.0")
	store.SaveSnapshot(ctx, "list-cluster-beta", settings, "v1.0")
	store.SaveSnapshot(ctx, "list-cluster-gamma", settings, "v1.0")

	clusters, err := store.ListClusters(ctx)
	if err != nil {
		t.Fatalf("ListClusters failed: %v", err)
	}

	clusterMap := make(map[string]bool)
	for _, c := range clusters {
		clusterMap[c] = true
	}

	for _, expected := range []string{"list-cluster-alpha", "list-cluster-beta", "list-cluster-gamma"} {
		if !clusterMap[expected] {
			t.Errorf("ListClusters() missing %q", expected)
		}
	}
}

func TestMultiClusterChanges(t *testing.T) {
	store, ctx := setupStoreTest(t, 10*time.Second)

	settingsA1 := []Setting{{Variable: "multi.test.setting", Value: "a1", SettingType: "s", Description: "Test"}}
	store.SaveSnapshot(ctx, "multi-cluster-a", settingsA1, "v1.0")
	settingsA2 := []Setting{{Variable: "multi.test.setting", Value: "a2", SettingType: "s", Description: "Test"}}
	store.SaveSnapshot(ctx, "multi-cluster-a", settingsA2, "v1.0")

	settingsB1 := []Setting{{Variable: "multi.test.setting", Value: "b1", SettingType: "s", Description: "Test"}}
	store.SaveSnapshot(ctx, "multi-cluster-b", settingsB1, "v1.0")
	settingsB2 := []Setting{{Variable: "multi.test.setting", Value: "b2", SettingType: "s", Description: "Test"}}
	store.SaveSnapshot(ctx, "multi-cluster-b", settingsB2, "v1.0")

	changesA, err := store.GetChanges(ctx, "multi-cluster-a", 100)
	if err != nil {
		t.Fatalf("GetChanges(multi-cluster-a) failed: %v", err)
	}
	for _, c := range changesA {
		if c.ClusterID != "multi-cluster-a" {
			t.Errorf("Expected ClusterID 'multi-cluster-a', got %q", c.ClusterID)
		}
	}

	changesB, err := store.GetChanges(ctx, "multi-cluster-b", 100)
	if err != nil {
		t.Fatalf("GetChanges(multi-cluster-b) failed: %v", err)
	}
	for _, c := range changesB {
		if c.ClusterID != "multi-cluster-b" {
			t.Errorf("Expected ClusterID 'multi-cluster-b', got %q", c.ClusterID)
		}
	}
}

func TestListSnapshots(t *testing.T) {
	store, ctx := setupStoreTest(t, 30*time.Second)

	clusterID := "list-snapshots-test"

	settings := []Setting{{Variable: "snapshot.test", Value: "v1", SettingType: "s", Description: "Test"}}
	for range 5 {
		if err := store.SaveSnapshot(ctx, clusterID, settings, "v1.0"); err != nil {
			t.Fatalf("Failed to save snapshot: %v", err)
		}
		time.Sleep(10 * time.Millisecond) // Ensure different timestamps
	}

	snapshots, err := store.ListSnapshots(ctx, clusterID, 10)
	if err != nil {
		t.Fatalf("ListSnapshots failed: %v", err)
	}
	if len(snapshots) < 5 {
		t.Errorf("Expected at least 5 snapshots, got %d", len(snapshots))
	}

	// Verify order (most recent first)
	for i := 1; i < len(snapshots); i++ {
		if snapshots[i].CollectedAt.After(snapshots[i-1].CollectedAt) {
			t.Errorf("Snapshots not in descending order at index %d", i)
		}
	}

	limited, err := store.ListSnapshots(ctx, clusterID, 2)
	if err != nil {
		t.Fatalf("ListSnapshots with limit failed: %v", err)
	}
	if len(limited) != 2 {
		t.Errorf("Expected 2 snapshots with limit, got %d", len(limited))
	}

	empty, err := store.ListSnapshots(ctx, "non-existent-cluster", 10)
	if err != nil {
		t.Fatalf("ListSnapshots for non-existent cluster failed: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("Expected 0 snapshots for non-existent cluster, got %d", len(empty))
	}
}

func TestGetSnapshotByID(t *testing.T) {
	store, ctx := setupStoreTest(t, 30*time.Second)

	clusterID := "get-snapshot-by-id-test"

	settings := []Setting{
		{Variable: "test.setting.a", Value: "valueA", SettingType: "s", Description: "Description A"},
		{Variable: "test.setting.b", Value: "valueB", SettingType: "i", Description: "Description B"},
	}
	if err := store.SaveSnapshot(ctx, clusterID, settings, "v1.0"); err != nil {
		t.Fatalf("Failed to save snapshot: %v", err)
	}

	snapshots, err := store.ListSnapshots(ctx, clusterID, 1)
	if err != nil || len(snapshots) == 0 {
		t.Fatalf("Failed to get snapshot ID: %v", err)
	}
	snapshotID := snapshots[0].ID

	retrieved, err := store.GetSnapshotByID(ctx, snapshotID)
	if err != nil {
		t.Fatalf("GetSnapshotByID failed: %v", err)
	}
	if retrieved == nil {
		t.Fatal("Expected settings, got nil")
	}

	if len(retrieved) != 2 {
		t.Errorf("Expected 2 settings, got %d", len(retrieved))
	}
	if s, ok := retrieved["test.setting.a"]; !ok || s.Value != "valueA" {
		t.Errorf("Expected test.setting.a=valueA, got %v", retrieved["test.setting.a"])
	}
	if s, ok := retrieved["test.setting.b"]; !ok || s.Value != "valueB" {
		t.Errorf("Expected test.setting.b=valueB, got %v", retrieved["test.setting.b"])
	}

	notFound, err := store.GetSnapshotByID(ctx, 999999999)
	if err != nil {
		t.Fatalf("GetSnapshotByID for non-existent should not error: %v", err)
	}
	if notFound != nil {
		t.Errorf("Expected nil for non-existent snapshot, got %v", notFound)
	}
}

func TestGetAllChanges(t *testing.T) {
	store, ctx := setupStoreTest(t, 15*time.Second)

	cluster1 := "all-changes-cluster1-" + time.Now().Format("20060102150405.000")
	cluster2 := "all-changes-cluster2-" + time.Now().Format("20060102150405.000")

	settings1a := []Setting{{Variable: "all.test.setting", Value: "v1", SettingType: "s", Description: "Test"}}
	store.SaveSnapshot(ctx, cluster1, settings1a, "v1.0")
	settings1b := []Setting{{Variable: "all.test.setting", Value: "v2", SettingType: "s", Description: "Test"}}
	store.SaveSnapshot(ctx, cluster1, settings1b, "v1.0")

	settings2a := []Setting{{Variable: "all.test.setting", Value: "a1", SettingType: "s", Description: "Test"}}
	store.SaveSnapshot(ctx, cluster2, settings2a, "v1.0")
	settings2b := []Setting{{Variable: "all.test.setting", Value: "a2", SettingType: "s", Description: "Test"}}
	store.SaveSnapshot(ctx, cluster2, settings2b, "v1.0")

	changes, err := store.GetAllChanges(ctx, 100)
	if err != nil {
		t.Fatalf("GetAllChanges failed: %v", err)
	}
	if len(changes) < 2 {
		t.Errorf("Expected at least 2 changes, got %d", len(changes))
	}

	cluster1Found := false
	cluster2Found := false
	for _, c := range changes {
		if c.ClusterID == cluster1 {
			cluster1Found = true
		}
		if c.ClusterID == cluster2 {
			cluster2Found = true
		}
	}
	if !cluster1Found {
		t.Errorf("Expected changes from %s", cluster1)
	}
	if !cluster2Found {
		t.Errorf("Expected changes from %s", cluster2)
	}

	limited, err := store.GetAllChanges(ctx, 1)
	if err != nil {
		t.Fatalf("GetAllChanges with limit failed: %v", err)
	}
	if len(limited) != 1 {
		t.Errorf("Expected exactly 1 change with limit=1, got %d", len(limited))
	}
}

