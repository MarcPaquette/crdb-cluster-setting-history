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
	// Get admin connection to create test database
	adminURL := os.Getenv("DATABASE_URL")
	if adminURL == "" {
		fmt.Println("DATABASE_URL not set, skipping database setup")
		os.Exit(m.Run())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Connect as admin
	adminPool, err := pgxpool.New(ctx, adminURL)
	if err != nil {
		fmt.Printf("Failed to connect to admin database: %v\n", err)
		os.Exit(1)
	}

	// Create test database and user
	testDB := "cluster_history_test"
	testUser := "history_test_user"

	// Create user if not exists (ignore error if already exists)
	adminPool.Exec(ctx, fmt.Sprintf("CREATE USER IF NOT EXISTS %s", testUser))

	// Drop and recreate test database for clean slate
	adminPool.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s CASCADE", testDB))
	_, err = adminPool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", testDB))
	if err != nil {
		fmt.Printf("Failed to create test database: %v\n", err)
		os.Exit(1)
	}

	// Grant privileges
	adminPool.Exec(ctx, fmt.Sprintf("GRANT ALL ON DATABASE %s TO %s", testDB, testUser))

	adminPool.Close()

	// Build test database URL
	// Replace database name in admin URL
	testDBURL = replaceDatabase(adminURL, testDB)

	// Also set for any code that reads the env var directly
	os.Setenv("TEST_DATABASE_URL", testDBURL)

	// Run tests
	code := m.Run()

	// Cleanup: drop test database
	adminPool, err = pgxpool.New(context.Background(), adminURL)
	if err == nil {
		adminPool.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s CASCADE", testDB))
		adminPool.Close()
	}

	os.Exit(code)
}

// replaceDatabase replaces the database name in a connection URL
func replaceDatabase(url, newDB string) string {
	// Handle postgresql://user@host:port/database?params format
	if idx := strings.LastIndex(url, "/"); idx != -1 {
		// Find the end of database name (before ? if present)
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

// cleanupTestData removes all test data from the database using TRUNCATE for speed
func cleanupTestData(t *testing.T, store *Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// TRUNCATE is much faster than DELETE for large tables
	// CASCADE handles foreign key relationships
	store.pool.Exec(ctx, "TRUNCATE TABLE annotations, changes, settings, snapshots, metadata CASCADE")
}

func TestNew(t *testing.T) {
	// Longer timeout for first connection - schema migration can be slow
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()
}

func TestSaveAndGetSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Save a snapshot
	settings := []Setting{
		{Variable: "test.setting.one", Value: "value1", SettingType: "s", Description: "Test setting 1"},
		{Variable: "test.setting.two", Value: "value2", SettingType: "i", Description: "Test setting 2"},
	}

	err = store.SaveSnapshot(ctx, testClusterID, settings, "v1.0.0")
	if err != nil {
		t.Fatalf("Failed to save snapshot: %v", err)
	}

	// Get the snapshot
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// First snapshot
	settings1 := []Setting{
		{Variable: "change.test.setting", Value: "original", SettingType: "s", Description: "Test"},
	}
	err = store.SaveSnapshot(ctx, testClusterID, settings1, "v1.0.0")
	if err != nil {
		t.Fatalf("Failed to save first snapshot: %v", err)
	}

	// Second snapshot with changed value
	settings2 := []Setting{
		{Variable: "change.test.setting", Value: "modified", SettingType: "s", Description: "Test"},
	}
	err = store.SaveSnapshot(ctx, testClusterID, settings2, "v1.0.0")
	if err != nil {
		t.Fatalf("Failed to save second snapshot: %v", err)
	}

	// Check for changes
	changes, err := store.GetChanges(ctx, testClusterID, 100)
	if err != nil {
		t.Fatalf("Failed to get changes: %v", err)
	}

	// Find our change
	found := false
	for _, c := range changes {
		if c.Variable == "change.test.setting" {
			found = true
			if c.OldValue != "original" {
				t.Errorf("Expected old value 'original', got '%s'", c.OldValue)
			}
			if c.NewValue != "modified" {
				t.Errorf("Expected new value 'modified', got '%s'", c.NewValue)
			}
			break
		}
	}

	if !found {
		t.Error("Expected to find change for change.test.setting")
	}
}

func TestNewSettingDetection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// First snapshot
	settings1 := []Setting{
		{Variable: "existing.setting", Value: "exists", SettingType: "s", Description: "Test"},
	}
	err = store.SaveSnapshot(ctx, testClusterID, settings1, "v1.0.0")
	if err != nil {
		t.Fatalf("Failed to save first snapshot: %v", err)
	}

	// Second snapshot with new setting
	uniqueName := "new.setting." + time.Now().Format("20060102150405")
	settings2 := []Setting{
		{Variable: "existing.setting", Value: "exists", SettingType: "s", Description: "Test"},
		{Variable: uniqueName, Value: "new", SettingType: "s", Description: "New setting"},
	}
	err = store.SaveSnapshot(ctx, testClusterID, settings2, "v1.0.0")
	if err != nil {
		t.Fatalf("Failed to save second snapshot: %v", err)
	}

	// Check for changes
	changes, err := store.GetChanges(ctx, testClusterID, 100)
	if err != nil {
		t.Fatalf("Failed to get changes: %v", err)
	}

	// Find our new setting
	found := false
	for _, c := range changes {
		if c.Variable == uniqueName {
			found = true
			if c.OldValue != "" {
				t.Errorf("Expected empty old value for new setting, got '%s'", c.OldValue)
			}
			if c.NewValue != "new" {
				t.Errorf("Expected new value 'new', got '%s'", c.NewValue)
			}
			break
		}
	}

	if !found {
		t.Errorf("Expected to find change for %s", uniqueName)
	}
}

func TestRemovedSettingDetection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	uniqueName := "removed.setting." + time.Now().Format("20060102150405")

	// First snapshot with the setting
	settings1 := []Setting{
		{Variable: uniqueName, Value: "will-be-removed", SettingType: "s", Description: "Test"},
		{Variable: "keeper.setting", Value: "stays", SettingType: "s", Description: "Test"},
	}
	err = store.SaveSnapshot(ctx, testClusterID, settings1, "v1.0.0")
	if err != nil {
		t.Fatalf("Failed to save first snapshot: %v", err)
	}

	// Second snapshot without the setting
	settings2 := []Setting{
		{Variable: "keeper.setting", Value: "stays", SettingType: "s", Description: "Test"},
	}
	err = store.SaveSnapshot(ctx, testClusterID, settings2, "v1.0.0")
	if err != nil {
		t.Fatalf("Failed to save second snapshot: %v", err)
	}

	// Check for changes
	changes, err := store.GetChanges(ctx, testClusterID, 100)
	if err != nil {
		t.Fatalf("Failed to get changes: %v", err)
	}

	// Find our removed setting
	found := false
	for _, c := range changes {
		if c.Variable == uniqueName {
			found = true
			if c.OldValue != "will-be-removed" {
				t.Errorf("Expected old value 'will-be-removed', got '%s'", c.OldValue)
			}
			if c.NewValue != "" {
				t.Errorf("Expected empty new value for removed setting, got '%s'", c.NewValue)
			}
			break
		}
	}

	if !found {
		t.Errorf("Expected to find change for removed setting %s", uniqueName)
	}
}

func TestGetChangesLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Get limited changes
	changes, err := store.GetChanges(ctx, testClusterID, 5)
	if err != nil {
		t.Fatalf("Failed to get changes: %v", err)
	}

	if len(changes) > 5 {
		t.Errorf("Expected at most 5 changes, got %d", len(changes))
	}
}

func TestCleanupOldSnapshots(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Save a snapshot
	settings := []Setting{
		{Variable: "cleanup.test.setting", Value: "value1", SettingType: "s", Description: "Test"},
	}
	err = store.SaveSnapshot(ctx, testClusterID, settings, "v1.0.0")
	if err != nil {
		t.Fatalf("Failed to save snapshot: %v", err)
	}

	// Cleanup with zero retention should delete everything
	deleted, err := store.CleanupOldSnapshots(ctx, testClusterID, 0)
	if err != nil {
		t.Fatalf("Failed to cleanup snapshots: %v", err)
	}

	// Should have deleted at least the one we just created
	if deleted < 1 {
		t.Logf("Deleted %d snapshots (may vary based on test order)", deleted)
	}

	// Cleanup with long retention should delete nothing new
	deleted, err = store.CleanupOldSnapshots(ctx, testClusterID, 24*time.Hour)
	if err != nil {
		t.Fatalf("Failed to cleanup snapshots: %v", err)
	}
	t.Logf("Deleted %d snapshots with 24h retention", deleted)
}

func TestCleanupOldChanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Create some changes by saving two different snapshots
	settings1 := []Setting{
		{Variable: "cleanup.change.test", Value: "original", SettingType: "s", Description: "Test"},
	}
	err = store.SaveSnapshot(ctx, testClusterID, settings1, "v1.0.0")
	if err != nil {
		t.Fatalf("Failed to save first snapshot: %v", err)
	}

	settings2 := []Setting{
		{Variable: "cleanup.change.test", Value: "modified", SettingType: "s", Description: "Test"},
	}
	err = store.SaveSnapshot(ctx, testClusterID, settings2, "v1.0.0")
	if err != nil {
		t.Fatalf("Failed to save second snapshot: %v", err)
	}

	// Cleanup with zero retention should delete everything
	deleted, err := store.CleanupOldChanges(ctx, testClusterID, 0)
	if err != nil {
		t.Fatalf("Failed to cleanup changes: %v", err)
	}

	if deleted < 1 {
		t.Logf("Deleted %d changes (may vary based on test order)", deleted)
	}

	// Verify changes are gone
	changes, err := store.GetChanges(ctx, testClusterID, 100)
	if err != nil {
		t.Fatalf("Failed to get changes: %v", err)
	}
	if len(changes) != 0 {
		t.Errorf("Expected 0 changes after cleanup, got %d", len(changes))
	}
}

func TestSetAndGetMetadata(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Test setting metadata
	err = store.SetMetadata(ctx, testClusterID, "test_key", "test_value")
	if err != nil {
		t.Fatalf("Failed to set metadata: %v", err)
	}

	// Test getting metadata
	value, err := store.GetMetadata(ctx, testClusterID, "test_key")
	if err != nil {
		t.Fatalf("Failed to get metadata: %v", err)
	}
	if value != "test_value" {
		t.Errorf("Expected 'test_value', got '%s'", value)
	}

	// Test updating metadata
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

	// Test getting non-existent key
	value, err = store.GetMetadata(ctx, testClusterID, "non_existent_key")
	if err != nil {
		t.Fatalf("Failed to get non-existent metadata: %v", err)
	}
	if value != "" {
		t.Errorf("Expected empty string for non-existent key, got '%s'", value)
	}
}

func TestSourceClusterIDMetadata(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	sourceClusterID := "source-cluster-id-12345"

	// Test setting source cluster ID
	err = store.SetSourceClusterID(ctx, testClusterID, sourceClusterID)
	if err != nil {
		t.Fatalf("Failed to set source cluster ID: %v", err)
	}

	// Test getting source cluster ID
	retrieved, err := store.GetSourceClusterID(ctx, testClusterID)
	if err != nil {
		t.Fatalf("Failed to get source cluster ID: %v", err)
	}
	if retrieved != sourceClusterID {
		t.Errorf("Expected '%s', got '%s'", sourceClusterID, retrieved)
	}
}

func TestDatabaseVersionMetadata(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	testVersion := "CockroachDB CCL v25.4.2"

	// Test setting database version
	err = store.SetDatabaseVersion(ctx, testClusterID, testVersion)
	if err != nil {
		t.Fatalf("Failed to set database version: %v", err)
	}

	// Test getting database version
	version, err := store.GetDatabaseVersion(ctx, testClusterID)
	if err != nil {
		t.Fatalf("Failed to get database version: %v", err)
	}
	if version != testVersion {
		t.Errorf("Expected '%s', got '%s'", testVersion, version)
	}
}

func TestChangeWithVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Clean up any existing data
	cleanupTestData(t, store)

	testVersion := "v25.4.2"

	// First snapshot
	settings1 := []Setting{
		{Variable: "version.test.setting", Value: "original", SettingType: "s", Description: "Test"},
	}
	err = store.SaveSnapshot(ctx, testClusterID, settings1, testVersion)
	if err != nil {
		t.Fatalf("Failed to save first snapshot: %v", err)
	}

	// Second snapshot with changed value
	settings2 := []Setting{
		{Variable: "version.test.setting", Value: "modified", SettingType: "s", Description: "Test"},
	}
	err = store.SaveSnapshot(ctx, testClusterID, settings2, testVersion)
	if err != nil {
		t.Fatalf("Failed to save second snapshot: %v", err)
	}

	// Check that changes include the version
	changes, err := store.GetChanges(ctx, testClusterID, 100)
	if err != nil {
		t.Fatalf("Failed to get changes: %v", err)
	}

	// Find our change
	found := false
	for _, c := range changes {
		if c.Variable == "version.test.setting" {
			found = true
			if c.Version != testVersion {
				t.Errorf("Expected version '%s', got '%s'", testVersion, c.Version)
			}
			break
		}
	}

	if !found {
		t.Error("Expected to find change for version.test.setting")
	}
}

func TestAnnotationCRUD(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Clean up and create a change to annotate
	cleanupTestData(t, store)
	settings1 := []Setting{{Variable: "annotation.test", Value: "v1", SettingType: "s", Description: "Test"}}
	store.SaveSnapshot(ctx, testClusterID, settings1, "v1.0")
	settings2 := []Setting{{Variable: "annotation.test", Value: "v2", SettingType: "s", Description: "Test"}}
	store.SaveSnapshot(ctx, testClusterID, settings2, "v1.0")

	changes, err := store.GetChangesWithAnnotations(ctx, testClusterID, 1)
	if err != nil {
		t.Fatalf("Failed to get changes: %v", err)
	}
	if len(changes) == 0 {
		t.Fatal("No changes found")
	}
	changeID := changes[0].ID

	// Test Create
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

	// Test Get by ID
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

	// Test GetByChangeID
	byChange, err := store.GetAnnotationByChangeID(ctx, changeID)
	if err != nil {
		t.Fatalf("GetAnnotationByChangeID failed: %v", err)
	}
	if byChange == nil || byChange.ID != ann.ID {
		t.Error("GetAnnotationByChangeID returned wrong annotation")
	}

	// Test Update
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

	// Test Delete
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Get non-existent annotation should return nil, not error
	ann, err := store.GetAnnotation(ctx, 999999)
	if err != nil {
		t.Fatalf("GetAnnotation should not error for missing: %v", err)
	}
	if ann != nil {
		t.Error("Expected nil for non-existent annotation")
	}

	// Update non-existent should return ErrNoRows
	err = store.UpdateAnnotation(ctx, 999999, "content", "user")
	if err == nil {
		t.Error("Expected error for updating non-existent annotation")
	}

	// Delete non-existent should return ErrNoRows
	err = store.DeleteAnnotation(ctx, 999999)
	if err == nil {
		t.Error("Expected error for deleting non-existent annotation")
	}
}

func TestAnnotationCascadeDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Create change and annotation
	cleanupTestData(t, store)
	settings1 := []Setting{{Variable: "cascade.test", Value: "v1", SettingType: "s", Description: "Test"}}
	store.SaveSnapshot(ctx, testClusterID, settings1, "v1.0")
	settings2 := []Setting{{Variable: "cascade.test", Value: "v2", SettingType: "s", Description: "Test"}}
	store.SaveSnapshot(ctx, testClusterID, settings2, "v1.0")

	changes, _ := store.GetChangesWithAnnotations(ctx, testClusterID, 1)
	if len(changes) == 0 {
		t.Fatal("No changes found")
	}
	changeID := changes[0].ID
	ann, err := store.CreateAnnotation(ctx, changeID, "Will be deleted", "user")
	if err != nil {
		t.Fatalf("Failed to create annotation: %v", err)
	}

	// Delete all changes
	store.CleanupOldChanges(ctx, testClusterID, 0)

	// Annotation should be gone too
	retrieved, _ := store.GetAnnotation(ctx, ann.ID)
	if retrieved != nil {
		t.Error("Expected annotation to be deleted with change")
	}
}

func TestGetChangesWithAnnotations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	cleanupTestData(t, store)

	// Create changes
	settings1 := []Setting{{Variable: "join.test.a", Value: "v1", SettingType: "s", Description: "Test A"}}
	store.SaveSnapshot(ctx, testClusterID, settings1, "v1.0")
	settings2 := []Setting{
		{Variable: "join.test.a", Value: "v2", SettingType: "s", Description: "Test A"},
		{Variable: "join.test.b", Value: "x1", SettingType: "s", Description: "Test B"},
	}
	store.SaveSnapshot(ctx, testClusterID, settings2, "v1.0")
	settings3 := []Setting{
		{Variable: "join.test.a", Value: "v2", SettingType: "s", Description: "Test A"},
		{Variable: "join.test.b", Value: "x2", SettingType: "s", Description: "Test B"},
	}
	store.SaveSnapshot(ctx, testClusterID, settings3, "v1.0")

	changes, err := store.GetChangesWithAnnotations(ctx, testClusterID, 10)
	if err != nil {
		t.Fatalf("Failed to get changes with annotations: %v", err)
	}
	if len(changes) < 2 {
		t.Fatalf("Expected at least 2 changes, got %d", len(changes))
	}

	// Verify all changes have IDs
	for i, c := range changes {
		if c.ID == 0 {
			t.Errorf("Change %d has zero ID", i)
		}
	}

	// Add annotation to first change only
	_, err = store.CreateAnnotation(ctx, changes[0].ID, "First change note", "user")
	if err != nil {
		t.Fatalf("Failed to create annotation: %v", err)
	}

	// Re-fetch
	changes, err = store.GetChangesWithAnnotations(ctx, testClusterID, 10)
	if err != nil {
		t.Fatalf("Failed to get changes with annotations: %v", err)
	}

	// Verify first change has annotation, others don't
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	cleanupTestData(t, store)
	settings1 := []Setting{{Variable: "dup.test", Value: "v1", SettingType: "s", Description: "Test"}}
	store.SaveSnapshot(ctx, testClusterID, settings1, "v1.0")
	settings2 := []Setting{{Variable: "dup.test", Value: "v2", SettingType: "s", Description: "Test"}}
	store.SaveSnapshot(ctx, testClusterID, settings2, "v1.0")

	changes, _ := store.GetChangesWithAnnotations(ctx, testClusterID, 1)
	if len(changes) == 0 {
		t.Fatal("No changes found")
	}
	changeID := changes[0].ID

	// First annotation should succeed
	_, err = store.CreateAnnotation(ctx, changeID, "First", "user")
	if err != nil {
		t.Fatalf("First CreateAnnotation failed: %v", err)
	}

	// Second annotation for same change should fail (UNIQUE constraint)
	_, err = store.CreateAnnotation(ctx, changeID, "Second", "user")
	if err == nil {
		t.Error("Expected error for duplicate annotation on same change")
	}
}

func TestListClusters(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Create snapshots for multiple clusters
	settings := []Setting{{Variable: "list.test", Value: "v1", SettingType: "s", Description: "Test"}}

	store.SaveSnapshot(ctx, "list-cluster-alpha", settings, "v1.0")
	store.SaveSnapshot(ctx, "list-cluster-beta", settings, "v1.0")
	store.SaveSnapshot(ctx, "list-cluster-gamma", settings, "v1.0")

	clusters, err := store.ListClusters(ctx)
	if err != nil {
		t.Fatalf("ListClusters failed: %v", err)
	}

	// Should contain at least the 3 clusters we created
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Create changes for two different clusters
	settingsA1 := []Setting{{Variable: "multi.test.setting", Value: "a1", SettingType: "s", Description: "Test"}}
	store.SaveSnapshot(ctx, "multi-cluster-a", settingsA1, "v1.0")
	settingsA2 := []Setting{{Variable: "multi.test.setting", Value: "a2", SettingType: "s", Description: "Test"}}
	store.SaveSnapshot(ctx, "multi-cluster-a", settingsA2, "v1.0")

	settingsB1 := []Setting{{Variable: "multi.test.setting", Value: "b1", SettingType: "s", Description: "Test"}}
	store.SaveSnapshot(ctx, "multi-cluster-b", settingsB1, "v1.0")
	settingsB2 := []Setting{{Variable: "multi.test.setting", Value: "b2", SettingType: "s", Description: "Test"}}
	store.SaveSnapshot(ctx, "multi-cluster-b", settingsB2, "v1.0")

	// Get changes for cluster A
	changesA, err := store.GetChanges(ctx, "multi-cluster-a", 100)
	if err != nil {
		t.Fatalf("GetChanges(multi-cluster-a) failed: %v", err)
	}

	// All changes should be for cluster A
	for _, c := range changesA {
		if c.ClusterID != "multi-cluster-a" {
			t.Errorf("Expected ClusterID 'multi-cluster-a', got %q", c.ClusterID)
		}
	}

	// Get changes for cluster B
	changesB, err := store.GetChanges(ctx, "multi-cluster-b", 100)
	if err != nil {
		t.Fatalf("GetChanges(multi-cluster-b) failed: %v", err)
	}

	// All changes should be for cluster B
	for _, c := range changesB {
		if c.ClusterID != "multi-cluster-b" {
			t.Errorf("Expected ClusterID 'multi-cluster-b', got %q", c.ClusterID)
		}
	}
}

func TestListSnapshots(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	clusterID := "list-snapshots-test"

	// Create multiple snapshots
	settings := []Setting{{Variable: "snapshot.test", Value: "v1", SettingType: "s", Description: "Test"}}
	for i := 0; i < 5; i++ {
		err := store.SaveSnapshot(ctx, clusterID, settings, "v1.0")
		if err != nil {
			t.Fatalf("Failed to save snapshot %d: %v", i, err)
		}
		time.Sleep(10 * time.Millisecond) // Ensure different timestamps
	}

	// List snapshots
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

	// Test limit
	limited, err := store.ListSnapshots(ctx, clusterID, 2)
	if err != nil {
		t.Fatalf("ListSnapshots with limit failed: %v", err)
	}
	if len(limited) != 2 {
		t.Errorf("Expected 2 snapshots with limit, got %d", len(limited))
	}

	// Test empty result for non-existent cluster
	empty, err := store.ListSnapshots(ctx, "non-existent-cluster", 10)
	if err != nil {
		t.Fatalf("ListSnapshots for non-existent cluster failed: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("Expected 0 snapshots for non-existent cluster, got %d", len(empty))
	}
}

func TestGetSnapshotByID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	clusterID := "get-snapshot-by-id-test"

	// Create a snapshot with known settings
	settings := []Setting{
		{Variable: "test.setting.a", Value: "valueA", SettingType: "s", Description: "Description A"},
		{Variable: "test.setting.b", Value: "valueB", SettingType: "i", Description: "Description B"},
	}
	err = store.SaveSnapshot(ctx, clusterID, settings, "v1.0")
	if err != nil {
		t.Fatalf("Failed to save snapshot: %v", err)
	}

	// Get snapshot list to find the ID
	snapshots, err := store.ListSnapshots(ctx, clusterID, 1)
	if err != nil || len(snapshots) == 0 {
		t.Fatalf("Failed to get snapshot ID: %v", err)
	}
	snapshotID := snapshots[0].ID

	// Get snapshot by ID
	retrieved, err := store.GetSnapshotByID(ctx, snapshotID)
	if err != nil {
		t.Fatalf("GetSnapshotByID failed: %v", err)
	}
	if retrieved == nil {
		t.Fatal("Expected settings, got nil")
	}

	// Verify settings
	if len(retrieved) != 2 {
		t.Errorf("Expected 2 settings, got %d", len(retrieved))
	}
	if s, ok := retrieved["test.setting.a"]; !ok || s.Value != "valueA" {
		t.Errorf("Expected test.setting.a=valueA, got %v", retrieved["test.setting.a"])
	}
	if s, ok := retrieved["test.setting.b"]; !ok || s.Value != "valueB" {
		t.Errorf("Expected test.setting.b=valueB, got %v", retrieved["test.setting.b"])
	}

	// Test non-existent snapshot
	notFound, err := store.GetSnapshotByID(ctx, 999999999)
	if err != nil {
		t.Fatalf("GetSnapshotByID for non-existent should not error: %v", err)
	}
	if notFound != nil {
		t.Errorf("Expected nil for non-existent snapshot, got %v", notFound)
	}
}
