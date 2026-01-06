package storage

import (
	"context"
	"os"
	"testing"
	"time"
)

func getTestDB(t *testing.T) string {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		url = os.Getenv("HISTORY_DATABASE_URL")
	}
	if url == "" {
		t.Skip("TEST_DATABASE_URL or HISTORY_DATABASE_URL not set")
	}
	return url
}

func TestNew(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

	err = store.SaveSnapshot(ctx, settings)
	if err != nil {
		t.Fatalf("Failed to save snapshot: %v", err)
	}

	// Get the snapshot
	snapshot, err := store.GetLatestSnapshot(ctx)
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
	err = store.SaveSnapshot(ctx, settings1)
	if err != nil {
		t.Fatalf("Failed to save first snapshot: %v", err)
	}

	// Second snapshot with changed value
	settings2 := []Setting{
		{Variable: "change.test.setting", Value: "modified", SettingType: "s", Description: "Test"},
	}
	err = store.SaveSnapshot(ctx, settings2)
	if err != nil {
		t.Fatalf("Failed to save second snapshot: %v", err)
	}

	// Check for changes
	changes, err := store.GetChanges(ctx, 100)
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
	err = store.SaveSnapshot(ctx, settings1)
	if err != nil {
		t.Fatalf("Failed to save first snapshot: %v", err)
	}

	// Second snapshot with new setting
	uniqueName := "new.setting." + time.Now().Format("20060102150405")
	settings2 := []Setting{
		{Variable: "existing.setting", Value: "exists", SettingType: "s", Description: "Test"},
		{Variable: uniqueName, Value: "new", SettingType: "s", Description: "New setting"},
	}
	err = store.SaveSnapshot(ctx, settings2)
	if err != nil {
		t.Fatalf("Failed to save second snapshot: %v", err)
	}

	// Check for changes
	changes, err := store.GetChanges(ctx, 100)
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
	err = store.SaveSnapshot(ctx, settings1)
	if err != nil {
		t.Fatalf("Failed to save first snapshot: %v", err)
	}

	// Second snapshot without the setting
	settings2 := []Setting{
		{Variable: "keeper.setting", Value: "stays", SettingType: "s", Description: "Test"},
	}
	err = store.SaveSnapshot(ctx, settings2)
	if err != nil {
		t.Fatalf("Failed to save second snapshot: %v", err)
	}

	// Check for changes
	changes, err := store.GetChanges(ctx, 100)
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
	changes, err := store.GetChanges(ctx, 5)
	if err != nil {
		t.Fatalf("Failed to get changes: %v", err)
	}

	if len(changes) > 5 {
		t.Errorf("Expected at most 5 changes, got %d", len(changes))
	}
}
