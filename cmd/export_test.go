package cmd

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"crdb-cluster-history/storage"
)

const testClusterID = "default"

func getHistoryURL(t *testing.T) string {
	url := os.Getenv("HISTORY_DATABASE_URL")
	if url == "" {
		t.Skip("HISTORY_DATABASE_URL not set")
	}
	return url
}

func TestWriteChangesCSV(t *testing.T) {
	t.Parallel()
	changes := []storage.Change{
		{
			ClusterID:   "test-cluster-123",
			DetectedAt:  time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
			Variable:    "test.setting.one",
			Version:     "v25.1.0",
			OldValue:    "old",
			NewValue:    "new",
			Description: "Test setting",
		},
		{
			ClusterID:   "test-cluster-123",
			DetectedAt:  time.Date(2025, 1, 15, 11, 0, 0, 0, time.UTC),
			Variable:    "test.setting.two",
			Version:     "v25.1.0",
			OldValue:    "",
			NewValue:    "added",
			Description: "New setting",
		},
	}

	var buf bytes.Buffer
	err := storage.WriteChangesCSV(&buf, changes)
	if err != nil {
		t.Fatalf("WriteChangesCSV failed: %v", err)
	}

	reader := csv.NewReader(&buf)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to parse CSV: %v", err)
	}

	if len(records) < 1 {
		t.Fatal("Expected at least header row")
	}
	expectedHeaders := []string{"cluster_id", "detected_at", "variable", "version", "old_value", "new_value", "description"}
	for i, h := range expectedHeaders {
		if i >= len(records[0]) || records[0][i] != h {
			t.Errorf("Expected header[%d] = %s, got %s", i, h, records[0][i])
		}
	}

	if len(records) != 3 {
		t.Errorf("Expected 3 rows (1 header + 2 data), got %d", len(records))
	}

	if records[1][0] != "test-cluster-123" {
		t.Errorf("Expected cluster_id 'test-cluster-123', got '%s'", records[1][0])
	}
	if records[1][2] != "test.setting.one" {
		t.Errorf("Expected variable 'test.setting.one', got '%s'", records[1][2])
	}
	if records[1][3] != "v25.1.0" {
		t.Errorf("Expected version 'v25.1.0', got '%s'", records[1][3])
	}
}

func TestWriteChangesCSVEmptyChanges(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	err := storage.WriteChangesCSV(&buf, []storage.Change{})
	if err != nil {
		t.Fatalf("WriteChangesCSV failed: %v", err)
	}

	reader := csv.NewReader(&buf)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to parse CSV: %v", err)
	}

	if len(records) != 1 {
		t.Errorf("Expected 1 row (header only), got %d", len(records))
	}
}

func TestRunExport(t *testing.T) {
	historyURL := getHistoryURL(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := storage.New(ctx, historyURL)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	store.CleanupOldChanges(ctx, testClusterID, 0)

	settings1 := []storage.Setting{
		{Variable: "export.cli.test", Value: "original", SettingType: "s", Description: "CLI export test"},
	}
	err = store.SaveSnapshot(ctx, testClusterID, settings1, "v25.1.0")
	if err != nil {
		t.Fatalf("Failed to save first snapshot: %v", err)
	}

	settings2 := []storage.Setting{
		{Variable: "export.cli.test", Value: "modified", SettingType: "s", Description: "CLI export test"},
	}
	err = store.SaveSnapshot(ctx, testClusterID, settings2, "v25.1.0")
	if err != nil {
		t.Fatalf("Failed to save second snapshot: %v", err)
	}

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test-export.zip")

	cfg := ExportConfig{
		HistoryURL: historyURL,
		OutputPath: outputPath,
	}

	err = RunExport(ctx, cfg)
	if err != nil {
		t.Fatalf("RunExport failed: %v", err)
	}

	zipFile, err := os.Open(outputPath)
	if err != nil {
		t.Fatalf("Failed to open zip: %v", err)
	}
	defer zipFile.Close()

	stat, err := zipFile.Stat()
	if err != nil {
		t.Fatalf("Failed to stat zip: %v", err)
	}
	zipReader, err := zip.NewReader(zipFile, stat.Size())
	if err != nil {
		t.Fatalf("Failed to read zip: %v", err)
	}

	if len(zipReader.File) == 0 {
		t.Fatal("Expected at least one file in zip")
	}

	csvFile := zipReader.File[0]
	if !strings.HasSuffix(csvFile.Name, ".csv") {
		t.Errorf("Expected CSV file, got %s", csvFile.Name)
	}
}

func TestRunExportDefaultPath(t *testing.T) {
	historyURL := getHistoryURL(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := storage.New(ctx, historyURL)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	store.CleanupOldChanges(ctx, testClusterID, 0)

	settings1 := []storage.Setting{
		{Variable: "export.default.test", Value: "v1", SettingType: "s", Description: "Test"},
	}
	if err := store.SaveSnapshot(ctx, testClusterID, settings1, "v25.1.0"); err != nil {
		t.Fatalf("Failed to save first snapshot: %v", err)
	}

	settings2 := []storage.Setting{
		{Variable: "export.default.test", Value: "v2", SettingType: "s", Description: "Test"},
	}
	if err := store.SaveSnapshot(ctx, testClusterID, settings2, "v25.1.0"); err != nil {
		t.Fatalf("Failed to save second snapshot: %v", err)
	}

	// Chdir to temp dir so the default output path doesn't pollute the workspace
	originalDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get working directory: %v", err)
	}
	tmpDir := t.TempDir()
	os.Chdir(tmpDir)
	defer os.Chdir(originalDir)

	cfg := ExportConfig{
		HistoryURL: historyURL,
		OutputPath: "",
	}

	err = RunExport(ctx, cfg)
	if err != nil {
		t.Fatalf("RunExport failed: %v", err)
	}

	files, err := filepath.Glob("crdb-cluster-history-export-*.zip")
	if err != nil {
		t.Fatalf("Failed to glob: %v", err)
	}

	if len(files) == 0 {
		t.Error("Expected default output file to be created")
	}
}
