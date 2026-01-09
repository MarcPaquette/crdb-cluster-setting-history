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

// testClusterID is used for all tests
const testClusterID = "default"

func getHistoryURL(t *testing.T) string {
	url := os.Getenv("HISTORY_DATABASE_URL")
	if url == "" {
		t.Skip("HISTORY_DATABASE_URL not set")
	}
	return url
}

func TestWriteChangesCSV(t *testing.T) {
	changes := []storage.Change{
		{
			DetectedAt:  time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
			Variable:    "test.setting.one",
			Version:     "v25.1.0",
			OldValue:    "old",
			NewValue:    "new",
			Description: "Test setting",
		},
		{
			DetectedAt:  time.Date(2025, 1, 15, 11, 0, 0, 0, time.UTC),
			Variable:    "test.setting.two",
			Version:     "v25.1.0",
			OldValue:    "",
			NewValue:    "added",
			Description: "New setting",
		},
	}

	var buf bytes.Buffer
	err := storage.WriteChangesCSV(&buf, "test-cluster-123", changes)
	if err != nil {
		t.Fatalf("WriteChangesCSV failed: %v", err)
	}

	// Parse the CSV
	reader := csv.NewReader(&buf)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to parse CSV: %v", err)
	}

	// Check header
	if len(records) < 1 {
		t.Fatal("Expected at least header row")
	}
	expectedHeaders := []string{"cluster_id", "detected_at", "variable", "version", "old_value", "new_value", "description"}
	for i, h := range expectedHeaders {
		if i >= len(records[0]) || records[0][i] != h {
			t.Errorf("Expected header[%d] = %s, got %s", i, h, records[0][i])
		}
	}

	// Check we have 2 data rows plus header
	if len(records) != 3 {
		t.Errorf("Expected 3 rows (1 header + 2 data), got %d", len(records))
	}

	// Check first data row
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
	var buf bytes.Buffer
	err := storage.WriteChangesCSV(&buf, "test-cluster", []storage.Change{})
	if err != nil {
		t.Fatalf("WriteChangesCSV failed: %v", err)
	}

	// Parse the CSV - should still have header
	reader := csv.NewReader(&buf)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to parse CSV: %v", err)
	}

	// Should have just the header
	if len(records) != 1 {
		t.Errorf("Expected 1 row (header only), got %d", len(records))
	}
}

func TestRunExport(t *testing.T) {
	sourceURL := getAdminURL(t)
	historyURL := getHistoryURL(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create test data in history database
	store, err := storage.New(ctx, historyURL)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Clean up any existing data
	store.CleanupOldChanges(ctx, testClusterID, 0)

	// Create some test changes
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

	// Create temp file for output
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test-export.zip")

	cfg := ExportConfig{
		SourceURL:  sourceURL,
		HistoryURL: historyURL,
		OutputPath: outputPath,
	}

	err = RunExport(ctx, cfg)
	if err != nil {
		t.Fatalf("RunExport failed: %v", err)
	}

	// Verify the zip file was created
	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		t.Fatal("Expected output file to be created")
	}

	// Open and verify the zip
	zipFile, err := os.Open(outputPath)
	if err != nil {
		t.Fatalf("Failed to open zip: %v", err)
	}
	defer zipFile.Close()

	stat, _ := zipFile.Stat()
	zipReader, err := zip.NewReader(zipFile, stat.Size())
	if err != nil {
		t.Fatalf("Failed to read zip: %v", err)
	}

	// Check there's a CSV file
	if len(zipReader.File) == 0 {
		t.Fatal("Expected at least one file in zip")
	}

	csvFile := zipReader.File[0]
	if !strings.HasSuffix(csvFile.Name, ".csv") {
		t.Errorf("Expected CSV file, got %s", csvFile.Name)
	}
}

func TestWriteChangesCSVWithVersion(t *testing.T) {
	changes := []storage.Change{
		{
			DetectedAt:  time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
			Variable:    "version.test.setting",
			Version:     "v25.2.0",
			OldValue:    "old",
			NewValue:    "new",
			Description: "Version test",
		},
	}

	var buf bytes.Buffer
	err := storage.WriteChangesCSV(&buf, "cluster-with-version", changes)
	if err != nil {
		t.Fatalf("WriteChangesCSV failed: %v", err)
	}

	// Parse the CSV
	reader := csv.NewReader(&buf)
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to parse CSV: %v", err)
	}

	// Check header includes version
	if len(records) < 2 {
		t.Fatal("Expected header and data rows")
	}

	// Find version column index
	versionIdx := -1
	for i, h := range records[0] {
		if h == "version" {
			versionIdx = i
			break
		}
	}
	if versionIdx == -1 {
		t.Fatal("Expected 'version' column in CSV header")
	}

	// Check version value in data row
	if records[1][versionIdx] != "v25.2.0" {
		t.Errorf("Expected version 'v25.2.0', got '%s'", records[1][versionIdx])
	}
}

func TestRunExportDefaultPath(t *testing.T) {
	sourceURL := getAdminURL(t)
	historyURL := getHistoryURL(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create test data
	store, err := storage.New(ctx, historyURL)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	store.CleanupOldChanges(ctx, testClusterID, 0)

	settings1 := []storage.Setting{
		{Variable: "export.default.test", Value: "v1", SettingType: "s", Description: "Test"},
	}
	store.SaveSnapshot(ctx, testClusterID, settings1, "v25.1.0")

	settings2 := []storage.Setting{
		{Variable: "export.default.test", Value: "v2", SettingType: "s", Description: "Test"},
	}
	store.SaveSnapshot(ctx, testClusterID, settings2, "v25.1.0")

	// Change to temp directory to avoid polluting workspace
	originalDir, _ := os.Getwd()
	tmpDir := t.TempDir()
	os.Chdir(tmpDir)
	defer os.Chdir(originalDir)

	cfg := ExportConfig{
		SourceURL:  sourceURL,
		HistoryURL: historyURL,
		OutputPath: "", // Empty - should use default
	}

	err = RunExport(ctx, cfg)
	if err != nil {
		t.Fatalf("RunExport failed: %v", err)
	}

	// Look for the generated file
	files, err := filepath.Glob("crdb-cluster-history-export-*.zip")
	if err != nil {
		t.Fatalf("Failed to glob: %v", err)
	}

	if len(files) == 0 {
		t.Error("Expected default output file to be created")
	}
}
