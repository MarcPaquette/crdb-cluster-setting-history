package cmd

import (
	"archive/zip"
	"context"
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
