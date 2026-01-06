package web

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"crdb-cluster-history/storage"
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

	store, err := storage.New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	server, err := New(store)
	if err != nil {
		t.Fatalf("Failed to create web server: %v", err)
	}

	if server == nil {
		t.Fatal("Expected non-nil server")
	}
}

func TestHandler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := storage.New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	server, err := New(store)
	if err != nil {
		t.Fatalf("Failed to create web server: %v", err)
	}

	handler := server.Handler()
	if handler == nil {
		t.Fatal("Expected non-nil handler")
	}
}

func TestHandleIndex(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := storage.New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	server, err := New(store)
	if err != nil {
		t.Fatalf("Failed to create web server: %v", err)
	}

	// Create a test request
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	// Serve the request
	server.Handler().ServeHTTP(w, req)

	// Check response
	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	// Check content type
	contentType := resp.Header.Get("Content-Type")
	if !strings.Contains(contentType, "text/html") {
		t.Errorf("Expected text/html content type, got %s", contentType)
	}

	// Check body contains expected elements
	body := w.Body.String()
	if !strings.Contains(body, "CockroachDB Cluster Settings History") {
		t.Error("Expected page title in response")
	}
	if !strings.Contains(body, "Auto-refresh") {
		t.Error("Expected Auto-refresh option in response")
	}
}

func TestHandleIndexWithChanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := storage.New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Create some test data
	settings1 := []storage.Setting{
		{Variable: "web.test.setting", Value: "original", SettingType: "s", Description: "Test"},
	}
	err = store.SaveSnapshot(ctx, settings1, "v1.0.0")
	if err != nil {
		t.Fatalf("Failed to save first snapshot: %v", err)
	}

	settings2 := []storage.Setting{
		{Variable: "web.test.setting", Value: "modified", SettingType: "s", Description: "Test"},
	}
	err = store.SaveSnapshot(ctx, settings2, "v1.0.0")
	if err != nil {
		t.Fatalf("Failed to save second snapshot: %v", err)
	}

	server, err := New(store)
	if err != nil {
		t.Fatalf("Failed to create web server: %v", err)
	}

	// Create a test request
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	// Serve the request
	server.Handler().ServeHTTP(w, req)

	// Check response contains table
	body := w.Body.String()
	if !strings.Contains(body, "<table>") {
		t.Error("Expected table in response when changes exist")
	}
	if !strings.Contains(body, "web.test.setting") {
		t.Error("Expected test setting in response")
	}
}

func TestHandleIndexNoChanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := storage.New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	server, err := New(store)
	if err != nil {
		t.Fatalf("Failed to create web server: %v", err)
	}

	// Create a test request
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	// Serve the request
	server.Handler().ServeHTTP(w, req)

	// Response should still be OK even with no changes
	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}
}

func TestHandleHealth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := storage.New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	server, err := New(store)
	if err != nil {
		t.Fatalf("Failed to create web server: %v", err)
	}

	// Create a test request to /health
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	// Serve the request
	server.Handler().ServeHTTP(w, req)

	// Check response
	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	// Check body contains "ok"
	body := w.Body.String()
	if body != "ok" {
		t.Errorf("Expected body 'ok', got '%s'", body)
	}
}

func TestHandleExport(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := storage.New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	server, err := New(store)
	if err != nil {
		t.Fatalf("Failed to create web server: %v", err)
	}

	// Create a test request to /export
	req := httptest.NewRequest(http.MethodGet, "/export", nil)
	w := httptest.NewRecorder()

	// Serve the request
	server.Handler().ServeHTTP(w, req)

	// Check response
	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	// Check content type is zip
	contentType := resp.Header.Get("Content-Type")
	if contentType != "application/zip" {
		t.Errorf("Expected application/zip content type, got %s", contentType)
	}

	// Check content disposition header
	disposition := resp.Header.Get("Content-Disposition")
	if !strings.Contains(disposition, "attachment") {
		t.Error("Expected Content-Disposition to contain 'attachment'")
	}
	if !strings.Contains(disposition, ".zip") {
		t.Error("Expected Content-Disposition to contain '.zip'")
	}
}

func TestHandleExportZipContainsCSV(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := storage.New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	server, err := New(store)
	if err != nil {
		t.Fatalf("Failed to create web server: %v", err)
	}

	// Create a test request to /export
	req := httptest.NewRequest(http.MethodGet, "/export", nil)
	w := httptest.NewRecorder()

	// Serve the request
	server.Handler().ServeHTTP(w, req)

	// Read the zip file
	body := w.Body.Bytes()
	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("Failed to read zip: %v", err)
	}

	// Check there's at least one file in the zip
	if len(zipReader.File) == 0 {
		t.Fatal("Expected at least one file in zip")
	}

	// Check the first file is a CSV
	csvFile := zipReader.File[0]
	if !strings.HasSuffix(csvFile.Name, ".csv") {
		t.Errorf("Expected CSV file, got %s", csvFile.Name)
	}
}

func TestHandleExportWithChanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := storage.New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Clean up any existing data first
	store.CleanupOldChanges(ctx, 0)

	// Create some test data
	settings1 := []storage.Setting{
		{Variable: "export.test.setting", Value: "original", SettingType: "s", Description: "Export test"},
	}
	err = store.SaveSnapshot(ctx, settings1, "v25.1.0")
	if err != nil {
		t.Fatalf("Failed to save first snapshot: %v", err)
	}

	settings2 := []storage.Setting{
		{Variable: "export.test.setting", Value: "modified", SettingType: "s", Description: "Export test"},
	}
	err = store.SaveSnapshot(ctx, settings2, "v25.1.0")
	if err != nil {
		t.Fatalf("Failed to save second snapshot: %v", err)
	}

	server, err := New(store)
	if err != nil {
		t.Fatalf("Failed to create web server: %v", err)
	}

	// Create a test request to /export
	req := httptest.NewRequest(http.MethodGet, "/export", nil)
	w := httptest.NewRecorder()

	// Serve the request
	server.Handler().ServeHTTP(w, req)

	// Read the zip file
	body := w.Body.Bytes()
	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("Failed to read zip: %v", err)
	}

	// Open the CSV file
	csvFile := zipReader.File[0]
	rc, err := csvFile.Open()
	if err != nil {
		t.Fatalf("Failed to open CSV: %v", err)
	}
	defer rc.Close()

	// Read CSV content
	csvContent, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("Failed to read CSV content: %v", err)
	}

	// Parse CSV
	csvReader := csv.NewReader(bytes.NewReader(csvContent))
	records, err := csvReader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to parse CSV: %v", err)
	}

	// Check header
	if len(records) < 1 {
		t.Fatal("Expected at least header row in CSV")
	}
	header := records[0]
	expectedHeaders := []string{"cluster_id", "detected_at", "variable", "version", "old_value", "new_value", "description"}
	for i, h := range expectedHeaders {
		if i >= len(header) || header[i] != h {
			t.Errorf("Expected header[%d] = %s, got %s", i, h, header[i])
		}
	}

	// Check that our test data is in the export
	found := false
	for _, record := range records[1:] {
		if len(record) >= 3 && record[2] == "export.test.setting" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected export.test.setting in CSV export")
	}
}

func TestHandleExportWithClusterID(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := storage.New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Set a test cluster ID
	testClusterID := "test-cluster-export-12345"
	err = store.SetClusterID(ctx, testClusterID)
	if err != nil {
		t.Fatalf("Failed to set cluster ID: %v", err)
	}

	server, err := New(store)
	if err != nil {
		t.Fatalf("Failed to create web server: %v", err)
	}

	// Create a test request to /export
	req := httptest.NewRequest(http.MethodGet, "/export", nil)
	w := httptest.NewRecorder()

	// Serve the request
	server.Handler().ServeHTTP(w, req)

	// Read the zip file
	body := w.Body.Bytes()
	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("Failed to read zip: %v", err)
	}

	// Check the CSV filename contains the cluster ID
	csvFile := zipReader.File[0]
	if !strings.Contains(csvFile.Name, testClusterID) {
		t.Errorf("Expected CSV filename to contain cluster ID, got %s", csvFile.Name)
	}
}
