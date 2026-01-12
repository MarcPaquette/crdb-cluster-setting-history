package web

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"crdb-cluster-history/config"
	"crdb-cluster-history/storage"
)

// testClusterID is used for all tests
const testClusterID = "default"

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
	err = store.SaveSnapshot(ctx, testClusterID, settings1, "v1.0.0")
	if err != nil {
		t.Fatalf("Failed to save first snapshot: %v", err)
	}

	settings2 := []storage.Setting{
		{Variable: "web.test.setting", Value: "modified", SettingType: "s", Description: "Test"},
	}
	err = store.SaveSnapshot(ctx, testClusterID, settings2, "v1.0.0")
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
	store.CleanupOldChanges(ctx, testClusterID, 0)

	// Create some test data
	settings1 := []storage.Setting{
		{Variable: "export.test.setting", Value: "original", SettingType: "s", Description: "Export test"},
	}
	err = store.SaveSnapshot(ctx, testClusterID, settings1, "v25.1.0")
	if err != nil {
		t.Fatalf("Failed to save first snapshot: %v", err)
	}

	settings2 := []storage.Setting{
		{Variable: "export.test.setting", Value: "modified", SettingType: "s", Description: "Export test"},
	}
	err = store.SaveSnapshot(ctx, testClusterID, settings2, "v25.1.0")
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

	// Set a test source cluster ID
	sourceClusterID := "test-cluster-export-12345"
	err = store.SetSourceClusterID(ctx, testClusterID, sourceClusterID)
	if err != nil {
		t.Fatalf("Failed to set source cluster ID: %v", err)
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

	// Check the CSV filename contains the source cluster ID
	csvFile := zipReader.File[0]
	if !strings.Contains(csvFile.Name, sourceClusterID) {
		t.Errorf("Expected CSV filename to contain source cluster ID, got %s", csvFile.Name)
	}
}

// cleanupAnnotationTestData cleans up test data for annotation tests
func cleanupAnnotationTestData(t *testing.T, store *storage.Store, ctx context.Context) {
	t.Helper()
	store.CleanupOldChanges(ctx, testClusterID, 0)
}

// createTestChange creates a change and returns its ID
func createTestChange(t *testing.T, store *storage.Store, ctx context.Context) int64 {
	settings1 := []storage.Setting{{Variable: "api.test.setting", Value: "v1", SettingType: "s", Description: "API Test"}}
	store.SaveSnapshot(ctx, testClusterID, settings1, "v1.0")
	settings2 := []storage.Setting{{Variable: "api.test.setting", Value: "v2", SettingType: "s", Description: "API Test"}}
	store.SaveSnapshot(ctx, testClusterID, settings2, "v1.0")

	changes, err := store.GetChangesWithAnnotations(ctx, testClusterID, 1)
	if err != nil || len(changes) == 0 {
		t.Fatal("Failed to create test change")
	}
	return changes[0].ID
}

func TestAnnotationAPI_Create(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := storage.New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	cleanupAnnotationTestData(t, store, ctx)
	changeID := createTestChange(t, store, ctx)

	server, err := New(store)
	if err != nil {
		t.Fatalf("Failed to create web server: %v", err)
	}

	// Test POST /api/annotations
	body := strings.NewReader(`{"change_id":` + itoa(changeID) + `,"content":"API test note"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/annotations", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected 201, got %d: %s", w.Code, w.Body.String())
	}

	// Verify response contains the annotation
	respBody := w.Body.String()
	if !strings.Contains(respBody, "API test note") {
		t.Errorf("Expected response to contain 'API test note', got %s", respBody)
	}
	if !strings.Contains(respBody, `"change_id"`) {
		t.Errorf("Expected response to contain change_id, got %s", respBody)
	}
}

func TestAnnotationAPI_CreateWithBasicAuth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := storage.New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	cleanupAnnotationTestData(t, store, ctx)
	changeID := createTestChange(t, store, ctx)

	server, err := New(store)
	if err != nil {
		t.Fatalf("Failed to create web server: %v", err)
	}

	body := strings.NewReader(`{"change_id":` + itoa(changeID) + `,"content":"Auth test note"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/annotations", body)
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("testadmin", "password")
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected 201, got %d: %s", w.Code, w.Body.String())
	}

	// Verify created_by contains the username
	respBody := w.Body.String()
	if !strings.Contains(respBody, "testadmin") {
		t.Errorf("Expected response to contain 'testadmin' as created_by, got %s", respBody)
	}
}

func TestAnnotationAPI_GetNotFound(t *testing.T) {
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

	req := httptest.NewRequest(http.MethodGet, "/api/annotations/999999", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected 404, got %d", w.Code)
	}
}

func TestAnnotationAPI_InvalidJSON(t *testing.T) {
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

	req := httptest.NewRequest(http.MethodPost, "/api/annotations", strings.NewReader("not json"))
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d", w.Code)
	}
}

func TestAnnotationAPI_EmptyContent(t *testing.T) {
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

	req := httptest.NewRequest(http.MethodPost, "/api/annotations",
		strings.NewReader(`{"change_id":1,"content":""}`))
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400 for empty content, got %d", w.Code)
	}
}

func TestAnnotationAPI_MissingChangeID(t *testing.T) {
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

	req := httptest.NewRequest(http.MethodPost, "/api/annotations",
		strings.NewReader(`{"content":"no change id"}`))
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400 for missing change_id, got %d", w.Code)
	}
}

func TestAnnotationAPI_Update(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := storage.New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	cleanupAnnotationTestData(t, store, ctx)
	changeID := createTestChange(t, store, ctx)

	// Create an annotation first
	ann, err := store.CreateAnnotation(ctx, changeID, "Original content", "user1")
	if err != nil {
		t.Fatalf("Failed to create annotation: %v", err)
	}

	server, err := New(store)
	if err != nil {
		t.Fatalf("Failed to create web server: %v", err)
	}

	// Update via API
	body := strings.NewReader(`{"content":"Updated content"}`)
	req := httptest.NewRequest(http.MethodPut, "/api/annotations/"+itoa(ann.ID), body)
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("user2", "password")
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// Verify response contains updated content and updated_by
	respBody := w.Body.String()
	if !strings.Contains(respBody, "Updated content") {
		t.Errorf("Expected 'Updated content' in response, got %s", respBody)
	}
	if !strings.Contains(respBody, "user2") {
		t.Errorf("Expected 'user2' as updated_by in response, got %s", respBody)
	}
}

func TestAnnotationAPI_Delete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := storage.New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	cleanupAnnotationTestData(t, store, ctx)
	changeID := createTestChange(t, store, ctx)

	// Create an annotation first
	ann, err := store.CreateAnnotation(ctx, changeID, "To be deleted", "user")
	if err != nil {
		t.Fatalf("Failed to create annotation: %v", err)
	}

	server, err := New(store)
	if err != nil {
		t.Fatalf("Failed to create web server: %v", err)
	}

	// Delete via API
	req := httptest.NewRequest(http.MethodDelete, "/api/annotations/"+itoa(ann.ID), nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("Expected 204, got %d: %s", w.Code, w.Body.String())
	}

	// Verify annotation is gone
	deleted, _ := store.GetAnnotation(ctx, ann.ID)
	if deleted != nil {
		t.Error("Expected annotation to be deleted")
	}
}

func TestAnnotationAPI_InvalidID(t *testing.T) {
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

	req := httptest.NewRequest(http.MethodGet, "/api/annotations/notanumber", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d", w.Code)
	}
}

func TestAnnotationAPI_MethodNotAllowed(t *testing.T) {
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

	// GET on /api/annotations (collection) should not be allowed
	req := httptest.NewRequest(http.MethodGet, "/api/annotations", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405, got %d", w.Code)
	}
}

// itoa is a helper to convert int64 to string
func itoa(i int64) string {
	return fmt.Sprintf("%d", i)
}

func TestHandleAPIClusters(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := storage.New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Create server with clusters configured
	clusters := []struct {
		ID   string
		Name string
	}{
		{ID: "prod", Name: "Production"},
		{ID: "staging", Name: "Staging"},
	}

	// We need to import config, but since we're testing the web package
	// we'll test that the endpoint returns an empty array when no clusters configured
	server, err := New(store)
	if err != nil {
		t.Fatalf("Failed to create web server: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/clusters", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", w.Code)
	}

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Expected application/json, got %s", contentType)
	}

	// Should return empty array when no clusters configured
	body := w.Body.String()
	if body != "[]\n" && body != "[]" {
		// If not empty, at least verify it's valid JSON array
		if body[0] != '[' {
			t.Errorf("Expected JSON array, got %s", body)
		}
	}

	_ = clusters // Suppress unused warning
}

func TestHandleAPIClustersMethodNotAllowed(t *testing.T) {
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

	req := httptest.NewRequest(http.MethodPost, "/api/clusters", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405, got %d", w.Code)
	}
}

func TestHandleCompare(t *testing.T) {
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

	req := httptest.NewRequest(http.MethodGet, "/compare", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", w.Code)
	}

	contentType := w.Header().Get("Content-Type")
	if !strings.Contains(contentType, "text/html") {
		t.Errorf("Expected text/html, got %s", contentType)
	}

	body := w.Body.String()
	if !strings.Contains(body, "Compare Cluster Settings") {
		t.Error("Expected page title in response")
	}
}

func TestHandleAPICompare(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := storage.New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Create test data for two clusters
	settings1 := []storage.Setting{
		{Variable: "compare.test.shared", Value: "same", SettingType: "s", Description: "Shared setting"},
		{Variable: "compare.test.different", Value: "value1", SettingType: "s", Description: "Different setting"},
		{Variable: "compare.test.only1", Value: "only-in-1", SettingType: "s", Description: "Only in cluster1"},
	}
	store.SaveSnapshot(ctx, "compare-cluster1", settings1, "v1.0")

	settings2 := []storage.Setting{
		{Variable: "compare.test.shared", Value: "same", SettingType: "s", Description: "Shared setting"},
		{Variable: "compare.test.different", Value: "value2", SettingType: "s", Description: "Different setting"},
		{Variable: "compare.test.only2", Value: "only-in-2", SettingType: "s", Description: "Only in cluster2"},
	}
	store.SaveSnapshot(ctx, "compare-cluster2", settings2, "v1.0")

	server, err := New(store)
	if err != nil {
		t.Fatalf("Failed to create web server: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/compare?cluster1=compare-cluster1&cluster2=compare-cluster2", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d: %s", w.Code, w.Body.String())
	}

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Expected application/json, got %s", contentType)
	}

	body := w.Body.String()
	// Should contain the different setting
	if !strings.Contains(body, "compare.test.different") {
		t.Error("Expected different setting in response")
	}
	// Should contain cluster1 only setting
	if !strings.Contains(body, "compare.test.only1") {
		t.Error("Expected cluster1-only setting in response")
	}
	// Should contain cluster2 only setting
	if !strings.Contains(body, "compare.test.only2") {
		t.Error("Expected cluster2-only setting in response")
	}
}

func TestHandleAPICompareMissingParams(t *testing.T) {
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

	// Missing both params
	req := httptest.NewRequest(http.MethodGet, "/api/compare", nil)
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400 for missing params, got %d", w.Code)
	}

	// Missing cluster2
	req = httptest.NewRequest(http.MethodGet, "/api/compare?cluster1=test", nil)
	w = httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400 for missing cluster2, got %d", w.Code)
	}

	// Same cluster
	req = httptest.NewRequest(http.MethodGet, "/api/compare?cluster1=test&cluster2=test", nil)
	w = httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400 for same clusters, got %d", w.Code)
	}
}

func TestHandleIndexWithClusterParam(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := storage.New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Create test data for specific cluster
	settings := []storage.Setting{
		{Variable: "cluster.param.test", Value: "test-value", SettingType: "s", Description: "Test"},
	}
	store.SaveSnapshot(ctx, "param-test-cluster", settings, "v1.0")

	settings2 := []storage.Setting{
		{Variable: "cluster.param.test", Value: "changed", SettingType: "s", Description: "Test"},
	}
	store.SaveSnapshot(ctx, "param-test-cluster", settings2, "v1.0")

	server, err := New(store, WithDefaultClusterID("other-cluster"))
	if err != nil {
		t.Fatalf("Failed to create web server: %v", err)
	}

	// Request with cluster param should show that cluster's data
	req := httptest.NewRequest(http.MethodGet, "/?cluster=param-test-cluster", nil)
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", w.Code)
	}

	body := w.Body.String()
	if !strings.Contains(body, "cluster.param.test") {
		t.Error("Expected test setting in response")
	}
}

func TestHandleAPISnapshots(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := storage.New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	clusterID := "api-snapshots-test-" + time.Now().Format("20060102150405.000")

	// Create some test snapshots
	settings := []storage.Setting{
		{Variable: "api.snapshot.test", Value: "v1", SettingType: "s", Description: "Test"},
	}
	for range 3 {
		err = store.SaveSnapshot(ctx, clusterID, settings, "v1.0")
		if err != nil {
			t.Fatalf("Failed to save snapshot: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	server, err := New(store, WithDefaultClusterID(clusterID))
	if err != nil {
		t.Fatalf("Failed to create web server: %v", err)
	}

	// Test without cluster param (uses default)
	req := httptest.NewRequest(http.MethodGet, "/api/snapshots", nil)
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", w.Code)
	}

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Expected application/json, got %s", contentType)
	}

	var snapshots []storage.SnapshotInfo
	if err := json.Unmarshal(w.Body.Bytes(), &snapshots); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}
	if len(snapshots) < 3 {
		t.Errorf("Expected at least 3 snapshots, got %d", len(snapshots))
	}

	// Test with explicit cluster param
	req = httptest.NewRequest(http.MethodGet, "/api/snapshots?cluster="+clusterID, nil)
	w = httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", w.Code)
	}

	// Test with limit
	req = httptest.NewRequest(http.MethodGet, "/api/snapshots?cluster="+clusterID+"&limit=2", nil)
	w = httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", w.Code)
	}

	var limited []storage.SnapshotInfo
	if err := json.Unmarshal(w.Body.Bytes(), &limited); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}
	if len(limited) != 2 {
		t.Errorf("Expected 2 snapshots with limit=2, got %d", len(limited))
	}

	// Test method not allowed
	req = httptest.NewRequest(http.MethodPost, "/api/snapshots", nil)
	w = httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405, got %d", w.Code)
	}
}

func TestHandleAPICompareSnapshots(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := storage.New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	clusterID := "compare-snapshots-test-" + time.Now().Format("20060102150405.000")

	// Create first snapshot
	settings1 := []storage.Setting{
		{Variable: "compare.shared", Value: "same", SettingType: "s", Description: "Shared"},
		{Variable: "compare.different", Value: "val1", SettingType: "s", Description: "Different"},
		{Variable: "compare.only1", Value: "only-in-1", SettingType: "s", Description: "Only in 1"},
	}
	store.SaveSnapshot(ctx, clusterID, settings1, "v1.0")

	// Create second snapshot
	settings2 := []storage.Setting{
		{Variable: "compare.shared", Value: "same", SettingType: "s", Description: "Shared"},
		{Variable: "compare.different", Value: "val2", SettingType: "s", Description: "Different"},
		{Variable: "compare.only2", Value: "only-in-2", SettingType: "s", Description: "Only in 2"},
	}
	time.Sleep(10 * time.Millisecond)
	store.SaveSnapshot(ctx, clusterID, settings2, "v1.0")

	// Get snapshot IDs
	snapshots, err := store.ListSnapshots(ctx, clusterID, 2)
	if err != nil || len(snapshots) < 2 {
		t.Fatalf("Failed to get snapshot IDs: %v", err)
	}
	snapshot1ID := snapshots[1].ID // older
	snapshot2ID := snapshots[0].ID // newer

	server, err := New(store)
	if err != nil {
		t.Fatalf("Failed to create web server: %v", err)
	}

	// Test valid comparison
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/compare-snapshots?snapshot1=%d&snapshot2=%d", snapshot1ID, snapshot2ID), nil)
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var result TimeCompareResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	// Should have one different, one before-only, one after-only
	if len(result.Different) != 1 {
		t.Errorf("Expected 1 different, got %d", len(result.Different))
	}
	if len(result.BeforeOnly) != 1 {
		t.Errorf("Expected 1 before-only, got %d", len(result.BeforeOnly))
	}
	if len(result.AfterOnly) != 1 {
		t.Errorf("Expected 1 after-only, got %d", len(result.AfterOnly))
	}

	// Test missing params
	req = httptest.NewRequest(http.MethodGet, "/api/compare-snapshots", nil)
	w = httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400 for missing params, got %d", w.Code)
	}

	// Test same snapshot
	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/compare-snapshots?snapshot1=%d&snapshot2=%d", snapshot1ID, snapshot1ID), nil)
	w = httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400 for same snapshot, got %d", w.Code)
	}

	// Test invalid IDs
	req = httptest.NewRequest(http.MethodGet, "/api/compare-snapshots?snapshot1=abc&snapshot2=123", nil)
	w = httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400 for invalid ID, got %d", w.Code)
	}

	// Test non-existent snapshot
	req = httptest.NewRequest(http.MethodGet, "/api/compare-snapshots?snapshot1=999999999&snapshot2=999999998", nil)
	w = httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("Expected 404 for non-existent snapshot, got %d", w.Code)
	}

	// Test method not allowed
	req = httptest.NewRequest(http.MethodPost, "/api/compare-snapshots?snapshot1=1&snapshot2=2", nil)
	w = httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405, got %d", w.Code)
	}
}

func TestHandleHistory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := storage.New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	clusters := []config.ClusterConfig{
		{ID: "prod", Name: "Production", DatabaseURL: "postgresql://prod"},
		{ID: "staging", Name: "Staging", DatabaseURL: "postgresql://staging"},
	}

	server, err := New(store, WithClusters(clusters), WithDefaultClusterID("prod"))
	if err != nil {
		t.Fatalf("Failed to create web server: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/history", nil)
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", w.Code)
	}

	contentType := w.Header().Get("Content-Type")
	if !strings.Contains(contentType, "text/html") {
		t.Errorf("Expected text/html, got %s", contentType)
	}
}

func TestWithRedactor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := storage.New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	redactor := storage.NewRedactor(storage.RedactorConfig{
		Enabled: true,
	})

	server, err := New(store, WithRedactor(redactor))
	if err != nil {
		t.Fatalf("Failed to create web server: %v", err)
	}

	// Just verify the server was created with the redactor
	// The redactor functionality is tested in storage/redact_test.go
	if server == nil {
		t.Fatal("Expected non-nil server")
	}
}

func TestWithClusters(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := storage.New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	clusters := []config.ClusterConfig{
		{ID: "prod", Name: "Production", DatabaseURL: "postgresql://prod"},
		{ID: "staging", Name: "Staging", DatabaseURL: "postgresql://staging"},
	}

	server, err := New(store, WithClusters(clusters))
	if err != nil {
		t.Fatalf("Failed to create web server: %v", err)
	}

	// Verify clusters are returned by API
	req := httptest.NewRequest(http.MethodGet, "/api/clusters", nil)
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", w.Code)
	}

	var result []map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if len(result) != 2 {
		t.Errorf("Expected 2 clusters, got %d", len(result))
	}
}
