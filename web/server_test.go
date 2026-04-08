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
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"crdb-cluster-history/auth"
	"crdb-cluster-history/config"
	"crdb-cluster-history/storage"
)

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

func setupTestStore(t *testing.T) (context.Context, *storage.Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	store, err := storage.New(ctx, getTestDB(t))
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return ctx, store
}

func setupTest(t *testing.T, opts ...Option) (context.Context, *storage.Store, *Server) {
	t.Helper()
	ctx, store := setupTestStore(t)
	server, err := New(store, opts...)
	if err != nil {
		t.Fatalf("Failed to create web server: %v", err)
	}
	return ctx, store, server
}

func TestNew(t *testing.T) {
	_, store := setupTestStore(t)

	server, err := New(store)
	if err != nil {
		t.Fatalf("Failed to create web server: %v", err)
	}
	if server == nil {
		t.Fatal("Expected non-nil server")
	}
}

func TestHandler(t *testing.T) {
	_, _, server := setupTest(t)

	handler := server.Handler()
	if handler == nil {
		t.Fatal("Expected non-nil handler")
	}
}

func TestHandleIndex(t *testing.T) {
	_, _, server := setupTest(t)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if !strings.Contains(contentType, "text/html") {
		t.Errorf("Expected text/html content type, got %s", contentType)
	}

	body := w.Body.String()
	if !strings.Contains(body, "CockroachDB Cluster Settings History") {
		t.Error("Expected page title in response")
	}
	if !strings.Contains(body, "Auto-refresh") {
		t.Error("Expected Auto-refresh option in response")
	}
}

func TestHandleIndexWithChanges(t *testing.T) {
	ctx, store, server := setupTest(t)

	settings1 := []storage.Setting{
		{Variable: "web.test.setting", Value: "original", SettingType: "s", Description: "Test"},
	}
	if err := store.SaveSnapshot(ctx, testClusterID, settings1, "v1.0.0"); err != nil {
		t.Fatalf("Failed to save first snapshot: %v", err)
	}

	settings2 := []storage.Setting{
		{Variable: "web.test.setting", Value: "modified", SettingType: "s", Description: "Test"},
	}
	if err := store.SaveSnapshot(ctx, testClusterID, settings2, "v1.0.0"); err != nil {
		t.Fatalf("Failed to save second snapshot: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)

	body := w.Body.String()
	if !strings.Contains(body, "<table>") {
		t.Error("Expected table in response when changes exist")
	}
	if !strings.Contains(body, "web.test.setting") {
		t.Error("Expected test setting in response")
	}
}

func TestHandleIndexNoChanges(t *testing.T) {
	_, _, server := setupTest(t)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}
}

func TestHandleHealth(t *testing.T) {
	_, _, server := setupTest(t)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body := w.Body.String()
	if body != "ok" {
		t.Errorf("Expected body 'ok', got '%s'", body)
	}
}

func TestHandleExport(t *testing.T) {
	_, _, server := setupTest(t)

	req := httptest.NewRequest(http.MethodGet, "/export", nil)
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType != "application/zip" {
		t.Errorf("Expected application/zip content type, got %s", contentType)
	}

	disposition := resp.Header.Get("Content-Disposition")
	if !strings.Contains(disposition, "attachment") {
		t.Error("Expected Content-Disposition to contain 'attachment'")
	}
	if !strings.Contains(disposition, ".zip") {
		t.Error("Expected Content-Disposition to contain '.zip'")
	}
}

func TestHandleExportZipContainsCSV(t *testing.T) {
	_, _, server := setupTest(t)

	req := httptest.NewRequest(http.MethodGet, "/export", nil)
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)

	body := w.Body.Bytes()
	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
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

func TestHandleExportWithChanges(t *testing.T) {
	ctx, store, server := setupTest(t)

	store.CleanupOldChanges(ctx, testClusterID, 0)

	settings1 := []storage.Setting{
		{Variable: "export.test.setting", Value: "original", SettingType: "s", Description: "Export test"},
	}
	if err := store.SaveSnapshot(ctx, testClusterID, settings1, "v25.1.0"); err != nil {
		t.Fatalf("Failed to save first snapshot: %v", err)
	}

	settings2 := []storage.Setting{
		{Variable: "export.test.setting", Value: "modified", SettingType: "s", Description: "Export test"},
	}
	if err := store.SaveSnapshot(ctx, testClusterID, settings2, "v25.1.0"); err != nil {
		t.Fatalf("Failed to save second snapshot: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/export", nil)
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)

	body := w.Body.Bytes()
	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("Failed to read zip: %v", err)
	}

	csvFile := zipReader.File[0]
	rc, err := csvFile.Open()
	if err != nil {
		t.Fatalf("Failed to open CSV: %v", err)
	}
	defer rc.Close()

	csvContent, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("Failed to read CSV content: %v", err)
	}

	csvReader := csv.NewReader(bytes.NewReader(csvContent))
	records, err := csvReader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to parse CSV: %v", err)
	}

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
	ctx, store, server := setupTest(t)

	sourceClusterID := "test-cluster-export-12345"
	if err := store.SetSourceClusterID(ctx, testClusterID, sourceClusterID); err != nil {
		t.Fatalf("Failed to set source cluster ID: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/export", nil)
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)

	body := w.Body.Bytes()
	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("Failed to read zip: %v", err)
	}

	csvFile := zipReader.File[0]
	if !strings.Contains(csvFile.Name, sourceClusterID) {
		t.Errorf("Expected CSV filename to contain source cluster ID, got %s", csvFile.Name)
	}
}

func cleanupAnnotationTestData(t *testing.T, store *storage.Store, ctx context.Context) {
	t.Helper()
	store.CleanupOldChanges(ctx, testClusterID, 0)
}

func createTestChange(t *testing.T, store *storage.Store, ctx context.Context) int64 {
	t.Helper()
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
	ctx, store, server := setupTest(t)

	cleanupAnnotationTestData(t, store, ctx)
	changeID := createTestChange(t, store, ctx)

	body := strings.NewReader(fmt.Sprintf(`{"change_id":%d,"content":"API test note"}`, changeID))
	req := httptest.NewRequest(http.MethodPost, "/api/annotations", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected 201, got %d: %s", w.Code, w.Body.String())
	}

	respBody := w.Body.String()
	if !strings.Contains(respBody, "API test note") {
		t.Errorf("Expected response to contain 'API test note', got %s", respBody)
	}
	if !strings.Contains(respBody, `"change_id"`) {
		t.Errorf("Expected response to contain change_id, got %s", respBody)
	}
}

func TestAnnotationAPI_CreateWithBasicAuth(t *testing.T) {
	ctx, store, server := setupTest(t)

	cleanupAnnotationTestData(t, store, ctx)
	changeID := createTestChange(t, store, ctx)

	body := strings.NewReader(fmt.Sprintf(`{"change_id":%d,"content":"Auth test note"}`, changeID))
	req := httptest.NewRequest(http.MethodPost, "/api/annotations", body)
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("testadmin", "password")
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected 201, got %d: %s", w.Code, w.Body.String())
	}

	respBody := w.Body.String()
	if !strings.Contains(respBody, "testadmin") {
		t.Errorf("Expected response to contain 'testadmin' as created_by, got %s", respBody)
	}
}

func TestAnnotationAPI_GetNotFound(t *testing.T) {
	_, _, server := setupTest(t)

	req := httptest.NewRequest(http.MethodGet, "/api/annotations/999999", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected 404, got %d", w.Code)
	}
}

func TestAnnotationAPI_InvalidJSON(t *testing.T) {
	_, _, server := setupTest(t)

	req := httptest.NewRequest(http.MethodPost, "/api/annotations", strings.NewReader("not json"))
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d", w.Code)
	}
}

func TestAnnotationAPI_EmptyContent(t *testing.T) {
	_, _, server := setupTest(t)

	req := httptest.NewRequest(http.MethodPost, "/api/annotations",
		strings.NewReader(`{"change_id":1,"content":""}`))
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400 for empty content, got %d", w.Code)
	}
}

func TestAnnotationAPI_MissingChangeID(t *testing.T) {
	_, _, server := setupTest(t)

	req := httptest.NewRequest(http.MethodPost, "/api/annotations",
		strings.NewReader(`{"content":"no change id"}`))
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400 for missing change_id, got %d", w.Code)
	}
}

func TestAnnotationAPI_Update(t *testing.T) {
	ctx, store, server := setupTest(t)

	cleanupAnnotationTestData(t, store, ctx)
	changeID := createTestChange(t, store, ctx)

	ann, err := store.CreateAnnotation(ctx, changeID, "Original content", "user1")
	if err != nil {
		t.Fatalf("Failed to create annotation: %v", err)
	}

	body := strings.NewReader(`{"content":"Updated content"}`)
	req := httptest.NewRequest(http.MethodPut, fmt.Sprintf("/api/annotations/%d", ann.ID), body)
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("user2", "password")
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d: %s", w.Code, w.Body.String())
	}

	respBody := w.Body.String()
	if !strings.Contains(respBody, "Updated content") {
		t.Errorf("Expected 'Updated content' in response, got %s", respBody)
	}
	if !strings.Contains(respBody, "user2") {
		t.Errorf("Expected 'user2' as updated_by in response, got %s", respBody)
	}
}

func TestAnnotationAPI_Delete(t *testing.T) {
	ctx, store, server := setupTest(t)

	cleanupAnnotationTestData(t, store, ctx)
	changeID := createTestChange(t, store, ctx)

	ann, err := store.CreateAnnotation(ctx, changeID, "To be deleted", "user")
	if err != nil {
		t.Fatalf("Failed to create annotation: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/annotations/%d", ann.ID), nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("Expected 204, got %d: %s", w.Code, w.Body.String())
	}

	deleted, _ := store.GetAnnotation(ctx, ann.ID)
	if deleted != nil {
		t.Error("Expected annotation to be deleted")
	}
}

func TestAnnotationAPI_InvalidID(t *testing.T) {
	_, _, server := setupTest(t)

	req := httptest.NewRequest(http.MethodGet, "/api/annotations/notanumber", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400, got %d", w.Code)
	}
}

func TestAnnotationAPI_MethodNotAllowed(t *testing.T) {
	_, _, server := setupTest(t)

	req := httptest.NewRequest(http.MethodGet, "/api/annotations", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405, got %d", w.Code)
	}
}

func TestHandleAPIClusters(t *testing.T) {
	_, _, server := setupTest(t)

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

	body := w.Body.String()
	if body != "[]\n" && body != "[]" {
		if body[0] != '[' {
			t.Errorf("Expected JSON array, got %s", body)
		}
	}
}

func TestHandleAPIClustersMethodNotAllowed(t *testing.T) {
	_, _, server := setupTest(t)

	req := httptest.NewRequest(http.MethodPost, "/api/clusters", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405, got %d", w.Code)
	}
}

func TestHandleCompare(t *testing.T) {
	_, _, server := setupTest(t)

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
	ctx, store, server := setupTest(t)

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
	if !strings.Contains(body, "compare.test.different") {
		t.Error("Expected different setting in response")
	}
	if !strings.Contains(body, "compare.test.only1") {
		t.Error("Expected cluster1-only setting in response")
	}
	if !strings.Contains(body, "compare.test.only2") {
		t.Error("Expected cluster2-only setting in response")
	}
}

func TestHandleAPICompareMissingParams(t *testing.T) {
	_, _, server := setupTest(t)

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
	ctx, store, server := setupTest(t, WithDefaultClusterID("other-cluster"))

	settings := []storage.Setting{
		{Variable: "cluster.param.test", Value: "test-value", SettingType: "s", Description: "Test"},
	}
	store.SaveSnapshot(ctx, "param-test-cluster", settings, "v1.0")

	settings2 := []storage.Setting{
		{Variable: "cluster.param.test", Value: "changed", SettingType: "s", Description: "Test"},
	}
	store.SaveSnapshot(ctx, "param-test-cluster", settings2, "v1.0")

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
	clusterID := "api-snapshots-test-" + time.Now().Format("20060102150405.000")
	ctx, store, server := setupTest(t, WithDefaultClusterID(clusterID))

	settings := []storage.Setting{
		{Variable: "api.snapshot.test", Value: "v1", SettingType: "s", Description: "Test"},
	}
	for range 3 {
		if err := store.SaveSnapshot(ctx, clusterID, settings, "v1.0"); err != nil {
			t.Fatalf("Failed to save snapshot: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
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
	clusterID := "compare-snapshots-test-" + time.Now().Format("20060102150405.000")
	ctx, store, server := setupTest(t)

	settings1 := []storage.Setting{
		{Variable: "compare.shared", Value: "same", SettingType: "s", Description: "Shared"},
		{Variable: "compare.different", Value: "val1", SettingType: "s", Description: "Different"},
		{Variable: "compare.only1", Value: "only-in-1", SettingType: "s", Description: "Only in 1"},
	}
	store.SaveSnapshot(ctx, clusterID, settings1, "v1.0")

	settings2 := []storage.Setting{
		{Variable: "compare.shared", Value: "same", SettingType: "s", Description: "Shared"},
		{Variable: "compare.different", Value: "val2", SettingType: "s", Description: "Different"},
		{Variable: "compare.only2", Value: "only-in-2", SettingType: "s", Description: "Only in 2"},
	}
	time.Sleep(10 * time.Millisecond)
	store.SaveSnapshot(ctx, clusterID, settings2, "v1.0")

	snapshots, err := store.ListSnapshots(ctx, clusterID, 2)
	if err != nil || len(snapshots) < 2 {
		t.Fatalf("Failed to get snapshot IDs: %v", err)
	}
	snapshot1ID := snapshots[1].ID // older
	snapshot2ID := snapshots[0].ID // newer

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
	clusters := []config.ClusterConfig{
		{ID: "prod", Name: "Production", DatabaseURL: "postgresql://prod"},
		{ID: "staging", Name: "Staging", DatabaseURL: "postgresql://staging"},
	}

	_, _, server := setupTest(t, WithClusters(clusters), WithDefaultClusterID("prod"))

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
	redactor := storage.NewRedactor(storage.RedactorConfig{
		Enabled: true,
	})

	_, _, server := setupTest(t, WithRedactor(redactor))

	if server == nil {
		t.Fatal("Expected non-nil server")
	}
}

func TestWithClusters(t *testing.T) {
	clusters := []config.ClusterConfig{
		{ID: "prod", Name: "Production", DatabaseURL: "postgresql://prod"},
		{ID: "staging", Name: "Staging", DatabaseURL: "postgresql://staging"},
	}

	_, _, server := setupTest(t, WithClusters(clusters))

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

func testAuthConfig() auth.Config {
	hash, _ := auth.HashPassword("secret")
	return auth.Config{
		Enabled:      true,
		Username:     "admin",
		PasswordHash: hash,
		Session:      auth.NewSessionConfig(false),
	}
}

func TestHandleFleet(t *testing.T) {
	_, _, server := setupTest(t)

	req := httptest.NewRequest(http.MethodGet, "/fleet", nil)
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
	if !strings.Contains(body, "Fleet Comparison") {
		t.Error("Expected page title in response")
	}
	if !strings.Contains(body, "multi-compare.html") == false && !strings.Contains(body, "fleet") {
		t.Error("Expected fleet page content")
	}
}

func TestHandleAPIClusterSettings(t *testing.T) {
	ctx, store, server := setupTest(t)

	clusterID := "fleet-test-cluster-" + time.Now().Format("20060102150405.000")

	settings := []storage.Setting{
		{Variable: "fleet.test.setting1", Value: "value1", SettingType: "s", Description: "Test setting 1"},
		{Variable: "fleet.test.setting2", Value: "value2", SettingType: "s", Description: "Test setting 2"},
	}
	if err := store.SaveSnapshot(ctx, clusterID, settings, "v1.0"); err != nil {
		t.Fatalf("Failed to save snapshot: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/cluster-settings?cluster="+clusterID, nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d: %s", w.Code, w.Body.String())
	}

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Expected application/json, got %s", contentType)
	}

	var result map[string]ClusterSettingResponse
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	if len(result) != 2 {
		t.Errorf("Expected 2 settings, got %d", len(result))
	}

	if s, ok := result["fleet.test.setting1"]; !ok {
		t.Error("Expected fleet.test.setting1 in result")
	} else {
		if s.Value != "value1" {
			t.Errorf("Expected value 'value1', got %q", s.Value)
		}
		if s.Description != "Test setting 1" {
			t.Errorf("Expected description 'Test setting 1', got %q", s.Description)
		}
	}
}

func TestHandleAPIClusterSettingsMissingCluster(t *testing.T) {
	_, _, server := setupTest(t)

	req := httptest.NewRequest(http.MethodGet, "/api/cluster-settings", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400 for missing cluster param, got %d", w.Code)
	}
}

func TestHandleAPIClusterSettingsInvalidCluster(t *testing.T) {
	clusters := []config.ClusterConfig{
		{ID: "valid-cluster", Name: "Valid", DatabaseURL: "postgresql://valid"},
	}
	_, _, server := setupTest(t, WithClusters(clusters))

	req := httptest.NewRequest(http.MethodGet, "/api/cluster-settings?cluster=invalid-cluster", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected 400 for invalid cluster, got %d", w.Code)
	}
}

func TestHandleAPIClusterSettingsMethodNotAllowed(t *testing.T) {
	_, _, server := setupTest(t)

	req := httptest.NewRequest(http.MethodPost, "/api/cluster-settings?cluster=test", nil)
	w := httptest.NewRecorder()

	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405, got %d", w.Code)
	}
}

func TestHandleLoginPage(t *testing.T) {
	_, _, server := setupTest(t, WithAuthConfig(testAuthConfig()))

	req := httptest.NewRequest(http.MethodGet, "/login", nil)
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "Login") {
		t.Error("Expected login page content")
	}
	if !strings.Contains(body, "username") {
		t.Error("Expected username field")
	}
}

func TestHandleLoginSubmit_Success(t *testing.T) {
	cfg := testAuthConfig()
	_, _, server := setupTest(t, WithAuthConfig(cfg))

	form := url.Values{}
	form.Set("username", "admin")
	form.Set("password", "secret")
	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusSeeOther {
		t.Errorf("Expected 303 redirect, got %d", w.Code)
	}
	if loc := w.Header().Get("Location"); loc != "/" {
		t.Errorf("Expected redirect to /, got %q", loc)
	}

	// Check session cookie was set
	cookies := w.Result().Cookies()
	found := false
	for _, c := range cookies {
		if c.Name == "session" {
			found = true
			_, valid := auth.ValidateSessionToken(c.Value, cfg.Session)
			if !valid {
				t.Error("Expected valid session token in cookie")
			}
		}
	}
	if !found {
		t.Error("Expected session cookie to be set")
	}
}

func TestHandleLoginSubmit_Failure(t *testing.T) {
	_, _, server := setupTest(t, WithAuthConfig(testAuthConfig()))

	form := url.Values{}
	form.Set("username", "admin")
	form.Set("password", "wrong")
	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200 (re-rendered login page), got %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "Invalid username or password") {
		t.Error("Expected error message in response")
	}
}

func TestHandleLogout(t *testing.T) {
	_, _, server := setupTest(t, WithAuthConfig(testAuthConfig()))

	req := httptest.NewRequest(http.MethodPost, "/logout", nil)
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusSeeOther {
		t.Errorf("Expected 303 redirect, got %d", w.Code)
	}
	if loc := w.Header().Get("Location"); loc != "/login" {
		t.Errorf("Expected redirect to /login, got %q", loc)
	}

	// Check session cookie was cleared
	cookies := w.Result().Cookies()
	for _, c := range cookies {
		if c.Name == "session" && c.MaxAge != -1 {
			t.Error("Expected session cookie to be cleared (MaxAge -1)")
		}
	}
}
