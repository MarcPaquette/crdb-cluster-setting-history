package web

import (
	"context"
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
	err = store.SaveSnapshot(ctx, settings1)
	if err != nil {
		t.Fatalf("Failed to save first snapshot: %v", err)
	}

	settings2 := []storage.Setting{
		{Variable: "web.test.setting", Value: "modified", SettingType: "s", Description: "Test"},
	}
	err = store.SaveSnapshot(ctx, settings2)
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
