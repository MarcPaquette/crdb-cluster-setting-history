package web

import (
	"archive/zip"
	"embed"
	"encoding/csv"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"time"

	"crdb-cluster-history/storage"
)

//go:embed templates/*
var templateFS embed.FS

type Server struct {
	store *storage.Store
	tmpl  *template.Template
}

func New(store *storage.Store) (*Server, error) {
	tmpl, err := template.ParseFS(templateFS, "templates/*.html")
	if err != nil {
		return nil, err
	}

	return &Server{
		store: store,
		tmpl:  tmpl,
	}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/export", s.handleExport)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	// Simple health check - verify we can query the database
	_, err := s.store.GetChanges(r.Context(), 1)
	if err != nil {
		http.Error(w, "unhealthy", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	changes, err := s.store.GetChanges(ctx, 100)
	if err != nil {
		log.Printf("Error getting changes: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	clusterID, err := s.store.GetClusterID(ctx)
	if err != nil {
		log.Printf("Error getting cluster ID: %v", err)
		// Don't fail, just leave it empty
	}

	dbVersion, err := s.store.GetDatabaseVersion(ctx)
	if err != nil {
		log.Printf("Error getting database version: %v", err)
		// Don't fail, just leave it empty
	}

	data := struct {
		ClusterID       string
		DatabaseVersion string
		Changes         []storage.Change
	}{
		ClusterID:       clusterID,
		DatabaseVersion: dbVersion,
		Changes:         changes,
	}

	if err := s.tmpl.ExecuteTemplate(w, "index.html", data); err != nil {
		log.Printf("Template error: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

func (s *Server) handleExport(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get all changes
	changes, err := s.store.GetChanges(ctx, 100000)
	if err != nil {
		log.Printf("Error getting changes for export: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Get cluster ID for filename and CSV
	clusterID, err := s.store.GetClusterID(ctx)
	if err != nil {
		log.Printf("Error getting cluster ID: %v", err)
		clusterID = "unknown"
	}

	// Set headers for zip download
	filename := fmt.Sprintf("crdb-cluster-history-export-%s.zip", time.Now().Format("20060102-150405"))
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))

	// Create zip writer
	zipWriter := zip.NewWriter(w)
	defer zipWriter.Close()

	// Create CSV file inside zip
	csvFileName := fmt.Sprintf("crdb-cluster-history-%s.csv", clusterID)
	csvFile, err := zipWriter.Create(csvFileName)
	if err != nil {
		log.Printf("Error creating CSV in zip: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Write CSV
	csvWriter := csv.NewWriter(csvFile)

	// Write header
	header := []string{"cluster_id", "detected_at", "variable", "version", "old_value", "new_value", "description"}
	if err := csvWriter.Write(header); err != nil {
		log.Printf("Error writing CSV header: %v", err)
		return
	}

	// Write rows
	for _, c := range changes {
		row := []string{
			clusterID,
			c.DetectedAt.Format(time.RFC3339),
			c.Variable,
			c.Version,
			c.OldValue,
			c.NewValue,
			c.Description,
		}
		if err := csvWriter.Write(row); err != nil {
			log.Printf("Error writing CSV row: %v", err)
			return
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		log.Printf("CSV writer error: %v", err)
	}
}
