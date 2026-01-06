package web

import (
	"embed"
	"html/template"
	"log"
	"net/http"

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

	data := struct {
		ClusterID string
		Changes   []storage.Change
	}{
		ClusterID: clusterID,
		Changes:   changes,
	}

	if err := s.tmpl.ExecuteTemplate(w, "index.html", data); err != nil {
		log.Printf("Template error: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}
