package web

import (
	"embed"
	"html/template"
	"log"
	"net/http"

	"cluster-history/storage"
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
	return mux
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	changes, err := s.store.GetChanges(r.Context(), 100)
	if err != nil {
		log.Printf("Error getting changes: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	data := struct {
		Changes []storage.Change
	}{
		Changes: changes,
	}

	if err := s.tmpl.ExecuteTemplate(w, "index.html", data); err != nil {
		log.Printf("Template error: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}
