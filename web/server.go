package web

import (
	"archive/zip"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"crdb-cluster-history/storage"

	"github.com/jackc/pgx/v5"
)

// AnnotationRequest is the JSON body for creating/updating annotations.
type AnnotationRequest struct {
	ChangeID int64  `json:"change_id,omitempty"`
	Content  string `json:"content"`
}

// AnnotationResponse is the JSON response for annotation operations.
type AnnotationResponse struct {
	ID        int64  `json:"id"`
	ChangeID  int64  `json:"change_id"`
	Content   string `json:"content"`
	CreatedBy string `json:"created_by"`
	CreatedAt string `json:"created_at"`
	UpdatedBy string `json:"updated_by,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

// ErrorResponse is the JSON response for errors.
type ErrorResponse struct {
	Error string `json:"error"`
}

//go:embed templates/*
var templateFS embed.FS

// Server handles HTTP requests for the web UI.
type Server struct {
	store    *storage.Store
	tmpl     *template.Template
	redactor *storage.Redactor
}

// Option configures the Server.
type Option func(*Server)

// WithRedactor sets the redactor for sensitive data.
func WithRedactor(r *storage.Redactor) Option {
	return func(s *Server) {
		s.redactor = r
	}
}

// New creates a new web server.
func New(store *storage.Store, opts ...Option) (*Server, error) {
	// Register custom template functions
	funcMap := template.FuncMap{
		"js": func(s string) template.JS {
			// Escape string for safe embedding in JavaScript string literals
			b, _ := json.Marshal(s)
			// Remove surrounding quotes since template uses '{{js .Content}}'
			if len(b) >= 2 {
				return template.JS(b[1 : len(b)-1])
			}
			return template.JS("")
		},
	}
	tmpl, err := template.New("").Funcs(funcMap).ParseFS(templateFS, "templates/*.html")
	if err != nil {
		return nil, err
	}

	s := &Server{
		store: store,
		tmpl:  tmpl,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/export", s.handleExport)
	mux.HandleFunc("/api/annotations", s.handleAnnotations)
	mux.HandleFunc("/api/annotations/", s.handleAnnotationByID)
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

	changes, err := s.store.GetChangesWithAnnotations(ctx, 100)
	if err != nil {
		log.Printf("Error getting changes: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Apply redaction if configured
	if s.redactor != nil {
		changes = s.redactChangesWithAnnotations(changes)
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
		Changes         []storage.ChangeWithAnnotation
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

	// Apply redaction if configured
	if s.redactor != nil {
		changes = s.redactor.RedactChanges(changes)
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
	if err := storage.WriteChangesCSV(csvFile, clusterID, changes); err != nil {
		log.Printf("Error writing CSV: %v", err)
	}
}

// handleAnnotations handles POST /api/annotations to create a new annotation.
func (s *Server) handleAnnotations(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req AnnotationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.jsonError(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if req.ChangeID == 0 {
		s.jsonError(w, "change_id is required", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.Content) == "" {
		s.jsonError(w, "content is required", http.StatusBadRequest)
		return
	}

	username := s.getUsernameFromRequest(r)

	ann, err := s.store.CreateAnnotation(r.Context(), req.ChangeID, req.Content, username)
	if err != nil {
		log.Printf("Error creating annotation: %v", err)
		errStr := err.Error()
		if strings.Contains(errStr, "foreign key") || strings.Contains(errStr, "violates") {
			s.jsonError(w, "Change not found", http.StatusNotFound)
			return
		}
		if strings.Contains(errStr, "unique") || strings.Contains(errStr, "duplicate") {
			s.jsonError(w, "Annotation already exists for this change", http.StatusConflict)
			return
		}
		s.jsonError(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(s.annotationToResponse(ann))
}

// handleAnnotationByID handles GET, PUT, DELETE /api/annotations/{id}
func (s *Server) handleAnnotationByID(w http.ResponseWriter, r *http.Request) {
	idStr := strings.TrimPrefix(r.URL.Path, "/api/annotations/")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		s.jsonError(w, "Invalid annotation ID", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.getAnnotation(w, r, id)
	case http.MethodPut:
		s.updateAnnotation(w, r, id)
	case http.MethodDelete:
		s.deleteAnnotation(w, r, id)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) getAnnotation(w http.ResponseWriter, r *http.Request, id int64) {
	ann, err := s.store.GetAnnotation(r.Context(), id)
	if err != nil {
		log.Printf("Error getting annotation: %v", err)
		s.jsonError(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	if ann == nil {
		s.jsonError(w, "Annotation not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.annotationToResponse(ann))
}

func (s *Server) updateAnnotation(w http.ResponseWriter, r *http.Request, id int64) {
	var req AnnotationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.jsonError(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if strings.TrimSpace(req.Content) == "" {
		s.jsonError(w, "content is required", http.StatusBadRequest)
		return
	}

	username := s.getUsernameFromRequest(r)

	err := s.store.UpdateAnnotation(r.Context(), id, req.Content, username)
	if err == pgx.ErrNoRows {
		s.jsonError(w, "Annotation not found", http.StatusNotFound)
		return
	}
	if err != nil {
		log.Printf("Error updating annotation: %v", err)
		s.jsonError(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	ann, err := s.store.GetAnnotation(r.Context(), id)
	if err != nil || ann == nil {
		s.jsonError(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.annotationToResponse(ann))
}

func (s *Server) deleteAnnotation(w http.ResponseWriter, r *http.Request, id int64) {
	err := s.store.DeleteAnnotation(r.Context(), id)
	if err == pgx.ErrNoRows {
		s.jsonError(w, "Annotation not found", http.StatusNotFound)
		return
	}
	if err != nil {
		log.Printf("Error deleting annotation: %v", err)
		s.jsonError(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// Helper methods

func (s *Server) jsonError(w http.ResponseWriter, message string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(ErrorResponse{Error: message})
}

func (s *Server) annotationToResponse(a *storage.Annotation) AnnotationResponse {
	resp := AnnotationResponse{
		ID:        a.ID,
		ChangeID:  a.ChangeID,
		Content:   a.Content,
		CreatedBy: a.CreatedBy,
		CreatedAt: a.CreatedAt.Format(time.RFC3339),
	}
	if a.UpdatedBy != "" {
		resp.UpdatedBy = a.UpdatedBy
	}
	if !a.UpdatedAt.IsZero() {
		resp.UpdatedAt = a.UpdatedAt.Format(time.RFC3339)
	}
	return resp
}

func (s *Server) getUsernameFromRequest(r *http.Request) string {
	username, _, ok := r.BasicAuth()
	if ok && username != "" {
		return username
	}
	return ""
}

func (s *Server) redactChangesWithAnnotations(changes []storage.ChangeWithAnnotation) []storage.ChangeWithAnnotation {
	result := make([]storage.ChangeWithAnnotation, len(changes))
	for i, c := range changes {
		result[i] = c
		result[i].Change = s.redactor.RedactChange(c.Change)
	}
	return result
}
