package web

import (
	"archive/zip"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"crdb-cluster-history/config"
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
	store            *storage.Store
	tmpl             *template.Template
	redactor         *storage.Redactor
	defaultClusterID string                 // Default cluster ID for single-cluster mode
	clusters         []config.ClusterConfig // List of configured clusters
}

// Option configures the Server.
type Option func(*Server)

// WithRedactor sets the redactor for sensitive data.
func WithRedactor(r *storage.Redactor) Option {
	return func(s *Server) {
		s.redactor = r
	}
}

// WithDefaultClusterID sets the default cluster ID for the server.
func WithDefaultClusterID(clusterID string) Option {
	return func(s *Server) {
		s.defaultClusterID = clusterID
	}
}

// WithClusters sets the list of configured clusters.
func WithClusters(clusters []config.ClusterConfig) Option {
	return func(s *Server) {
		s.clusters = clusters
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
		store:            store,
		tmpl:             tmpl,
		defaultClusterID: "default", // Default for backward compatibility
	}

	for _, opt := range opts {
		opt(s)
	}

	return s, nil
}

// getClusterID returns the cluster ID from the request, or the default.
func (s *Server) getClusterID(r *http.Request) string {
	clusterID := r.URL.Query().Get("cluster")
	if clusterID == "" {
		return s.defaultClusterID
	}
	return clusterID
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/export", s.handleExport)
	mux.HandleFunc("/compare", s.handleCompare)
	mux.HandleFunc("/history", s.handleHistory)
	mux.HandleFunc("/api/clusters", s.handleAPIClusters)
	mux.HandleFunc("/api/compare", s.handleAPICompare)
	mux.HandleFunc("/api/snapshots", s.handleAPISnapshots)
	mux.HandleFunc("/api/compare-snapshots", s.handleAPICompareSnapshots)
	mux.HandleFunc("/api/annotations", s.handleAnnotations)
	mux.HandleFunc("/api/annotations/", s.handleAnnotationByID)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	// Simple health check - verify we can query the database
	clusterID := s.getClusterID(r)
	_, err := s.store.GetChanges(r.Context(), clusterID, 1)
	if err != nil {
		http.Error(w, "unhealthy", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	clusterID := s.getClusterID(r)

	changes, err := s.store.GetChangesWithAnnotations(ctx, clusterID, 100)
	if err != nil {
		log.Printf("Error getting changes: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Apply redaction if configured
	if s.redactor != nil {
		changes = s.redactChangesWithAnnotations(changes)
	}

	sourceClusterID, err := s.store.GetSourceClusterID(ctx, clusterID)
	if err != nil {
		log.Printf("Error getting source cluster ID: %v", err)
		// Don't fail, just leave it empty
	}

	dbVersion, err := s.store.GetDatabaseVersion(ctx, clusterID)
	if err != nil {
		log.Printf("Error getting database version: %v", err)
		// Don't fail, just leave it empty
	}

	data := struct {
		ClusterID       string
		CurrentCluster  string
		DatabaseVersion string
		Changes         []storage.ChangeWithAnnotation
		Clusters        []config.ClusterConfig
	}{
		ClusterID:       sourceClusterID,
		CurrentCluster:  clusterID,
		DatabaseVersion: dbVersion,
		Changes:         changes,
		Clusters:        s.clusters,
	}

	if err := s.tmpl.ExecuteTemplate(w, "index.html", data); err != nil {
		log.Printf("Template error: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

func (s *Server) handleExport(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	clusterID := s.getClusterID(r)

	// Get all changes
	changes, err := s.store.GetChanges(ctx, clusterID, 100000)
	if err != nil {
		log.Printf("Error getting changes for export: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Apply redaction if configured
	if s.redactor != nil {
		changes = s.redactor.RedactChanges(changes)
	}

	// Get source cluster ID for filename and CSV
	sourceClusterID, err := s.store.GetSourceClusterID(ctx, clusterID)
	if err != nil {
		log.Printf("Error getting source cluster ID: %v", err)
		sourceClusterID = clusterID
	}
	if sourceClusterID == "" {
		sourceClusterID = clusterID
	}

	// Set headers for zip download
	filename := fmt.Sprintf("crdb-cluster-history-export-%s.zip", time.Now().Format("20060102-150405"))
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))

	// Create zip writer
	zipWriter := zip.NewWriter(w)
	defer zipWriter.Close()

	// Create CSV file inside zip
	csvFileName := fmt.Sprintf("crdb-cluster-history-%s.csv", sourceClusterID)
	csvFile, err := zipWriter.Create(csvFileName)
	if err != nil {
		log.Printf("Error creating CSV in zip: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Write CSV
	if err := storage.WriteChangesCSV(csvFile, sourceClusterID, changes); err != nil {
		log.Printf("Error writing CSV: %v", err)
	}
}

// ClusterInfo represents cluster information for the API response.
type ClusterInfo struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// handleAPIClusters returns the list of configured clusters as JSON.
func (s *Server) handleAPIClusters(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	clusters := make([]ClusterInfo, len(s.clusters))
	for i, c := range s.clusters {
		clusters[i] = ClusterInfo{ID: c.ID, Name: c.Name}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(clusters)
}

// CompareResult represents the comparison between two clusters.
type CompareResult struct {
	Cluster1Only []SettingDiff `json:"cluster1_only"`
	Cluster2Only []SettingDiff `json:"cluster2_only"`
	Different    []SettingDiff `json:"different"`
}

// SettingDiff represents a difference in a setting between clusters.
type SettingDiff struct {
	Variable    string `json:"variable"`
	Value1      string `json:"value1,omitempty"`
	Value2      string `json:"value2,omitempty"`
	Description string `json:"description,omitempty"`
}

// TimeCompareResult represents the comparison between two snapshots in time.
type TimeCompareResult struct {
	BeforeOnly []SettingDiff `json:"before_only"` // Settings only in the earlier snapshot
	AfterOnly  []SettingDiff `json:"after_only"`  // Settings only in the later snapshot
	Different  []SettingDiff `json:"different"`   // Settings with different values
}

// handleCompare renders the comparison page.
func (s *Server) handleCompare(w http.ResponseWriter, r *http.Request) {
	data := struct {
		Clusters []config.ClusterConfig
	}{
		Clusters: s.clusters,
	}

	if err := s.tmpl.ExecuteTemplate(w, "compare.html", data); err != nil {
		log.Printf("Template error: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

// handleAPICompare returns the comparison data between two clusters as JSON.
func (s *Server) handleAPICompare(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	cluster1 := r.URL.Query().Get("cluster1")
	cluster2 := r.URL.Query().Get("cluster2")

	if cluster1 == "" || cluster2 == "" {
		s.jsonError(w, "cluster1 and cluster2 query parameters are required", http.StatusBadRequest)
		return
	}

	if cluster1 == cluster2 {
		s.jsonError(w, "cluster1 and cluster2 must be different", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// Get settings for both clusters
	settings1, err := s.store.GetLatestSnapshot(ctx, cluster1)
	if err != nil {
		log.Printf("Error getting settings for cluster %s: %v", cluster1, err)
		s.jsonError(w, "Failed to get settings for cluster1", http.StatusInternalServerError)
		return
	}

	settings2, err := s.store.GetLatestSnapshot(ctx, cluster2)
	if err != nil {
		log.Printf("Error getting settings for cluster %s: %v", cluster2, err)
		s.jsonError(w, "Failed to get settings for cluster2", http.StatusInternalServerError)
		return
	}

	// Compare settings
	result := CompareResult{
		Cluster1Only: []SettingDiff{},
		Cluster2Only: []SettingDiff{},
		Different:    []SettingDiff{},
	}

	// Find settings only in cluster1 or different
	for variable, setting1 := range settings1 {
		setting2, exists := settings2[variable]
		if !exists {
			result.Cluster1Only = append(result.Cluster1Only, SettingDiff{
				Variable:    variable,
				Value1:      setting1.Value,
				Description: setting1.Description,
			})
		} else if setting1.Value != setting2.Value {
			result.Different = append(result.Different, SettingDiff{
				Variable:    variable,
				Value1:      setting1.Value,
				Value2:      setting2.Value,
				Description: setting1.Description,
			})
		}
	}

	// Find settings only in cluster2
	for variable, setting2 := range settings2 {
		if _, exists := settings1[variable]; !exists {
			result.Cluster2Only = append(result.Cluster2Only, SettingDiff{
				Variable:    variable,
				Value2:      setting2.Value,
				Description: setting2.Description,
			})
		}
	}

	// Sort results by variable name
	sort.Slice(result.Cluster1Only, func(i, j int) bool {
		return result.Cluster1Only[i].Variable < result.Cluster1Only[j].Variable
	})
	sort.Slice(result.Cluster2Only, func(i, j int) bool {
		return result.Cluster2Only[i].Variable < result.Cluster2Only[j].Variable
	})
	sort.Slice(result.Different, func(i, j int) bool {
		return result.Different[i].Variable < result.Different[j].Variable
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// handleHistory renders the time-based comparison page.
func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	data := struct {
		Clusters       []config.ClusterConfig
		CurrentCluster string
	}{
		Clusters:       s.clusters,
		CurrentCluster: s.getClusterID(r),
	}

	if err := s.tmpl.ExecuteTemplate(w, "history.html", data); err != nil {
		log.Printf("Template error: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

// handleAPISnapshots returns a list of snapshots for a cluster as JSON.
func (s *Server) handleAPISnapshots(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	clusterID := r.URL.Query().Get("cluster")
	if clusterID == "" {
		clusterID = s.defaultClusterID
	}

	limitStr := r.URL.Query().Get("limit")
	limit := 100 // default
	if limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil && parsed > 0 && parsed <= 1000 {
			limit = parsed
		}
	}

	ctx := r.Context()
	snapshots, err := s.store.ListSnapshots(ctx, clusterID, limit)
	if err != nil {
		log.Printf("Error listing snapshots for cluster %s: %v", clusterID, err)
		s.jsonError(w, "Failed to list snapshots", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(snapshots)
}

// handleAPICompareSnapshots returns the comparison between two snapshots as JSON.
func (s *Server) handleAPICompareSnapshots(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	snapshot1Str := r.URL.Query().Get("snapshot1")
	snapshot2Str := r.URL.Query().Get("snapshot2")

	if snapshot1Str == "" || snapshot2Str == "" {
		s.jsonError(w, "snapshot1 and snapshot2 query parameters are required", http.StatusBadRequest)
		return
	}

	snapshot1ID, err := strconv.ParseInt(snapshot1Str, 10, 64)
	if err != nil {
		s.jsonError(w, "invalid snapshot1 ID", http.StatusBadRequest)
		return
	}

	snapshot2ID, err := strconv.ParseInt(snapshot2Str, 10, 64)
	if err != nil {
		s.jsonError(w, "invalid snapshot2 ID", http.StatusBadRequest)
		return
	}

	if snapshot1ID == snapshot2ID {
		s.jsonError(w, "snapshot1 and snapshot2 must be different", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// Get settings for both snapshots
	settings1, err := s.store.GetSnapshotByID(ctx, snapshot1ID)
	if err != nil {
		log.Printf("Error getting snapshot %d: %v", snapshot1ID, err)
		s.jsonError(w, "Failed to get snapshot1", http.StatusInternalServerError)
		return
	}
	if settings1 == nil {
		s.jsonError(w, "snapshot1 not found", http.StatusNotFound)
		return
	}

	settings2, err := s.store.GetSnapshotByID(ctx, snapshot2ID)
	if err != nil {
		log.Printf("Error getting snapshot %d: %v", snapshot2ID, err)
		s.jsonError(w, "Failed to get snapshot2", http.StatusInternalServerError)
		return
	}
	if settings2 == nil {
		s.jsonError(w, "snapshot2 not found", http.StatusNotFound)
		return
	}

	// Compare settings
	result := TimeCompareResult{
		BeforeOnly: []SettingDiff{},
		AfterOnly:  []SettingDiff{},
		Different:  []SettingDiff{},
	}

	// Find settings only in snapshot1 (before) or different
	for variable, setting1 := range settings1 {
		setting2, exists := settings2[variable]
		if !exists {
			result.BeforeOnly = append(result.BeforeOnly, SettingDiff{
				Variable:    variable,
				Value1:      setting1.Value,
				Description: setting1.Description,
			})
		} else if setting1.Value != setting2.Value {
			result.Different = append(result.Different, SettingDiff{
				Variable:    variable,
				Value1:      setting1.Value,
				Value2:      setting2.Value,
				Description: setting1.Description,
			})
		}
	}

	// Find settings only in snapshot2 (after)
	for variable, setting2 := range settings2 {
		if _, exists := settings1[variable]; !exists {
			result.AfterOnly = append(result.AfterOnly, SettingDiff{
				Variable:    variable,
				Value2:      setting2.Value,
				Description: setting2.Description,
			})
		}
	}

	// Sort results by variable name
	sort.Slice(result.BeforeOnly, func(i, j int) bool {
		return result.BeforeOnly[i].Variable < result.BeforeOnly[j].Variable
	})
	sort.Slice(result.AfterOnly, func(i, j int) bool {
		return result.AfterOnly[i].Variable < result.AfterOnly[j].Variable
	})
	sort.Slice(result.Different, func(i, j int) bool {
		return result.Different[i].Variable < result.Different[j].Variable
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
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
