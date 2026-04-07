package web

import (
	"archive/zip"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"crdb-cluster-history/auth"
	"crdb-cluster-history/config"
	"crdb-cluster-history/storage"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

const (
	DefaultPageLimit     = 100
	MaxExportLimit       = 100_000
	DefaultSnapshotLimit = 100
	MaxSnapshotLimit     = 1000

	defaultClusterIDValue = "default"

	// PostgreSQL error codes
	pgForeignKeyViolation = "23503"
	pgUniqueViolation     = "23505"
)

//go:embed templates/*
var templateFS embed.FS

// Store defines the storage operations needed by the web server.
type Store interface {
	Ping(ctx context.Context) error
	GetChanges(ctx context.Context, clusterID string, limit int) ([]storage.Change, error)
	StreamChanges(ctx context.Context, clusterID string, fn func(storage.Change) error) error
	GetChangesWithAnnotations(ctx context.Context, clusterID string, limit int) ([]storage.ChangeWithAnnotation, error)
	GetSourceClusterID(ctx context.Context, clusterID string) (string, error)
	GetDatabaseVersion(ctx context.Context, clusterID string) (string, error)
	GetLatestSnapshot(ctx context.Context, clusterID string) (map[string]storage.Setting, error)
	ListSnapshots(ctx context.Context, clusterID string, limit int) ([]storage.SnapshotInfo, error)
	GetSnapshotByID(ctx context.Context, snapshotID int64) (map[string]storage.Setting, error)
	CreateAnnotation(ctx context.Context, changeID int64, content, createdBy string) (*storage.Annotation, error)
	GetAnnotation(ctx context.Context, id int64) (*storage.Annotation, error)
	UpdateAnnotation(ctx context.Context, id int64, content, updatedBy string) error
	DeleteAnnotation(ctx context.Context, id int64) error
}

// Server handles HTTP requests for the web UI.
type Server struct {
	store            Store
	tmpl             *template.Template
	redactor         *storage.Redactor
	defaultClusterID string                 // Default cluster ID for single-cluster mode
	clusters         []config.ClusterConfig // List of configured clusters
	authCfg          auth.Config            // Authentication configuration
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

// WithAuthConfig sets the authentication configuration.
func WithAuthConfig(cfg auth.Config) Option {
	return func(s *Server) {
		s.authCfg = cfg
	}
}

// New creates a new web server.
func New(store Store, opts ...Option) (*Server, error) {
	// Register custom template functions
	funcMap := template.FuncMap{
		"js": func(s string) template.JS {
			// Escape string for safe embedding in JavaScript string literals
			encoded, _ := json.Marshal(s)
			// Remove surrounding quotes since template uses '{{js .Content}}'
			if len(encoded) < 2 {
				return template.JS("")
			}
			return template.JS(encoded[1 : len(encoded)-1])
		},
	}
	tmpl, err := template.New("").Funcs(funcMap).ParseFS(templateFS, "templates/*.html")
	if err != nil {
		return nil, err
	}

	s := &Server{
		store:            store,
		tmpl:             tmpl,
		defaultClusterID: defaultClusterIDValue,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s, nil
}

// getClusterID returns the cluster ID from the request, or the default.
// Returns empty string if the cluster ID is not in the configured list.
func (s *Server) getClusterID(r *http.Request) string {
	clusterID := r.URL.Query().Get("cluster")
	if clusterID == "" {
		return s.defaultClusterID
	}
	if s.isValidCluster(clusterID) {
		return clusterID
	}
	return s.defaultClusterID
}

// isValidCluster checks if the given cluster ID is in the configured list.
func (s *Server) isValidCluster(id string) bool {
	if len(s.clusters) == 0 {
		return true // No validation when clusters aren't configured
	}
	for _, c := range s.clusters {
		if c.ID == id {
			return true
		}
	}
	return false
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/login", s.handleLogin)
	mux.HandleFunc("/logout", s.handleLogout)
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

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.renderLogin(w, r, "")
	case http.MethodPost:
		s.handleLoginSubmit(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) renderLogin(w http.ResponseWriter, r *http.Request, errorMsg string) {
	data := struct {
		Error string
		Nonce string
	}{
		Error: errorMsg,
		Nonce: GetNonce(r.Context()),
	}

	if err := s.tmpl.ExecuteTemplate(w, "login.html", data); err != nil {
		slog.Error("Template error", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

func (s *Server) handleLoginSubmit(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		s.renderLogin(w, r, "Invalid form data")
		return
	}

	username := r.FormValue("username")
	password := r.FormValue("password")

	if username == "" || password == "" {
		s.renderLogin(w, r, "Username and password are required")
		return
	}

	if !auth.CheckCredentials(username, password, s.authCfg) {
		s.renderLogin(w, r, "Invalid username or password")
		return
	}

	auth.SetSessionCookie(w, username, s.authCfg.Session)
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	auth.ClearSessionCookie(w)
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Ping(r.Context()); err != nil {
		http.Error(w, "unhealthy", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	clusterID := s.getClusterID(r)

	changes, err := s.store.GetChangesWithAnnotations(ctx, clusterID, DefaultPageLimit)
	if err != nil {
		slog.Error("Error getting changes", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Apply redaction if configured
	if s.redactor != nil {
		changes = s.redactChangesWithAnnotations(changes)
	}

	sourceClusterID, err := s.store.GetSourceClusterID(ctx, clusterID)
	if err != nil {
		slog.Error("Error getting source cluster ID", "error", err)
		// Don't fail, just leave it empty
	}

	dbVersion, err := s.store.GetDatabaseVersion(ctx, clusterID)
	if err != nil {
		slog.Error("Error getting database version", "error", err)
		// Don't fail, just leave it empty
	}

	data := struct {
		ClusterID       string
		CurrentCluster  string
		DatabaseVersion string
		Changes         []storage.ChangeWithAnnotation
		Clusters        []config.ClusterConfig
		Nonce           string
	}{
		ClusterID:       sourceClusterID,
		CurrentCluster:  clusterID,
		DatabaseVersion: dbVersion,
		Changes:         changes,
		Clusters:        s.clusters,
		Nonce:           GetNonce(ctx),
	}

	if err := s.tmpl.ExecuteTemplate(w, "index.html", data); err != nil {
		slog.Error("Template error", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

func (s *Server) handleExport(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	clusterID := s.getClusterID(r)

	// Get source cluster ID for filename
	sourceClusterID, err := s.store.GetSourceClusterID(ctx, clusterID)
	if err != nil {
		slog.Error("Error getting source cluster ID", "error", err)
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
		slog.Error("Error creating CSV in zip", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Stream changes directly to CSV without buffering all in memory
	csvWriter := storage.NewCSVChangeWriter(csvFile)
	if err := csvWriter.WriteHeader(); err != nil {
		slog.Error("Error writing CSV header", "error", err)
		return
	}

	err = s.store.StreamChanges(ctx, clusterID, func(c storage.Change) error {
		if s.redactor != nil {
			c = s.redactor.RedactChange(c)
		}
		return csvWriter.WriteChange(c)
	})
	if err != nil {
		slog.Error("Error streaming changes to CSV", "error", err)
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		slog.Error("CSV flush error", "error", err)
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

	jsonResponse(w, http.StatusOK, clusters)
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

// diffResult holds the three-way diff of two setting maps.
type diffResult struct {
	OnlyInA   []SettingDiff
	OnlyInB   []SettingDiff
	Different []SettingDiff
}

// compareSettings diffs two setting maps, returning only-in-a, only-in-b, and different entries, sorted by variable name.
func compareSettings(a, b map[string]storage.Setting) diffResult {
	result := diffResult{
		OnlyInA:   []SettingDiff{},
		OnlyInB:   []SettingDiff{},
		Different: []SettingDiff{},
	}

	for variable, sa := range a {
		sb, exists := b[variable]
		if !exists {
			result.OnlyInA = append(result.OnlyInA, SettingDiff{
				Variable:    variable,
				Value1:      sa.Value,
				Description: sa.Description,
			})
		} else if sa.Value != sb.Value {
			result.Different = append(result.Different, SettingDiff{
				Variable:    variable,
				Value1:      sa.Value,
				Value2:      sb.Value,
				Description: sa.Description,
			})
		}
	}

	for variable, sb := range b {
		if _, exists := a[variable]; !exists {
			result.OnlyInB = append(result.OnlyInB, SettingDiff{
				Variable:    variable,
				Value2:      sb.Value,
				Description: sb.Description,
			})
		}
	}

	sort.Slice(result.OnlyInA, func(i, j int) bool { return result.OnlyInA[i].Variable < result.OnlyInA[j].Variable })
	sort.Slice(result.OnlyInB, func(i, j int) bool { return result.OnlyInB[i].Variable < result.OnlyInB[j].Variable })
	sort.Slice(result.Different, func(i, j int) bool { return result.Different[i].Variable < result.Different[j].Variable })

	return result
}

// handleCompare renders the comparison page.
func (s *Server) handleCompare(w http.ResponseWriter, r *http.Request) {
	data := struct {
		Clusters []config.ClusterConfig
		Nonce    string
	}{
		Clusters: s.clusters,
		Nonce:    GetNonce(r.Context()),
	}

	if err := s.tmpl.ExecuteTemplate(w, "compare.html", data); err != nil {
		slog.Error("Template error", "error", err)
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
		slog.Error("Error getting settings for cluster", "cluster", cluster1, "error", err)
		s.jsonError(w, "Failed to get settings for cluster1", http.StatusInternalServerError)
		return
	}

	settings2, err := s.store.GetLatestSnapshot(ctx, cluster2)
	if err != nil {
		slog.Error("Error getting settings for cluster", "cluster", cluster2, "error", err)
		s.jsonError(w, "Failed to get settings for cluster2", http.StatusInternalServerError)
		return
	}

	diff := compareSettings(settings1, settings2)
	result := CompareResult{
		Cluster1Only: diff.OnlyInA,
		Cluster2Only: diff.OnlyInB,
		Different:    diff.Different,
	}

	jsonResponse(w, http.StatusOK, result)
}

// handleHistory renders the time-based comparison page.
func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	data := struct {
		Clusters       []config.ClusterConfig
		CurrentCluster string
		Nonce          string
	}{
		Clusters:       s.clusters,
		CurrentCluster: s.getClusterID(r),
		Nonce:          GetNonce(r.Context()),
	}

	if err := s.tmpl.ExecuteTemplate(w, "history.html", data); err != nil {
		slog.Error("Template error", "error", err)
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
	limit := DefaultSnapshotLimit
	if limitStr != "" {
		if parsed, err := strconv.Atoi(limitStr); err == nil && parsed > 0 && parsed <= MaxSnapshotLimit {
			limit = parsed
		}
	}

	ctx := r.Context()
	snapshots, err := s.store.ListSnapshots(ctx, clusterID, limit)
	if err != nil {
		slog.Error("Error listing snapshots", "cluster", clusterID, "error", err)
		s.jsonError(w, "Failed to list snapshots", http.StatusInternalServerError)
		return
	}

	jsonResponse(w, http.StatusOK, snapshots)
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
		slog.Error("Error getting snapshot", "snapshot", snapshot1ID, "error", err)
		s.jsonError(w, "Failed to get snapshot1", http.StatusInternalServerError)
		return
	}
	if settings1 == nil {
		s.jsonError(w, "snapshot1 not found", http.StatusNotFound)
		return
	}

	settings2, err := s.store.GetSnapshotByID(ctx, snapshot2ID)
	if err != nil {
		slog.Error("Error getting snapshot", "snapshot", snapshot2ID, "error", err)
		s.jsonError(w, "Failed to get snapshot2", http.StatusInternalServerError)
		return
	}
	if settings2 == nil {
		s.jsonError(w, "snapshot2 not found", http.StatusNotFound)
		return
	}

	diff := compareSettings(settings1, settings2)
	result := TimeCompareResult{
		BeforeOnly: diff.OnlyInA,
		AfterOnly:  diff.OnlyInB,
		Different:  diff.Different,
	}

	jsonResponse(w, http.StatusOK, result)
}

// handleAnnotations handles POST /api/annotations to create a new annotation.
func (s *Server) handleAnnotations(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req AnnotationRequest
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MB limit
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
		slog.Error("Error creating annotation", "error", err)
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			switch pgErr.Code {
			case pgForeignKeyViolation:
				s.jsonError(w, "Change not found", http.StatusNotFound)
				return
			case pgUniqueViolation:
				s.jsonError(w, "Annotation already exists for this change", http.StatusConflict)
				return
			}
		}
		s.jsonError(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	jsonResponse(w, http.StatusCreated, s.annotationToResponse(ann))
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
		slog.Error("Error getting annotation", "error", err)
		s.jsonError(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	if ann == nil {
		s.jsonError(w, "Annotation not found", http.StatusNotFound)
		return
	}

	jsonResponse(w, http.StatusOK, s.annotationToResponse(ann))
}

func (s *Server) updateAnnotation(w http.ResponseWriter, r *http.Request, id int64) {
	var req AnnotationRequest
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MB limit
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
		slog.Error("Error updating annotation", "error", err)
		s.jsonError(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	ann, err := s.store.GetAnnotation(r.Context(), id)
	if err != nil || ann == nil {
		s.jsonError(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	jsonResponse(w, http.StatusOK, s.annotationToResponse(ann))
}

func (s *Server) deleteAnnotation(w http.ResponseWriter, r *http.Request, id int64) {
	err := s.store.DeleteAnnotation(r.Context(), id)
	if err == pgx.ErrNoRows {
		s.jsonError(w, "Annotation not found", http.StatusNotFound)
		return
	}
	if err != nil {
		slog.Error("Error deleting annotation", "error", err)
		s.jsonError(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// Helper methods

func jsonResponse(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (s *Server) jsonError(w http.ResponseWriter, message string, status int) {
	jsonResponse(w, status, ErrorResponse{Error: message})
}

func (s *Server) annotationToResponse(a *storage.Annotation) AnnotationResponse {
	resp := AnnotationResponse{
		ID:        a.ID,
		ChangeID:  a.ChangeID,
		Content:   a.Content,
		CreatedBy: a.CreatedBy,
		CreatedAt: a.CreatedAt.Format(time.RFC3339),
		UpdatedBy: a.UpdatedBy,
	}
	if !a.UpdatedAt.IsZero() {
		resp.UpdatedAt = a.UpdatedAt.Format(time.RFC3339)
	}
	return resp
}

func (s *Server) getUsernameFromRequest(r *http.Request) string {
	username, _, _ := r.BasicAuth()
	if username != "" {
		return username
	}
	// Fall back to session cookie
	if cookie, err := r.Cookie("session"); err == nil {
		if name, valid := auth.ValidateSessionToken(cookie.Value, s.authCfg.Session); valid {
			return name
		}
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
