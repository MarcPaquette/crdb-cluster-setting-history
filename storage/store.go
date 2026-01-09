package storage

import (
	"context"
	"encoding/csv"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Setting struct {
	Variable    string
	Value       string
	SettingType string
	Description string
}

type Change struct {
	ClusterID   string // Which cluster this change belongs to
	DetectedAt  time.Time
	Variable    string
	OldValue    string
	NewValue    string
	Description string
	Version     string
}

// Annotation represents a user comment on a specific change.
type Annotation struct {
	ID        int64
	ChangeID  int64
	Content   string
	CreatedBy string
	CreatedAt time.Time
	UpdatedBy string    // Empty if never updated
	UpdatedAt time.Time // Zero value if never updated
}

// ChangeWithAnnotation combines a Change with its ID and optional Annotation.
type ChangeWithAnnotation struct {
	Change
	ID         int64       // The change ID (needed for annotation operations)
	Annotation *Annotation // nil if no annotation exists
}

// SnapshotInfo represents metadata about a snapshot (without full settings).
type SnapshotInfo struct {
	ID          int64     `json:"id,string"` // String to avoid JavaScript precision loss
	ClusterID   string    `json:"cluster_id"`
	CollectedAt time.Time `json:"collected_at"`
}

type Store struct {
	pool *pgxpool.Pool
}

// querier is an interface that both pgxpool.Pool and pgx.Tx implement.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func New(ctx context.Context, connString string) (*Store, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, err
	}

	if err := initSchema(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}

	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func initSchema(ctx context.Context, pool *pgxpool.Pool) error {
	schema := `
	CREATE TABLE IF NOT EXISTS snapshots (
		id SERIAL PRIMARY KEY,
		collected_at TIMESTAMPTZ NOT NULL
	);

	CREATE TABLE IF NOT EXISTS settings (
		id SERIAL PRIMARY KEY,
		snapshot_id INT REFERENCES snapshots(id) ON DELETE CASCADE,
		variable TEXT NOT NULL,
		value TEXT NOT NULL,
		setting_type TEXT,
		description TEXT
	);

	CREATE INDEX IF NOT EXISTS idx_settings_snapshot ON settings(snapshot_id);

	CREATE TABLE IF NOT EXISTS changes (
		id SERIAL PRIMARY KEY,
		detected_at TIMESTAMPTZ NOT NULL,
		variable TEXT NOT NULL,
		old_value TEXT,
		new_value TEXT,
		description TEXT
	);

	CREATE INDEX IF NOT EXISTS idx_changes_detected ON changes(detected_at DESC);

	CREATE TABLE IF NOT EXISTS metadata (
		key TEXT PRIMARY KEY,
		value TEXT NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL
	);
	`
	_, err := pool.Exec(ctx, schema)
	if err != nil {
		return err
	}

	// Migrate existing FK constraint to include ON DELETE CASCADE
	// Check if the old constraint exists without CASCADE
	var needsMigration bool
	err = pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_constraint
			WHERE conname = 'settings_snapshot_id_fkey'
			AND confdeltype = 'a'
		)
	`).Scan(&needsMigration)
	if err != nil {
		return err
	}

	if needsMigration {
		_, err = pool.Exec(ctx, "ALTER TABLE settings DROP CONSTRAINT settings_snapshot_id_fkey")
		if err != nil {
			return err
		}
		_, err = pool.Exec(ctx, "ALTER TABLE settings ADD CONSTRAINT settings_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES snapshots(id) ON DELETE CASCADE")
		if err != nil {
			return err
		}
	}

	// Add description column to changes table if it doesn't exist
	var hasDescriptionColumn bool
	err = pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_name = 'changes' AND column_name = 'description'
		)
	`).Scan(&hasDescriptionColumn)
	if err != nil {
		return err
	}

	if !hasDescriptionColumn {
		_, err = pool.Exec(ctx, "ALTER TABLE changes ADD COLUMN description TEXT")
		if err != nil {
			return err
		}
	}

	// Add version column to changes table if it doesn't exist
	var hasVersionColumn bool
	err = pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_name = 'changes' AND column_name = 'version'
		)
	`).Scan(&hasVersionColumn)
	if err != nil {
		return err
	}

	if !hasVersionColumn {
		_, err = pool.Exec(ctx, "ALTER TABLE changes ADD COLUMN version TEXT")
		if err != nil {
			return err
		}
	}

	// Add annotations table if it doesn't exist
	var hasAnnotationsTable bool
	err = pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_name = 'annotations'
		)
	`).Scan(&hasAnnotationsTable)
	if err != nil {
		return err
	}

	if !hasAnnotationsTable {
		_, err = pool.Exec(ctx, `
			CREATE TABLE annotations (
				id SERIAL PRIMARY KEY,
				change_id INT NOT NULL UNIQUE REFERENCES changes(id) ON DELETE CASCADE,
				content TEXT NOT NULL,
				created_by TEXT NOT NULL DEFAULT '',
				created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				updated_by TEXT,
				updated_at TIMESTAMPTZ
			);
			CREATE INDEX idx_annotations_change_id ON annotations(change_id);
		`)
		if err != nil {
			return err
		}
	}

	// Multi-cluster support: Add cluster_id column to snapshots table
	if err := addClusterIDColumn(ctx, pool, "snapshots"); err != nil {
		return err
	}

	// Multi-cluster support: Add cluster_id column to changes table
	if err := addClusterIDColumn(ctx, pool, "changes"); err != nil {
		return err
	}

	// Multi-cluster support: Migrate metadata table to support per-cluster metadata
	if err := migrateMetadataForMultiCluster(ctx, pool); err != nil {
		return err
	}

	return nil
}

// addClusterIDColumn adds cluster_id column to a table if it doesn't exist.
// Existing rows get 'default' as their cluster_id for backward compatibility.
func addClusterIDColumn(ctx context.Context, pool *pgxpool.Pool, tableName string) error {
	// Use ADD COLUMN IF NOT EXISTS for idempotency (handles concurrent migrations)
	_, err := pool.Exec(ctx, "ALTER TABLE "+tableName+" ADD COLUMN IF NOT EXISTS cluster_id TEXT NOT NULL DEFAULT 'default'")
	if err != nil {
		return err
	}

	// Create index for efficient cluster-based queries
	indexName := "idx_" + tableName + "_cluster"
	orderColumn := "collected_at"
	if tableName == "changes" {
		orderColumn = "detected_at"
	}
	_, err = pool.Exec(ctx, "CREATE INDEX IF NOT EXISTS "+indexName+" ON "+tableName+"(cluster_id, "+orderColumn+" DESC)")
	if err != nil {
		return err
	}

	return nil
}

// migrateMetadataForMultiCluster adds cluster_id to metadata table and updates primary key.
func migrateMetadataForMultiCluster(ctx context.Context, pool *pgxpool.Pool) error {
	// Use ADD COLUMN IF NOT EXISTS for idempotency
	_, err := pool.Exec(ctx, "ALTER TABLE metadata ADD COLUMN IF NOT EXISTS cluster_id TEXT NOT NULL DEFAULT 'default'")
	if err != nil {
		return err
	}

	// Check if primary key already includes cluster_id
	var pkIncludesClusterID bool
	err = pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.key_column_usage
			WHERE table_name = 'metadata'
			AND column_name = 'cluster_id'
			AND constraint_name = 'metadata_pkey'
		)
	`).Scan(&pkIncludesClusterID)
	if err != nil {
		return err
	}

	if !pkIncludesClusterID {
		// Drop old primary key and add new composite primary key in one statement
		// CockroachDB requires both operations in the same ALTER TABLE statement
		_, err = pool.Exec(ctx, "ALTER TABLE metadata DROP CONSTRAINT metadata_pkey, ADD PRIMARY KEY (cluster_id, key)")
		if err != nil {
			// Ignore error if another connection already migrated the PK
			// This can happen with concurrent test execution
			if !isConstraintAlreadyExists(err) {
				return err
			}
		}
	}

	return nil
}

// isConstraintAlreadyExists checks if the error indicates a constraint already exists
func isConstraintAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "already exists") || strings.Contains(errStr, "42710")
}

func (s *Store) GetLatestSnapshot(ctx context.Context, clusterID string) (map[string]Setting, error) {
	return s.getLatestSnapshotWith(ctx, s.pool, clusterID)
}

// getLatestSnapshotWith retrieves the latest snapshot using the provided querier.
// This allows the same logic to be used with either a pool or a transaction.
func (s *Store) getLatestSnapshotWith(ctx context.Context, q querier, clusterID string) (map[string]Setting, error) {
	var snapshotID int64
	err := q.QueryRow(ctx,
		"SELECT id FROM snapshots WHERE cluster_id = $1 ORDER BY collected_at DESC LIMIT 1",
		clusterID,
	).Scan(&snapshotID)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	rows, err := q.Query(ctx,
		"SELECT variable, value, setting_type, description FROM settings WHERE snapshot_id = $1",
		snapshotID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	settings := make(map[string]Setting)
	for rows.Next() {
		var setting Setting
		if err := rows.Scan(&setting.Variable, &setting.Value, &setting.SettingType, &setting.Description); err != nil {
			return nil, err
		}
		settings[setting.Variable] = setting
	}

	return settings, rows.Err()
}

// ListSnapshots returns recent snapshots for a cluster, ordered by most recent first.
func (s *Store) ListSnapshots(ctx context.Context, clusterID string, limit int) ([]SnapshotInfo, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, cluster_id, collected_at
		 FROM snapshots
		 WHERE cluster_id = $1
		 ORDER BY collected_at DESC
		 LIMIT $2`,
		clusterID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var snapshots []SnapshotInfo
	for rows.Next() {
		var snap SnapshotInfo
		if err := rows.Scan(&snap.ID, &snap.ClusterID, &snap.CollectedAt); err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snap)
	}
	return snapshots, rows.Err()
}

// GetSnapshotByID retrieves all settings for a specific snapshot by its ID.
// Returns nil, nil if the snapshot does not exist.
func (s *Store) GetSnapshotByID(ctx context.Context, snapshotID int64) (map[string]Setting, error) {
	// First verify the snapshot exists
	var exists bool
	err := s.pool.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM snapshots WHERE id = $1)",
		snapshotID,
	).Scan(&exists)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}

	rows, err := s.pool.Query(ctx,
		"SELECT variable, value, setting_type, description FROM settings WHERE snapshot_id = $1",
		snapshotID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	settings := make(map[string]Setting)
	for rows.Next() {
		var setting Setting
		if err := rows.Scan(&setting.Variable, &setting.Value, &setting.SettingType, &setting.Description); err != nil {
			return nil, err
		}
		settings[setting.Variable] = setting
	}
	return settings, rows.Err()
}

func (s *Store) SaveSnapshot(ctx context.Context, clusterID string, settings []Setting, version string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	now := time.Now()

	// Get previous settings for comparison (inside transaction to avoid race condition)
	prevSettings, err := s.getLatestSnapshotWith(ctx, tx, clusterID)
	if err != nil {
		return err
	}

	// Create new snapshot
	var snapshotID int64
	err = tx.QueryRow(ctx,
		"INSERT INTO snapshots (cluster_id, collected_at) VALUES ($1, $2) RETURNING id",
		clusterID, now,
	).Scan(&snapshotID)
	if err != nil {
		return err
	}

	// Insert all settings using batch for efficiency
	batch := &pgx.Batch{}
	currentSettings := make(map[string]Setting)
	for _, setting := range settings {
		batch.Queue(
			"INSERT INTO settings (snapshot_id, variable, value, setting_type, description) VALUES ($1, $2, $3, $4, $5)",
			snapshotID, setting.Variable, setting.Value, setting.SettingType, setting.Description,
		)
		currentSettings[setting.Variable] = setting
	}

	// Check for modified or new settings
	for variable, current := range currentSettings {
		if prev, exists := prevSettings[variable]; exists {
			if prev.Value != current.Value {
				batch.Queue(
					"INSERT INTO changes (cluster_id, detected_at, variable, old_value, new_value, description, version) VALUES ($1, $2, $3, $4, $5, $6, $7)",
					clusterID, now, variable, prev.Value, current.Value, current.Description, version,
				)
			}
		} else if prevSettings != nil {
			// New setting (only record if we had previous snapshot)
			batch.Queue(
				"INSERT INTO changes (cluster_id, detected_at, variable, old_value, new_value, description, version) VALUES ($1, $2, $3, $4, $5, $6, $7)",
				clusterID, now, variable, nil, current.Value, current.Description, version,
			)
		}
	}

	// Check for removed settings
	for variable, prev := range prevSettings {
		if _, exists := currentSettings[variable]; !exists {
			batch.Queue(
				"INSERT INTO changes (cluster_id, detected_at, variable, old_value, new_value, description, version) VALUES ($1, $2, $3, $4, $5, $6, $7)",
				clusterID, now, variable, prev.Value, nil, prev.Description, version,
			)
		}
	}

	// Execute batch
	br := tx.SendBatch(ctx, batch)
	if err := br.Close(); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *Store) GetChanges(ctx context.Context, clusterID string, limit int) ([]Change, error) {
	rows, err := s.pool.Query(ctx,
		"SELECT cluster_id, detected_at, variable, old_value, new_value, description, version FROM changes WHERE cluster_id = $1 ORDER BY detected_at DESC LIMIT $2",
		clusterID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var changes []Change
	for rows.Next() {
		var c Change
		var oldValue, newValue, description, version *string
		if err := rows.Scan(&c.ClusterID, &c.DetectedAt, &c.Variable, &oldValue, &newValue, &description, &version); err != nil {
			return nil, err
		}
		if oldValue != nil {
			c.OldValue = *oldValue
		}
		if newValue != nil {
			c.NewValue = *newValue
		}
		if description != nil {
			c.Description = *description
		}
		if version != nil {
			c.Version = *version
		}
		changes = append(changes, c)
	}

	return changes, rows.Err()
}

// GetAllChanges retrieves changes for all clusters (used for export).
func (s *Store) GetAllChanges(ctx context.Context, limit int) ([]Change, error) {
	rows, err := s.pool.Query(ctx,
		"SELECT cluster_id, detected_at, variable, old_value, new_value, description, version FROM changes ORDER BY detected_at DESC LIMIT $1",
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var changes []Change
	for rows.Next() {
		var c Change
		var oldValue, newValue, description, version *string
		if err := rows.Scan(&c.ClusterID, &c.DetectedAt, &c.Variable, &oldValue, &newValue, &description, &version); err != nil {
			return nil, err
		}
		if oldValue != nil {
			c.OldValue = *oldValue
		}
		if newValue != nil {
			c.NewValue = *newValue
		}
		if description != nil {
			c.Description = *description
		}
		if version != nil {
			c.Version = *version
		}
		changes = append(changes, c)
	}

	return changes, rows.Err()
}

// CleanupOldSnapshots removes snapshots older than the specified duration for a specific cluster.
// Associated settings are automatically deleted via ON DELETE CASCADE.
func (s *Store) CleanupOldSnapshots(ctx context.Context, clusterID string, retention time.Duration) (int64, error) {
	cutoff := time.Now().Add(-retention)
	result, err := s.pool.Exec(ctx,
		"DELETE FROM snapshots WHERE cluster_id = $1 AND collected_at < $2",
		clusterID, cutoff,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

// CleanupOldChanges removes change records older than the specified duration for a specific cluster.
func (s *Store) CleanupOldChanges(ctx context.Context, clusterID string, retention time.Duration) (int64, error) {
	cutoff := time.Now().Add(-retention)
	result, err := s.pool.Exec(ctx,
		"DELETE FROM changes WHERE cluster_id = $1 AND detected_at < $2",
		clusterID, cutoff,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

// SetMetadata stores a key-value pair in the metadata table for a specific cluster.
func (s *Store) SetMetadata(ctx context.Context, clusterID, key, value string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO metadata (cluster_id, key, value, updated_at) VALUES ($1, $2, $3, NOW())
		 ON CONFLICT (cluster_id, key) DO UPDATE SET value = $3, updated_at = NOW()`,
		clusterID, key, value,
	)
	return err
}

// GetMetadata retrieves a value from the metadata table for a specific cluster.
func (s *Store) GetMetadata(ctx context.Context, clusterID, key string) (string, error) {
	var value string
	err := s.pool.QueryRow(ctx,
		"SELECT value FROM metadata WHERE cluster_id = $1 AND key = $2",
		clusterID, key,
	).Scan(&value)
	if err == pgx.ErrNoRows {
		return "", nil
	}
	return value, err
}

// GetSourceClusterID retrieves the source cluster's unique ID (from crdb_internal.cluster_id()).
func (s *Store) GetSourceClusterID(ctx context.Context, clusterID string) (string, error) {
	return s.GetMetadata(ctx, clusterID, "source_cluster_id")
}

// SetSourceClusterID stores the source cluster's unique ID.
func (s *Store) SetSourceClusterID(ctx context.Context, clusterID, sourceClusterID string) error {
	return s.SetMetadata(ctx, clusterID, "source_cluster_id", sourceClusterID)
}

// GetDatabaseVersion retrieves the stored database version for a specific cluster.
func (s *Store) GetDatabaseVersion(ctx context.Context, clusterID string) (string, error) {
	return s.GetMetadata(ctx, clusterID, "database_version")
}

// SetDatabaseVersion stores the database version for a specific cluster.
func (s *Store) SetDatabaseVersion(ctx context.Context, clusterID, version string) error {
	return s.SetMetadata(ctx, clusterID, "database_version", version)
}

// ListClusters returns all distinct cluster IDs that have data.
func (s *Store) ListClusters(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT DISTINCT cluster_id FROM (
			SELECT cluster_id FROM snapshots
			UNION
			SELECT cluster_id FROM changes
			UNION
			SELECT cluster_id FROM metadata
		) ORDER BY cluster_id`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var clusters []string
	for rows.Next() {
		var clusterID string
		if err := rows.Scan(&clusterID); err != nil {
			return nil, err
		}
		clusters = append(clusters, clusterID)
	}

	return clusters, rows.Err()
}

// WriteChangesCSV writes changes to a CSV format.
func WriteChangesCSV(w io.Writer, clusterID string, changes []Change) error {
	csvWriter := csv.NewWriter(w)
	defer csvWriter.Flush()

	header := []string{"cluster_id", "detected_at", "variable", "version", "old_value", "new_value", "description"}
	if err := csvWriter.Write(header); err != nil {
		return err
	}

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
			return err
		}
	}

	return csvWriter.Error()
}

// CreateAnnotation creates a new annotation for a change.
// Returns the created annotation with its ID populated.
func (s *Store) CreateAnnotation(ctx context.Context, changeID int64, content, createdBy string) (*Annotation, error) {
	var a Annotation
	err := s.pool.QueryRow(ctx,
		`INSERT INTO annotations (change_id, content, created_by, created_at)
		 VALUES ($1, $2, $3, NOW())
		 RETURNING id, change_id, content, created_by, created_at`,
		changeID, content, createdBy,
	).Scan(&a.ID, &a.ChangeID, &a.Content, &a.CreatedBy, &a.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &a, nil
}

// GetAnnotation retrieves an annotation by its ID.
func (s *Store) GetAnnotation(ctx context.Context, id int64) (*Annotation, error) {
	var a Annotation
	var updatedBy *string
	var updatedAt *time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT id, change_id, content, created_by, created_at, updated_by, updated_at
		 FROM annotations WHERE id = $1`,
		id,
	).Scan(&a.ID, &a.ChangeID, &a.Content, &a.CreatedBy, &a.CreatedAt, &updatedBy, &updatedAt)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if updatedBy != nil {
		a.UpdatedBy = *updatedBy
	}
	if updatedAt != nil {
		a.UpdatedAt = *updatedAt
	}
	return &a, nil
}

// GetAnnotationByChangeID retrieves an annotation for a specific change.
func (s *Store) GetAnnotationByChangeID(ctx context.Context, changeID int64) (*Annotation, error) {
	var a Annotation
	var updatedBy *string
	var updatedAt *time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT id, change_id, content, created_by, created_at, updated_by, updated_at
		 FROM annotations WHERE change_id = $1`,
		changeID,
	).Scan(&a.ID, &a.ChangeID, &a.Content, &a.CreatedBy, &a.CreatedAt, &updatedBy, &updatedAt)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if updatedBy != nil {
		a.UpdatedBy = *updatedBy
	}
	if updatedAt != nil {
		a.UpdatedAt = *updatedAt
	}
	return &a, nil
}

// UpdateAnnotation updates an existing annotation.
func (s *Store) UpdateAnnotation(ctx context.Context, id int64, content, updatedBy string) error {
	result, err := s.pool.Exec(ctx,
		`UPDATE annotations SET content = $1, updated_by = $2, updated_at = NOW()
		 WHERE id = $3`,
		content, updatedBy, id,
	)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// DeleteAnnotation removes an annotation.
func (s *Store) DeleteAnnotation(ctx context.Context, id int64) error {
	result, err := s.pool.Exec(ctx,
		`DELETE FROM annotations WHERE id = $1`,
		id,
	)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// GetChangesWithAnnotations retrieves changes with their annotations using a LEFT JOIN.
func (s *Store) GetChangesWithAnnotations(ctx context.Context, clusterID string, limit int) ([]ChangeWithAnnotation, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT c.id, c.cluster_id, c.detected_at, c.variable, c.old_value, c.new_value, c.description, c.version,
		        a.id, a.content, a.created_by, a.created_at, a.updated_by, a.updated_at
		 FROM changes c
		 LEFT JOIN annotations a ON a.change_id = c.id
		 WHERE c.cluster_id = $1
		 ORDER BY c.detected_at DESC
		 LIMIT $2`,
		clusterID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []ChangeWithAnnotation
	for rows.Next() {
		var cwa ChangeWithAnnotation
		var oldValue, newValue, description, version *string
		var annID *int64
		var annContent, annCreatedBy, annUpdatedBy *string
		var annCreatedAt, annUpdatedAt *time.Time

		err := rows.Scan(
			&cwa.ID, &cwa.ClusterID, &cwa.DetectedAt, &cwa.Variable, &oldValue, &newValue, &description, &version,
			&annID, &annContent, &annCreatedBy, &annCreatedAt, &annUpdatedBy, &annUpdatedAt,
		)
		if err != nil {
			return nil, err
		}

		if oldValue != nil {
			cwa.OldValue = *oldValue
		}
		if newValue != nil {
			cwa.NewValue = *newValue
		}
		if description != nil {
			cwa.Description = *description
		}
		if version != nil {
			cwa.Version = *version
		}

		// Only populate annotation if it exists
		if annID != nil {
			cwa.Annotation = &Annotation{
				ID:        *annID,
				ChangeID:  cwa.ID,
				Content:   *annContent,
				CreatedBy: *annCreatedBy,
				CreatedAt: *annCreatedAt,
			}
			if annUpdatedBy != nil {
				cwa.Annotation.UpdatedBy = *annUpdatedBy
			}
			if annUpdatedAt != nil {
				cwa.Annotation.UpdatedAt = *annUpdatedAt
			}
		}

		results = append(results, cwa)
	}

	return results, rows.Err()
}

// GetLatestSettings retrieves the current settings for a cluster (for comparison).
func (s *Store) GetLatestSettings(ctx context.Context, clusterID string) (map[string]Setting, error) {
	return s.GetLatestSnapshot(ctx, clusterID)
}
