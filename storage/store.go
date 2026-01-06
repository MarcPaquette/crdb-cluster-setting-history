package storage

import (
	"context"
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
	DetectedAt time.Time
	Variable   string
	OldValue   string
	NewValue   string
}

type Store struct {
	pool *pgxpool.Pool
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
		new_value TEXT
	);

	CREATE INDEX IF NOT EXISTS idx_changes_detected ON changes(detected_at DESC);
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

	return nil
}

func (s *Store) GetLatestSnapshot(ctx context.Context) (map[string]Setting, error) {
	var snapshotID int64
	err := s.pool.QueryRow(ctx, "SELECT id FROM snapshots ORDER BY collected_at DESC LIMIT 1").Scan(&snapshotID)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
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

// getLatestSnapshotTx retrieves the latest snapshot within a transaction
func (s *Store) getLatestSnapshotTx(ctx context.Context, tx pgx.Tx) (map[string]Setting, error) {
	var snapshotID int64
	err := tx.QueryRow(ctx, "SELECT id FROM snapshots ORDER BY collected_at DESC LIMIT 1").Scan(&snapshotID)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx,
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

func (s *Store) SaveSnapshot(ctx context.Context, settings []Setting) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	now := time.Now()

	// Get previous settings for comparison (inside transaction to avoid race condition)
	prevSettings, err := s.getLatestSnapshotTx(ctx, tx)
	if err != nil {
		return err
	}

	// Create new snapshot
	var snapshotID int64
	err = tx.QueryRow(ctx,
		"INSERT INTO snapshots (collected_at) VALUES ($1) RETURNING id",
		now,
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
					"INSERT INTO changes (detected_at, variable, old_value, new_value) VALUES ($1, $2, $3, $4)",
					now, variable, prev.Value, current.Value,
				)
			}
		} else if prevSettings != nil {
			// New setting (only record if we had previous snapshot)
			batch.Queue(
				"INSERT INTO changes (detected_at, variable, old_value, new_value) VALUES ($1, $2, $3, $4)",
				now, variable, nil, current.Value,
			)
		}
	}

	// Check for removed settings
	if prevSettings != nil {
		for variable, prev := range prevSettings {
			if _, exists := currentSettings[variable]; !exists {
				batch.Queue(
					"INSERT INTO changes (detected_at, variable, old_value, new_value) VALUES ($1, $2, $3, $4)",
					now, variable, prev.Value, nil,
				)
			}
		}
	}

	// Execute batch
	br := tx.SendBatch(ctx, batch)
	if err := br.Close(); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *Store) GetChanges(ctx context.Context, limit int) ([]Change, error) {
	rows, err := s.pool.Query(ctx,
		"SELECT detected_at, variable, old_value, new_value FROM changes ORDER BY detected_at DESC LIMIT $1",
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var changes []Change
	for rows.Next() {
		var c Change
		var oldValue, newValue *string
		if err := rows.Scan(&c.DetectedAt, &c.Variable, &oldValue, &newValue); err != nil {
			return nil, err
		}
		if oldValue != nil {
			c.OldValue = *oldValue
		}
		if newValue != nil {
			c.NewValue = *newValue
		}
		changes = append(changes, c)
	}

	return changes, rows.Err()
}

// CleanupOldSnapshots removes snapshots older than the specified duration.
// Associated settings are automatically deleted via ON DELETE CASCADE.
func (s *Store) CleanupOldSnapshots(ctx context.Context, retention time.Duration) (int64, error) {
	cutoff := time.Now().Add(-retention)
	result, err := s.pool.Exec(ctx,
		"DELETE FROM snapshots WHERE collected_at < $1",
		cutoff,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

// CleanupOldChanges removes change records older than the specified duration.
func (s *Store) CleanupOldChanges(ctx context.Context, retention time.Duration) (int64, error) {
	cutoff := time.Now().Add(-retention)
	result, err := s.pool.Exec(ctx,
		"DELETE FROM changes WHERE detected_at < $1",
		cutoff,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}
