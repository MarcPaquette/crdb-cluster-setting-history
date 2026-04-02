package storage

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
)

// migration represents a single schema migration step.
type migration struct {
	version     int
	description string
	sql         string
}

// migrations is the ordered list of all schema migrations.
// Each migration must be idempotent (use IF NOT EXISTS, ADD COLUMN IF NOT EXISTS, etc.)
// to safely handle concurrent execution across multiple replicas.
var migrations = []migration{
	{
		version:     1,
		description: "create base tables (snapshots, settings, changes, metadata)",
		sql: `
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

			CREATE TABLE IF NOT EXISTS metadata (
				key TEXT PRIMARY KEY,
				value TEXT NOT NULL,
				updated_at TIMESTAMPTZ NOT NULL
			);
		`,
	},
	{
		version:     2,
		description: "add description and version columns to changes",
		sql: `
			ALTER TABLE changes ADD COLUMN IF NOT EXISTS description TEXT;
			ALTER TABLE changes ADD COLUMN IF NOT EXISTS version TEXT;
		`,
	},
	{
		version:     3,
		description: "add annotations table",
		sql: `
			CREATE TABLE IF NOT EXISTS annotations (
				id SERIAL PRIMARY KEY,
				change_id INT NOT NULL UNIQUE REFERENCES changes(id) ON DELETE CASCADE,
				content TEXT NOT NULL,
				created_by TEXT NOT NULL DEFAULT '',
				created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				updated_by TEXT,
				updated_at TIMESTAMPTZ
			);
			CREATE INDEX IF NOT EXISTS idx_annotations_change_id ON annotations(change_id);
		`,
	},
	{
		version:     4,
		description: "add multi-cluster support (cluster_id columns and composite metadata PK)",
		sql: `
			ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS cluster_id TEXT NOT NULL DEFAULT 'default';
			CREATE INDEX IF NOT EXISTS idx_snapshots_cluster ON snapshots(cluster_id, collected_at DESC);

			ALTER TABLE changes ADD COLUMN IF NOT EXISTS cluster_id TEXT NOT NULL DEFAULT 'default';
			CREATE INDEX IF NOT EXISTS idx_changes_cluster ON changes(cluster_id, detected_at DESC);

			ALTER TABLE metadata ADD COLUMN IF NOT EXISTS cluster_id TEXT NOT NULL DEFAULT 'default';
		`,
	},
	{
		version:     5,
		description: "migrate metadata primary key to composite (cluster_id, key)",
		sql: `
			-- This is handled specially in code since it requires checking existing PK structure
		`,
	},
}

// runMigrations applies all pending migrations to the database.
// The schema_migrations table must already exist (created by initAndMigrate).
// All migrations are idempotent, so concurrent execution is safe.
func runMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	currentVersion := 0
	err := pool.QueryRow(ctx, "SELECT COALESCE(MAX(version), 0) FROM schema_migrations").Scan(&currentVersion)
	if err != nil {
		return fmt.Errorf("reading current migration version: %w", err)
	}

	if currentVersion >= len(migrations) {
		slog.Info("Schema is up to date", "version", currentVersion)
		return nil
	}

	for _, m := range migrations {
		if m.version <= currentVersion {
			continue
		}

		slog.Info("Running migration", "version", m.version, "description", m.description)

		if m.version == 5 {
			if err := migrateMetadataPK(ctx, pool); err != nil {
				return fmt.Errorf("migration %d (%s): %w", m.version, m.description, err)
			}
		} else {
			if _, err := pool.Exec(ctx, m.sql); err != nil {
				return fmt.Errorf("migration %d (%s): %w", m.version, m.description, err)
			}
		}

		_, err := pool.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", m.version)
		if err != nil {
			return fmt.Errorf("recording migration %d: %w", m.version, err)
		}
	}

	slog.Info("Migrations complete", "version", len(migrations))
	return nil
}

// migrateMetadataPK handles the metadata table primary key migration.
// This needs special logic because it must check the existing PK structure
// and CockroachDB requires DROP/ADD PK in the same ALTER TABLE statement.
func migrateMetadataPK(ctx context.Context, pool *pgxpool.Pool) error {
	var pkIncludesClusterID bool
	err := pool.QueryRow(ctx, `
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

	if pkIncludesClusterID {
		return nil
	}

	_, err = pool.Exec(ctx, "ALTER TABLE metadata DROP CONSTRAINT metadata_pkey, ADD PRIMARY KEY (cluster_id, key)")
	if err != nil && !isConstraintAlreadyExists(err) {
		return err
	}

	return nil
}

// initAndMigrate creates the migration tracking table, handles existing databases,
// then runs any pending migrations.
func initAndMigrate(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INT NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("creating schema_migrations table: %w", err)
	}

	if err := migrateExistingDB(ctx, pool); err != nil {
		return err
	}

	return runMigrations(ctx, pool)
}

// migrateExistingDB detects databases created before the migration system was introduced
// and records all migrations as applied so they aren't re-run.
// This is needed because existing databases already have the full schema but no schema_migrations records.
func migrateExistingDB(ctx context.Context, pool *pgxpool.Pool) error {
	var migrationCount int
	err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM schema_migrations").Scan(&migrationCount)
	if err != nil {
		return err
	}

	if migrationCount > 0 {
		return nil // Already has migration history
	}

	var hasSnapshots bool
	err = pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_name = 'snapshots'
		)
	`).Scan(&hasSnapshots)
	if err != nil {
		return err
	}

	if !hasSnapshots {
		return nil // Fresh database, migrations will run normally
	}

	slog.Info("Detected existing database, recording migration history")
	for _, m := range migrations {
		_, err := pool.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", m.version)
		if err != nil {
			return fmt.Errorf("recording existing migration %d: %w", m.version, err)
		}
	}

	return nil
}
