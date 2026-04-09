package storage

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

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
//
// Migration 1 creates the full current schema (all columns, indexes, PKs) so
// that fresh databases need only CREATE TABLE — no slow ALTER TABLE cycles.
// Migrations 2-6 exist for databases created by older versions; on a fresh
// database their IF NOT EXISTS / ADD COLUMN IF NOT EXISTS clauses are no-ops.
//
// Indexes are defined inline with CREATE TABLE (not as separate CREATE INDEX)
// to avoid schema change contention on CockroachDB, where a separate DDL
// statement triggers another multi-phase schema change lifecycle.
var migrations = []migration{
	{
		version:     1,
		description: "create base tables (snapshots, settings, changes, metadata, annotations)",
		sql: `
			CREATE TABLE IF NOT EXISTS snapshots (
				id SERIAL PRIMARY KEY,
				collected_at TIMESTAMPTZ NOT NULL,
				cluster_id TEXT NOT NULL DEFAULT 'default',
				INDEX idx_snapshots_cluster (cluster_id, collected_at DESC)
			);

			CREATE TABLE IF NOT EXISTS settings (
				id SERIAL PRIMARY KEY,
				snapshot_id INT REFERENCES snapshots(id) ON DELETE CASCADE,
				variable TEXT NOT NULL,
				value TEXT NOT NULL,
				setting_type TEXT,
				description TEXT,
				INDEX idx_settings_snapshot (snapshot_id)
			);

			CREATE TABLE IF NOT EXISTS changes (
				id SERIAL PRIMARY KEY,
				detected_at TIMESTAMPTZ NOT NULL,
				variable TEXT NOT NULL,
				old_value TEXT,
				new_value TEXT,
				description TEXT,
				version TEXT,
				cluster_id TEXT NOT NULL DEFAULT 'default',
				INDEX idx_changes_detected (detected_at DESC),
				INDEX idx_changes_cluster (cluster_id, detected_at DESC)
			);

			CREATE TABLE IF NOT EXISTS metadata (
				cluster_id TEXT NOT NULL DEFAULT 'default',
				key TEXT NOT NULL,
				value TEXT NOT NULL,
				updated_at TIMESTAMPTZ NOT NULL,
				PRIMARY KEY (cluster_id, key)
			);

			CREATE TABLE IF NOT EXISTS annotations (
				id SERIAL PRIMARY KEY,
				change_id INT NOT NULL UNIQUE REFERENCES changes(id) ON DELETE CASCADE,
				content TEXT NOT NULL,
				created_by TEXT NOT NULL DEFAULT '',
				created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				updated_by TEXT,
				updated_at TIMESTAMPTZ
			);
		`,
	},
	{
		// On fresh databases these columns already exist (created in migration 1).
		version:     2,
		description: "add description and version columns to changes",
		sql: `
			ALTER TABLE changes ADD COLUMN IF NOT EXISTS description TEXT;
			ALTER TABLE changes ADD COLUMN IF NOT EXISTS version TEXT;
		`,
	},
	{
		// On fresh databases this table already exists (created in migration 1).
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
		`,
	},
	{
		// On fresh databases these columns/indexes already exist (created in migration 1).
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
	{
		version:     6,
		description: "drop leftover unique constraint on metadata key column",
		sql: `
			-- When CockroachDB drops a primary key, it auto-creates a secondary UNIQUE
			-- constraint on the old PK columns (named metadata_key_key). This prevents
			-- different clusters from storing the same metadata key. Drop it.
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

	logSchemaChangeJobs(ctx, pool)

	for _, m := range migrations {
		if m.version <= currentVersion {
			continue
		}

		slog.Info("Running migration", "version", m.version, "description", m.description)

		if m.version == 5 {
			if err := migrateMetadataPK(ctx, pool); err != nil {
				return fmt.Errorf("migration %d (%s): %w", m.version, m.description, err)
			}
		} else if m.version == 6 {
			if err := dropMetadataKeyUnique(ctx, pool); err != nil {
				return fmt.Errorf("migration %d (%s): %w", m.version, m.description, err)
			}
		} else {
			if err := execDDL(ctx, pool, m.sql); err != nil {
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

	if err := execDDL(ctx, pool, "ALTER TABLE metadata DROP CONSTRAINT metadata_pkey, ADD PRIMARY KEY (cluster_id, key)"); err != nil && !isConstraintAlreadyExists(err) {
		return err
	}

	return nil
}

// dropMetadataKeyUnique drops the secondary UNIQUE constraint on metadata(key) that
// CockroachDB auto-creates when the old single-column PK is dropped in migration 5.
// This constraint prevents different clusters from using the same metadata key.
func dropMetadataKeyUnique(ctx context.Context, pool *pgxpool.Pool) error {
	var exists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.table_constraints
			WHERE table_name = 'metadata'
			AND constraint_name = 'metadata_key_key'
			AND constraint_type = 'UNIQUE'
		)
	`).Scan(&exists)
	if err != nil {
		return err
	}

	if !exists {
		return nil
	}

	return execDDL(ctx, pool, "DROP INDEX metadata_key_key CASCADE")
}

// splitStatements splits multi-statement SQL on semicolons, returning
// only non-empty, non-comment-only statements.
func splitStatements(sql string) []string {
	var stmts []string
	for _, raw := range strings.Split(sql, ";") {
		s := strings.TrimSpace(raw)
		if s == "" || onlyComments(s) {
			continue
		}
		stmts = append(stmts, s)
	}
	return stmts
}

// execDDL executes DDL SQL using the PostgreSQL simple query protocol.
// Multi-statement SQL is split on semicolons and each statement is executed
// individually with per-statement logging and a per-statement timeout.
// Between statements, it waits for any active schema change jobs to complete
// to avoid contention (CockroachDB serializes schema changes on the same table).
func execDDL(ctx context.Context, pool *pgxpool.Pool, sql string) error {
	stmts := splitStatements(sql)
	if len(stmts) == 0 {
		return nil
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection: %w", err)
	}
	defer conn.Release()

	for i, stmt := range stmts {
		preview := stmtPreview(stmt)
		slog.Info("Executing DDL statement", "step", fmt.Sprintf("%d/%d", i+1, len(stmts)), "sql", preview)

		stmtCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
		start := time.Now()

		// Use PgConn().Exec() — simple query protocol, no Prepare/Parse step.
		_, err := conn.Conn().PgConn().Exec(stmtCtx, stmt).ReadAll()
		elapsed := time.Since(start)
		cancel()

		if err != nil {
			slog.Error("DDL statement failed", "sql", preview, "elapsed", elapsed, "error", err)
			logSchemaChangeJobs(ctx, pool)
			return fmt.Errorf("executing %q: %w", preview, err)
		}
		slog.Info("DDL statement completed", "sql", preview, "elapsed", elapsed)

		// Wait for schema change jobs to finish before the next statement.
		// CockroachDB serializes schema changes on the same table, so a
		// CREATE INDEX right after CREATE TABLE will block until the table's
		// schema change fully propagates through all lease phases.
		if i < len(stmts)-1 {
			waitForSchemaChanges(ctx, conn)
		}
	}
	return nil
}

// waitForSchemaChanges polls until all active schema change jobs complete.
// CockroachDB v22+ uses the declarative schema changer ("NEW SCHEMA CHANGE")
// in addition to the legacy "SCHEMA CHANGE" job type — both must be checked.
func waitForSchemaChanges(ctx context.Context, conn *pgxpool.Conn) {
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	for {
		var count int
		err := conn.QueryRow(waitCtx,
			`SELECT count(*) FROM [SHOW JOBS]
			 WHERE job_type IN ('SCHEMA CHANGE', 'NEW SCHEMA CHANGE')
			   AND status NOT IN ('succeeded', 'canceled', 'failed')`).Scan(&count)
		if err != nil || count == 0 {
			return
		}

		slog.Info("Waiting for schema change jobs to complete", "active_jobs", count)

		select {
		case <-waitCtx.Done():
			slog.Warn("Timed out waiting for schema change jobs", "remaining", count)
			return
		case <-time.After(1 * time.Second):
		}
	}
}

// stmtPreview returns the first meaningful line of a SQL statement for logging.
func stmtPreview(sql string) string {
	for _, line := range strings.Split(sql, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "--") {
			if len(line) > 80 {
				return line[:80] + "..."
			}
			return line
		}
	}
	return "(empty)"
}

// onlyComments returns true if the SQL string contains only -- line comments and whitespace.
func onlyComments(sql string) bool {
	for _, line := range strings.Split(sql, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "--") {
			return false
		}
	}
	return true
}

// logSchemaChangeJobs queries CockroachDB for running or pending schema change
// jobs and logs them. This helps diagnose DDL hangs caused by job contention.
func logSchemaChangeJobs(ctx context.Context, pool *pgxpool.Pool) {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	rows, err := pool.Query(queryCtx,
		`SELECT job_id, job_type, description, status, running_status, created, modified
		 FROM [SHOW JOBS]
		 WHERE job_type IN ('SCHEMA CHANGE', 'NEW SCHEMA CHANGE')
		   AND status NOT IN ('succeeded', 'canceled', 'failed')
		 ORDER BY created DESC
		 LIMIT 10`)
	if err != nil {
		slog.Warn("Could not query schema change jobs", "error", err)
		return
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		var jobID int64
		var jobType, description, status string
		var runningStatus *string
		var created, modified time.Time
		if err := rows.Scan(&jobID, &jobType, &description, &status, &runningStatus, &created, &modified); err != nil {
			slog.Warn("Could not scan schema change job row", "error", err)
			continue
		}
		rs := ""
		if runningStatus != nil {
			rs = *runningStatus
		}
		slog.Warn("Active schema change job found",
			"job_id", jobID,
			"job_type", jobType,
			"status", status,
			"running_status", rs,
			"description", description,
			"created", created,
			"modified", modified,
		)
		found = true
	}
	if !found {
		slog.Info("No active schema change jobs found")
	}
}

// logDatabaseInfo logs diagnostic information about the connected database.
func logDatabaseInfo(ctx context.Context, pool *pgxpool.Pool) {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var version string
	if err := pool.QueryRow(queryCtx, "SELECT version()").Scan(&version); err != nil {
		slog.Warn("Could not query database version", "error", err)
	} else {
		slog.Info("History database version", "version", version)
	}

	var dbName, currentUser string
	if err := pool.QueryRow(queryCtx, "SELECT current_database(), current_user").Scan(&dbName, &currentUser); err != nil {
		slog.Warn("Could not query connection info", "error", err)
	} else {
		slog.Info("History database connection", "database", dbName, "user", currentUser)
	}
}

// Migrate connects to the given database and runs all pending schema migrations.
// This is used by the init command to create tables as part of initialization.
func Migrate(ctx context.Context, connString string) error {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return fmt.Errorf("connecting for migration: %w", err)
	}
	defer pool.Close()
	return initAndMigrate(ctx, pool)
}

// initAndMigrate creates the migration tracking table, handles existing databases,
// then runs any pending migrations.
func initAndMigrate(ctx context.Context, pool *pgxpool.Pool) error {
	logDatabaseInfo(ctx, pool)

	if err := execDDL(ctx, pool, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INT NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`); err != nil {
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
