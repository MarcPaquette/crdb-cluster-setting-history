package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"crdb-cluster-history/cmd"
	"crdb-cluster-history/collector"
	"crdb-cluster-history/internal/testdbsuffix"
	"crdb-cluster-history/storage"

	"github.com/jackc/pgx/v5"
)

func TestFullIntegration(t *testing.T) {
	testClusterID := fmt.Sprintf("integ-%d", time.Now().UnixNano())
	sourceURL := os.Getenv("DATABASE_URL")
	if sourceURL == "" {
		t.Skip("DATABASE_URL not set, skipping integration test")
	}

	// HISTORY_ADMIN_URL points to the history cluster's admin connection.
	// Falls back to DATABASE_URL for single-instance setups.
	historyAdminURL := os.Getenv("HISTORY_ADMIN_URL")
	if historyAdminURL == "" {
		historyAdminURL = sourceURL
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	suffix := testdbsuffix.Suffix()
	dbName := "cluster_history_integ_test" + suffix
	username := "history_integ_user" + suffix

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()

		conn, err := pgx.Connect(cleanupCtx, historyAdminURL)
		if err != nil {
			t.Logf("Cleanup: failed to connect for cleanup: %v", err)
			return
		}
		defer conn.Close(cleanupCtx)

		_, err = conn.Exec(cleanupCtx, "DROP DATABASE IF EXISTS "+pgx.Identifier{dbName}.Sanitize()+" CASCADE")
		if err != nil {
			t.Logf("Cleanup: failed to drop database: %v", err)
		}

		conn.Exec(cleanupCtx, "ALTER DEFAULT PRIVILEGES FOR ROLE root REVOKE ALL ON TABLES FROM "+pgx.Identifier{username}.Sanitize())
		conn.Exec(cleanupCtx, "REVOKE SYSTEM VIEWCLUSTERMETADATA FROM "+pgx.Identifier{username}.Sanitize())
		_, err = conn.Exec(cleanupCtx, "DROP USER IF EXISTS "+pgx.Identifier{username}.Sanitize())
		if err != nil {
			t.Logf("Cleanup: failed to drop user: %v", err)
		}

		t.Log("Cleanup: test database and user removed")
	})

	t.Log("Step 1: Initializing database and user...")
	initCfg := cmd.InitConfig{
		AdminURL:     historyAdminURL,
		DatabaseName: dbName,
		Username:     username,
		Password:     "",
	}

	if err := cmd.RunInit(ctx, initCfg); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	t.Log("Step 2: Connecting to history database...")
	historyURL := replaceDBUserAndName(historyAdminURL, username, dbName)
	store, err := storage.New(ctx, historyURL)
	if err != nil {
		t.Fatalf("Failed to connect to history database: %v", err)
	}
	defer store.Close()

	t.Log("Step 3: Running collector...")
	coll, err := collector.New(ctx, testClusterID, sourceURL, store, time.Hour)
	if err != nil {
		t.Fatalf("Failed to create collector: %v", err)
	}
	defer coll.Close()

	if err := coll.Collect(ctx); err != nil {
		t.Fatalf("First collection failed: %v", err)
	}

	t.Log("Step 4: Verifying stored data...")
	snapshot, err := store.GetLatestSnapshot(ctx, testClusterID)
	if err != nil {
		t.Fatalf("Failed to get latest snapshot: %v", err)
	}
	if len(snapshot) == 0 {
		t.Fatal("Expected snapshot to have settings after collection")
	}
	t.Logf("First snapshot contains %d settings", len(snapshot))

	t.Log("Step 5: Running second collection...")
	if err := coll.Collect(ctx); err != nil {
		t.Fatalf("Second collection failed: %v", err)
	}

	changes, err := store.GetChanges(ctx, testClusterID, 10)
	if err != nil {
		t.Fatalf("Failed to get changes: %v", err)
	}
	t.Logf("Found %d changes after two collections", len(changes))

	count := 0
	for variable, setting := range snapshot {
		if count >= 3 {
			break
		}
		t.Logf("  %s = %s", variable, setting.Value)
		count++
	}
}

// TestFreshMigrationCompletes verifies that init + migration on a fresh database
// finishes within a tight timeout, catching any DDL hang regressions.
func TestFreshMigrationCompletes(t *testing.T) {
	sourceURL := os.Getenv("DATABASE_URL")
	if sourceURL == "" {
		t.Skip("DATABASE_URL not set, skipping migration test")
	}

	historyAdminURL := os.Getenv("HISTORY_ADMIN_URL")
	if historyAdminURL == "" {
		historyAdminURL = sourceURL
	}

	// Tight timeout — fresh migration should complete in seconds, not minutes.
	// If this test hangs, it means DDL contention has regressed.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	suffix := testdbsuffix.Suffix()
	dbName := "cluster_history_migration_test" + suffix
	username := "history_migration_user" + suffix

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()

		conn, err := pgx.Connect(cleanupCtx, historyAdminURL)
		if err != nil {
			t.Logf("Cleanup: failed to connect: %v", err)
			return
		}
		defer conn.Close(cleanupCtx)

		conn.Exec(cleanupCtx, "DROP DATABASE IF EXISTS "+pgx.Identifier{dbName}.Sanitize()+" CASCADE")
		conn.Exec(cleanupCtx, "ALTER DEFAULT PRIVILEGES FOR ROLE root REVOKE ALL ON TABLES FROM "+pgx.Identifier{username}.Sanitize())
		conn.Exec(cleanupCtx, "DROP USER IF EXISTS "+pgx.Identifier{username}.Sanitize())
	})

	// Step 1: Init creates the database, user, and grants.
	t.Log("Step 1: Running init on fresh database...")
	initStart := time.Now()
	initCfg := cmd.InitConfig{
		AdminURL:     historyAdminURL,
		DatabaseName: dbName,
		Username:     username,
		Password:     "",
	}
	if err := cmd.RunInit(ctx, initCfg); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	t.Logf("Init completed in %v", time.Since(initStart))

	// Step 2: storage.New triggers initAndMigrate — the step that previously hung.
	t.Log("Step 2: Connecting via storage.New (triggers migration)...")
	migrateStart := time.Now()
	historyURL := replaceDBUserAndName(historyAdminURL, username, dbName)
	store, err := storage.New(ctx, historyURL)
	if err != nil {
		t.Fatalf("storage.New failed (migration hang?): %v", err)
	}
	defer store.Close()
	t.Logf("Migration completed in %v", time.Since(migrateStart))

	// Step 3: Verify all expected tables exist.
	t.Log("Step 3: Verifying all tables were created...")
	conn, err := pgx.Connect(ctx, historyURL)
	if err != nil {
		t.Fatalf("Failed to connect to verify tables: %v", err)
	}
	defer conn.Close(ctx)

	expectedTables := []string{
		"schema_migrations",
		"snapshots",
		"settings",
		"changes",
		"metadata",
		"annotations",
	}
	for _, table := range expectedTables {
		var exists bool
		err := conn.QueryRow(ctx,
			"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
			table,
		).Scan(&exists)
		if err != nil {
			t.Fatalf("Failed to check table %s: %v", table, err)
		}
		if !exists {
			t.Errorf("Expected table %s to exist after migration", table)
		}
	}

	// Step 4: Verify migration version is current.
	var maxVersion int
	err = conn.QueryRow(ctx, "SELECT COALESCE(MAX(version), 0) FROM schema_migrations").Scan(&maxVersion)
	if err != nil {
		t.Fatalf("Failed to read migration version: %v", err)
	}
	if maxVersion != 6 {
		t.Errorf("Expected migration version 6, got %d", maxVersion)
	}
	t.Logf("Migration version: %d", maxVersion)
}

// TestInitCreatesTables verifies that RunInit creates all schema tables,
// not just the database and user. The fleet-test script depends on this.
func TestInitCreatesTables(t *testing.T) {
	sourceURL := os.Getenv("DATABASE_URL")
	if sourceURL == "" {
		t.Skip("DATABASE_URL not set")
	}
	historyAdminURL := os.Getenv("HISTORY_ADMIN_URL")
	if historyAdminURL == "" {
		historyAdminURL = sourceURL
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	suffix := testdbsuffix.Suffix()
	dbName := "cluster_history_init_tables_test" + suffix
	username := "history_init_tables_user" + suffix

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		conn, err := pgx.Connect(cleanupCtx, historyAdminURL)
		if err != nil {
			return
		}
		defer conn.Close(cleanupCtx)
		conn.Exec(cleanupCtx, "DROP DATABASE IF EXISTS "+pgx.Identifier{dbName}.Sanitize()+" CASCADE")
		conn.Exec(cleanupCtx, "ALTER DEFAULT PRIVILEGES FOR ROLE root REVOKE ALL ON TABLES FROM "+pgx.Identifier{username}.Sanitize())
		conn.Exec(cleanupCtx, "DROP USER IF EXISTS "+pgx.Identifier{username}.Sanitize())
	})

	if err := cmd.RunInit(ctx, cmd.InitConfig{
		AdminURL:     historyAdminURL,
		DatabaseName: dbName,
		Username:     username,
	}); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	// Verify tables exist WITHOUT calling storage.New — init alone should create them.
	adminHistoryURL := replaceDBName(historyAdminURL, dbName)
	conn, err := pgx.Connect(ctx, adminHistoryURL)
	if err != nil {
		t.Fatalf("Failed to connect to history DB: %v", err)
	}
	defer conn.Close(ctx)

	for _, table := range []string{"schema_migrations", "snapshots", "settings", "changes", "metadata", "annotations"} {
		var exists bool
		err := conn.QueryRow(ctx,
			"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
			table,
		).Scan(&exists)
		if err != nil {
			t.Fatalf("Failed to check table %s: %v", table, err)
		}
		if !exists {
			t.Errorf("Expected table %s to exist after init", table)
		}
	}
}

// replaceDBName builds a connection URL from an admin URL by replacing only
// the database name, preserving the user, host, port, and query params.
func replaceDBName(adminURL, dbName string) string {
	const prefix = "postgresql://"
	if !strings.HasPrefix(adminURL, prefix) {
		return adminURL
	}
	rest := adminURL[len(prefix):]
	atIdx := strings.Index(rest, "@")
	if atIdx == -1 {
		return adminURL
	}
	user := rest[:atIdx]
	return replaceDBUserAndName(adminURL, user, dbName)
}

// replaceDBUserAndName builds a connection URL from an admin URL by replacing
// the username and database name, preserving the host, port, and query params.
func replaceDBUserAndName(adminURL, user, dbName string) string {
	// Parse: postgresql://root@localhost:26258/defaultdb?sslmode=disable
	// Result: postgresql://user@localhost:26258/dbName?sslmode=disable
	const prefix = "postgresql://"
	if !strings.HasPrefix(adminURL, prefix) {
		return adminURL
	}
	rest := adminURL[len(prefix):]
	atIdx := strings.Index(rest, "@")
	if atIdx == -1 {
		return adminURL
	}
	hostAndPath := rest[atIdx+1:]
	slashIdx := strings.Index(hostAndPath, "/")
	if slashIdx == -1 {
		return prefix + user + "@" + hostAndPath + "/" + dbName
	}
	host := hostAndPath[:slashIdx]
	afterDB := hostAndPath[slashIdx+1:]
	query := ""
	if qIdx := strings.Index(afterDB, "?"); qIdx != -1 {
		query = afterDB[qIdx:]
	}
	return prefix + user + "@" + host + "/" + dbName + query
}
