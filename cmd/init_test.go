package cmd

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func getAdminURL(t *testing.T) string {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set")
	}
	return url
}

func cleanupInitResources(t *testing.T, adminURL, dbName, userName string, sourceUsernames ...string) {
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		conn, err := pgx.Connect(ctx, adminURL)
		if err != nil {
			return
		}
		defer conn.Close(ctx)
		conn.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{dbName}.Sanitize())
		// Revoke default privileges before dropping user
		conn.Exec(ctx, "ALTER DEFAULT PRIVILEGES FOR ROLE root REVOKE ALL ON TABLES FROM "+pgx.Identifier{userName}.Sanitize())
		conn.Exec(ctx, "DROP USER IF EXISTS "+pgx.Identifier{userName}.Sanitize())
		for _, su := range sourceUsernames {
			conn.Exec(ctx, "REVOKE SYSTEM VIEWCLUSTERMETADATA FROM "+pgx.Identifier{su}.Sanitize())
		}
	})
}

func TestRunInitInsecureMode(t *testing.T) {
	adminURL := getAdminURL(t)

	timestamp := time.Now().Format("20060102150405")
	dbName := "test_init_db_" + timestamp
	userName := "test_init_user_" + timestamp

	cleanupInitResources(t, adminURL, dbName, userName)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := InitConfig{
		AdminURL:     adminURL,
		DatabaseName: dbName,
		Username:     userName,
		Password:     "", // Insecure mode
	}

	err := RunInit(ctx, cfg)
	if err != nil {
		t.Fatalf("RunInit failed: %v", err)
	}

	conn, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close(ctx)

	var dbExists bool
	err = conn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM [SHOW DATABASES] WHERE database_name = $1)",
		cfg.DatabaseName,
	).Scan(&dbExists)
	if err != nil {
		t.Fatalf("Failed to check database: %v", err)
	}
	if !dbExists {
		t.Errorf("Database %s was not created", cfg.DatabaseName)
	}

	var userExists bool
	err = conn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM [SHOW USERS] WHERE username = $1)",
		cfg.Username,
	).Scan(&userExists)
	if err != nil {
		t.Fatalf("Failed to check user: %v", err)
	}
	if !userExists {
		t.Errorf("User %s was not created", cfg.Username)
	}
}

func TestRunInitIdempotent(t *testing.T) {
	adminURL := getAdminURL(t)

	timestamp := time.Now().Format("20060102150405")
	dbName := "test_idempotent_db_" + timestamp
	userName := "test_idempotent_user_" + timestamp

	cleanupInitResources(t, adminURL, dbName, userName, userName)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := InitConfig{
		AdminURL:       adminURL,
		DatabaseName:   dbName,
		Username:       userName,
		Password:       "",
		SourceUsername: userName,
	}

	err := RunInit(ctx, cfg)
	if err != nil {
		t.Fatalf("First RunInit failed: %v", err)
	}

	err = RunInit(ctx, cfg)
	if err != nil {
		t.Fatalf("Second RunInit failed: %v", err)
	}
}

func TestRunInitGrantsViewClusterMetadata(t *testing.T) {
	adminURL := getAdminURL(t)

	timestamp := time.Now().Format("20060102150405")
	dbName := "test_grant_db_" + timestamp
	userName := "test_grant_user_" + timestamp

	cleanupInitResources(t, adminURL, dbName, userName, userName)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := InitConfig{
		AdminURL:       adminURL,
		DatabaseName:   dbName,
		Username:       userName,
		Password:       "",
		SourceUsername: userName,
	}

	err := RunInit(ctx, cfg)
	if err != nil {
		t.Fatalf("RunInit failed: %v", err)
	}

	conn, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close(ctx)

	var hasGrant bool
	err = conn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM [SHOW SYSTEM GRANTS] WHERE grantee = $1 AND privilege_type = 'VIEWCLUSTERMETADATA')",
		userName,
	).Scan(&hasGrant)
	if err != nil {
		t.Fatalf("Failed to check system grant: %v", err)
	}
	if !hasGrant {
		t.Errorf("User %s should have VIEWCLUSTERMETADATA grant", userName)
	}
}

func TestRunInitSkipsGrantWhenNoSourceUsername(t *testing.T) {
	adminURL := getAdminURL(t)

	timestamp := time.Now().Format("20060102150405")
	dbName := "test_nogrant_db_" + timestamp
	userName := "test_nogrant_user_" + timestamp

	cleanupInitResources(t, adminURL, dbName, userName)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := InitConfig{
		AdminURL:     adminURL,
		DatabaseName: dbName,
		Username:     userName,
		Password:     "",
	}

	err := RunInit(ctx, cfg)
	if err != nil {
		t.Fatalf("RunInit failed: %v", err)
	}

	conn, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close(ctx)

	var hasGrant bool
	err = conn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM [SHOW SYSTEM GRANTS] WHERE grantee = $1 AND privilege_type = 'VIEWCLUSTERMETADATA')",
		userName,
	).Scan(&hasGrant)
	if err != nil {
		t.Fatalf("Failed to check system grant: %v", err)
	}
	if hasGrant {
		t.Errorf("User %s should NOT have VIEWCLUSTERMETADATA grant when SourceUsername is empty", userName)
	}
}

func TestIsInsecureMode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	adminURL := getAdminURL(t)

	conn, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close(ctx)

	result := isInsecureMode(conn)
	t.Logf("isInsecureMode returned: %v", result)

	if conn.Config().TLSConfig == nil && !result {
		t.Error("Expected insecure mode detection when TLS is disabled")
	}
}
