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

func TestRunInitInsecureMode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	adminURL := getAdminURL(t)

	// Use unique names to avoid conflicts
	timestamp := time.Now().Format("20060102150405")
	cfg := InitConfig{
		AdminURL:     adminURL,
		DatabaseName: "test_init_db_" + timestamp,
		Username:     "test_init_user_" + timestamp,
		Password:     "", // Insecure mode
	}

	err := RunInit(ctx, cfg)
	if err != nil {
		t.Fatalf("RunInit failed: %v", err)
	}

	// Verify database was created
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

	// Verify user was created
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

	// Cleanup
	_, _ = conn.Exec(ctx, "DROP DATABASE IF EXISTS "+cfg.DatabaseName)
	_, _ = conn.Exec(ctx, "DROP USER IF EXISTS "+cfg.Username)
}

func TestRunInitIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	adminURL := getAdminURL(t)

	timestamp := time.Now().Format("20060102150405")
	cfg := InitConfig{
		AdminURL:     adminURL,
		DatabaseName: "test_idempotent_db_" + timestamp,
		Username:     "test_idempotent_user_" + timestamp,
		Password:     "",
	}

	// Run init twice - should not fail
	err := RunInit(ctx, cfg)
	if err != nil {
		t.Fatalf("First RunInit failed: %v", err)
	}

	err = RunInit(ctx, cfg)
	if err != nil {
		t.Fatalf("Second RunInit failed: %v", err)
	}

	// Cleanup
	conn, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("Failed to connect for cleanup: %v", err)
	}
	defer conn.Close(ctx)
	_, _ = conn.Exec(ctx, "DROP DATABASE IF EXISTS "+cfg.DatabaseName)
	_, _ = conn.Exec(ctx, "DROP USER IF EXISTS "+cfg.Username)
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

	// This test just ensures the function doesn't panic
	result := isInsecureMode(ctx, conn)
	t.Logf("isInsecureMode returned: %v", result)

	// If we're connecting with sslmode=disable, it should detect insecure mode
	if conn.Config().TLSConfig == nil {
		if !result {
			t.Log("Warning: Expected insecure mode detection when TLS is disabled")
		}
	}
}
