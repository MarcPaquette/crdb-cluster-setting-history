package cmd

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
)

type InitConfig struct {
	AdminURL       string // Connection URL with admin privileges
	DatabaseName   string // Name of the history database to create
	Username       string // Username for the history user
	Password       string // Password for the history user (optional in insecure mode)
	SourceUsername string // Username for monitoring the source cluster (optional; receives VIEWCLUSTERMETADATA grant)
}

func RunInit(ctx context.Context, cfg InitConfig) error {
	slog.Info("Connecting to CockroachDB as admin")

	conn, err := pgx.Connect(ctx, cfg.AdminURL)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close(ctx)

	dbName := pgx.Identifier{cfg.DatabaseName}.Sanitize()
	userName := pgx.Identifier{cfg.Username}.Sanitize()

	// Check if running in insecure mode
	insecureMode := isInsecureMode(conn)
	if insecureMode {
		slog.Warn("Insecure mode detected - passwords will not be set")
		slog.Warn("Insecure mode is not recommended for production: connections are not encrypted, authentication may be bypassed")
	}

	// Create database
	slog.Info("Creating database", "database", cfg.DatabaseName)
	_, err = conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName))
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	// Create user
	slog.Info("Creating user", "user", cfg.Username)
	// Check if user exists first
	var exists bool
	err = conn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM [SHOW USERS] WHERE username = $1)", cfg.Username).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check user existence: %w", err)
	}

	if exists {
		slog.Info("User already exists", "user", cfg.Username)
		if !insecureMode && cfg.Password != "" {
			slog.Info("Updating password for user", "user", cfg.Username)
			_, err = conn.Exec(ctx, fmt.Sprintf("ALTER USER %s WITH PASSWORD $1", userName), cfg.Password)
			if err != nil {
				return fmt.Errorf("failed to update user password: %w", err)
			}
		}
	} else {
		if insecureMode || cfg.Password == "" {
			// Create user without password in insecure mode
			_, err = conn.Exec(ctx, fmt.Sprintf("CREATE USER IF NOT EXISTS %s", userName))
		} else {
			_, err = conn.Exec(ctx, fmt.Sprintf("CREATE USER %s WITH PASSWORD $1", userName), cfg.Password)
		}
		if err != nil {
			return fmt.Errorf("failed to create user: %w", err)
		}
	}

	// Grant minimal database-level privileges (least privilege principle)
	// - CONNECT: required to connect to the database
	// - CREATE: required for initial schema migration (creating tables)
	slog.Info("Granting database-level privileges", "database", cfg.DatabaseName, "user", cfg.Username)
	_, err = conn.Exec(ctx, fmt.Sprintf("GRANT CONNECT, CREATE ON DATABASE %s TO %s", dbName, userName))
	if err != nil {
		return fmt.Errorf("failed to grant database privileges: %w", err)
	}

	// Switch to the new database and set default table privileges
	slog.Info("Setting default table privileges (SELECT, INSERT, UPDATE, DELETE only)")
	_, err = conn.Exec(ctx, fmt.Sprintf("USE %s", dbName))
	if err != nil {
		slog.Warn("Could not switch to database", "error", err)
	} else {
		// Grant only data manipulation privileges on tables - not DROP, ALTER, etc.
		_, err = conn.Exec(ctx, fmt.Sprintf("ALTER DEFAULT PRIVILEGES GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %s", userName))
		if err != nil {
			// This might fail if not supported, log but continue
			slog.Warn("Could not set default privileges", "error", err)
		}
	}

	// Grant VIEWCLUSTERMETADATA to the source monitoring user (if specified)
	if cfg.SourceUsername != "" {
		sourceUserName := pgx.Identifier{cfg.SourceUsername}.Sanitize()
		slog.Info("Granting VIEWCLUSTERMETADATA to source monitoring user", "user", cfg.SourceUsername)
		_, err = conn.Exec(ctx, fmt.Sprintf("GRANT SYSTEM VIEWCLUSTERMETADATA TO %s", sourceUserName))
		if err != nil {
			slog.Warn("Could not grant VIEWCLUSTERMETADATA", "user", cfg.SourceUsername, "error", err)
		}
	} else {
		slog.Info("SOURCE_USERNAME not set, skipping VIEWCLUSTERMETADATA grant",
			"hint", "To grant manually: GRANT SYSTEM VIEWCLUSTERMETADATA TO <monitoring_user>")
	}

	slog.Info("Initialization complete")
	if insecureMode {
		slog.Info("Set HISTORY_DATABASE_URL to connect", "example", fmt.Sprintf("postgresql://%s@<host>:26257/%s?sslmode=disable", cfg.Username, cfg.DatabaseName))
	} else {
		slog.Info("Set HISTORY_DATABASE_URL to connect", "example", fmt.Sprintf("postgresql://%s:<password>@<host>:26257/%s", cfg.Username, cfg.DatabaseName))
	}

	return nil
}

// isInsecureMode checks if CockroachDB is running in insecure mode
// by checking if the connection was established without TLS.
func isInsecureMode(conn *pgx.Conn) bool {
	connConfig := conn.Config()
	return connConfig != nil && connConfig.TLSConfig == nil
}
