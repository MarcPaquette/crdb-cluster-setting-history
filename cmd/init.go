package cmd

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"
)

type InitConfig struct {
	AdminURL     string // Connection URL with admin privileges
	DatabaseName string // Name of the history database to create
	Username     string // Username for the history user
	Password     string // Password for the history user (optional in insecure mode)
}

func RunInit(ctx context.Context, cfg InitConfig) error {
	log.Printf("Connecting to CockroachDB as admin...")

	conn, err := pgx.Connect(ctx, cfg.AdminURL)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close(ctx)

	// Check if running in insecure mode
	insecureMode := isInsecureMode(ctx, conn)
	if insecureMode {
		log.Printf("Detected insecure mode - passwords will not be set")
	}

	// Create database
	log.Printf("Creating database %q...", cfg.DatabaseName)
	_, err = conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", pgx.Identifier{cfg.DatabaseName}.Sanitize()))
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	// Create user
	log.Printf("Creating user %q...", cfg.Username)
	// Check if user exists first
	var exists bool
	err = conn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM [SHOW USERS] WHERE username = $1)", cfg.Username).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check user existence: %w", err)
	}

	if exists {
		log.Printf("User %q already exists", cfg.Username)
		if !insecureMode && cfg.Password != "" {
			log.Printf("Updating password for user %q...", cfg.Username)
			_, err = conn.Exec(ctx, fmt.Sprintf("ALTER USER %s WITH PASSWORD $1", pgx.Identifier{cfg.Username}.Sanitize()), cfg.Password)
			if err != nil {
				return fmt.Errorf("failed to update user password: %w", err)
			}
		}
	} else {
		if insecureMode || cfg.Password == "" {
			// Create user without password in insecure mode
			_, err = conn.Exec(ctx, fmt.Sprintf("CREATE USER IF NOT EXISTS %s", pgx.Identifier{cfg.Username}.Sanitize()))
		} else {
			_, err = conn.Exec(ctx, fmt.Sprintf("CREATE USER %s WITH PASSWORD $1", pgx.Identifier{cfg.Username}.Sanitize()), cfg.Password)
		}
		if err != nil {
			return fmt.Errorf("failed to create user: %w", err)
		}
	}

	// Grant privileges on database
	log.Printf("Granting privileges on database %q to user %q...", cfg.DatabaseName, cfg.Username)
	_, err = conn.Exec(ctx, fmt.Sprintf("GRANT ALL ON DATABASE %s TO %s",
		pgx.Identifier{cfg.DatabaseName}.Sanitize(),
		pgx.Identifier{cfg.Username}.Sanitize()))
	if err != nil {
		return fmt.Errorf("failed to grant database privileges: %w", err)
	}

	// Switch to the new database and set default privileges
	log.Printf("Setting default privileges...")
	_, err = conn.Exec(ctx, fmt.Sprintf("USE %s", pgx.Identifier{cfg.DatabaseName}.Sanitize()))
	if err != nil {
		log.Printf("Warning: could not switch to database: %v", err)
	} else {
		_, err = conn.Exec(ctx, fmt.Sprintf("ALTER DEFAULT PRIVILEGES GRANT ALL ON TABLES TO %s",
			pgx.Identifier{cfg.Username}.Sanitize()))
		if err != nil {
			// This might fail if not supported, log but continue
			log.Printf("Warning: could not set default privileges: %v", err)
		}
	}

	log.Printf("Initialization complete!")
	log.Printf("")
	log.Printf("Set the following environment variable to connect:")
	if insecureMode {
		log.Printf("  export HISTORY_DATABASE_URL=\"postgresql://%s@<host>:26257/%s?sslmode=disable\"", cfg.Username, cfg.DatabaseName)
	} else {
		log.Printf("  export HISTORY_DATABASE_URL=\"postgresql://%s:<password>@<host>:26257/%s\"", cfg.Username, cfg.DatabaseName)
	}

	return nil
}

// isInsecureMode checks if CockroachDB is running in insecure mode
// by checking if the connection was established without TLS.
func isInsecureMode(_ context.Context, conn *pgx.Conn) bool {
	connConfig := conn.Config()
	return connConfig != nil && connConfig.TLSConfig == nil
}
