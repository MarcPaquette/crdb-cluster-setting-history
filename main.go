package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cluster-history/cmd"
	"cluster-history/collector"
	"cluster-history/storage"
	"cluster-history/web"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "init" {
		runInit()
		return
	}

	runServer()
}

func runInit() {
	// DATABASE_URL: Connection with admin privileges to create database and user
	adminURL := os.Getenv("DATABASE_URL")
	if adminURL == "" {
		log.Fatal("DATABASE_URL environment variable is required (admin connection)")
	}

	dbName := getEnv("HISTORY_DB_NAME", "cluster_history")
	username := getEnv("HISTORY_USERNAME", "history_user")
	password := os.Getenv("HISTORY_PASSWORD") // Optional in insecure mode

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := cmd.InitConfig{
		AdminURL:     adminURL,
		DatabaseName: dbName,
		Username:     username,
		Password:     password,
	}

	if err := cmd.RunInit(ctx, cfg); err != nil {
		log.Fatalf("Initialization failed: %v", err)
	}
}

func runServer() {
	// DATABASE_URL: Connection to the CockroachDB cluster being monitored (read-only access to SHOW CLUSTER SETTINGS)
	sourceURL := os.Getenv("DATABASE_URL")
	if sourceURL == "" {
		log.Fatal("DATABASE_URL environment variable is required")
	}

	// HISTORY_DATABASE_URL: Connection to store history data (separate database, separate user with read/write access)
	historyURL := os.Getenv("HISTORY_DATABASE_URL")
	if historyURL == "" {
		log.Fatal("HISTORY_DATABASE_URL environment variable is required")
	}

	pollInterval := getEnvDuration("POLL_INTERVAL", 15*time.Minute)
	httpPort := getEnv("HTTP_PORT", "8080")

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize storage (connects to history database)
	store, err := storage.New(ctx, historyURL)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}
	defer store.Close()

	// Initialize web server
	webServer, err := web.New(store)
	if err != nil {
		log.Fatalf("Failed to initialize web server: %v", err)
	}

	// Start collector (reads from source database, writes to history database)
	coll := collector.New(sourceURL, store, pollInterval)
	go coll.Start(ctx)

	// Start HTTP server
	server := &http.Server{
		Addr:    ":" + httpPort,
		Handler: webServer.Handler(),
	}

	go func() {
		log.Printf("Starting web server on http://localhost:%s", httpPort)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	server.Shutdown(shutdownCtx)
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage: %s [command]

Commands:
  init    Initialize the history database and user
  (none)  Run the cluster history server

Environment Variables:
  DATABASE_URL          CockroachDB connection string (required)
                        For 'init': admin connection to create database/user
                        For server: connection to monitored cluster

  HISTORY_DATABASE_URL  Connection to history database (required for server)

  POLL_INTERVAL         Collection interval (default: 15m)
  HTTP_PORT             Web server port (default: 8080)

  For 'init' command:
  HISTORY_DB_NAME       Database name to create (default: cluster_history)
  HISTORY_USERNAME      Username to create (default: history_user)
  HISTORY_PASSWORD      Password for the new user (optional in insecure mode)
`, os.Args[0])
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		d, err := time.ParseDuration(value)
		if err != nil {
			log.Printf("Invalid duration for %s: %v, using default", key, err)
			return defaultValue
		}
		return d
	}
	return defaultValue
}
