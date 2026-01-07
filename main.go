package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"crdb-cluster-history/auth"
	"crdb-cluster-history/cmd"
	"crdb-cluster-history/collector"
	"crdb-cluster-history/storage"
	"crdb-cluster-history/web"
)

// Version is set at build time via -ldflags
var Version = "dev"

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "init":
			runInit()
			return
		case "export":
			runExport()
			return
		case "-h", "--help", "help":
			usage()
			return
		case "-v", "--version", "version":
			fmt.Printf("crdb-cluster-history %s\n", Version)
			return
		default:
			fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", os.Args[1])
			usage()
			os.Exit(1)
		}
	}

	runServer()
}

func runExport() {
	sourceURL := os.Getenv("DATABASE_URL")
	if sourceURL == "" {
		log.Fatal("DATABASE_URL environment variable is required")
	}

	historyURL := os.Getenv("HISTORY_DATABASE_URL")
	if historyURL == "" {
		log.Fatal("HISTORY_DATABASE_URL environment variable is required")
	}

	outputPath := ""
	if len(os.Args) > 2 {
		outputPath = os.Args[2]
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cfg := cmd.ExportConfig{
		SourceURL:  sourceURL,
		HistoryURL: historyURL,
		OutputPath: outputPath,
	}

	if err := cmd.RunExport(ctx, cfg); err != nil {
		log.Fatalf("Export failed: %v", err)
	}
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
	retention := getEnvDuration("RETENTION", 0) // 0 means no cleanup
	httpPort := getEnv("HTTP_PORT", "8080")

	// Security configuration
	tlsEnabled := getEnvBool("TLS_ENABLED", false)
	tlsCertFile := os.Getenv("TLS_CERT_FILE")
	tlsKeyFile := os.Getenv("TLS_KEY_FILE")

	// Authentication configuration
	authEnabled := getEnvBool("AUTH_ENABLED", false)
	authCfg := auth.Config{
		Enabled:     authEnabled,
		Username:    getEnv("AUTH_USERNAME", "admin"),
		APIKeys:     auth.ParseAPIKeys(os.Getenv("AUTH_API_KEYS")),
		PublicPaths: auth.ParsePublicPaths(os.Getenv("AUTH_PUBLIC_PATHS")),
	}

	if authEnabled {
		password := os.Getenv("AUTH_PASSWORD")
		if password == "" {
			log.Fatal("AUTH_PASSWORD is required when AUTH_ENABLED=true")
		}
		hash, err := auth.HashPassword(password)
		if err != nil {
			log.Fatalf("Failed to hash password: %v", err)
		}
		authCfg.PasswordHash = hash
		log.Printf("Authentication enabled (user: %s)", authCfg.Username)
	}

	// Rate limiting configuration
	rateLimitEnabled := getEnvBool("RATE_LIMIT_ENABLED", false)
	rateLimiter := web.NewRateLimiter(web.RateLimiterConfig{
		Enabled:           rateLimitEnabled,
		RequestsPerSecond: getEnvFloat("RATE_LIMIT_RPS", 10),
		Burst:             getEnvInt("RATE_LIMIT_BURST", 20),
	})
	if rateLimitEnabled {
		log.Printf("Rate limiting enabled (%.1f req/s, burst %d)", getEnvFloat("RATE_LIMIT_RPS", 10), getEnvInt("RATE_LIMIT_BURST", 20))
	}

	// Redaction configuration
	redactCfg := storage.RedactorConfig{
		Enabled:            getEnvBool("REDACT_SENSITIVE", false),
		AdditionalPatterns: os.Getenv("REDACT_PATTERNS"),
	}
	redactor := storage.NewRedactor(redactCfg)
	if redactCfg.Enabled {
		log.Printf("Sensitive data redaction enabled")
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize storage (connects to history database)
	store, err := storage.New(ctx, historyURL)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}
	defer store.Close()

	// Initialize web server with redactor
	webServer, err := web.New(store, web.WithRedactor(redactor))
	if err != nil {
		log.Fatalf("Failed to initialize web server: %v", err)
	}

	// Initialize collector (reads from source database, writes to history database)
	coll, err := collector.New(ctx, sourceURL, store, pollInterval)
	if err != nil {
		log.Fatalf("Failed to initialize collector: %v", err)
	}
	defer coll.Close()

	if retention > 0 {
		coll.WithRetention(retention)
		log.Printf("Data retention: %v", retention)
	}

	go coll.Start(ctx)

	// Build handler with middleware chain
	handler := web.ChainMiddleware(
		webServer.Handler(),
		auth.Middleware(authCfg),
		rateLimiter.Middleware,
		web.SecurityHeaders(tlsEnabled),
	)

	// Start HTTP(S) server with security timeouts
	server := &http.Server{
		Addr:              ":" + httpPort,
		Handler:           handler,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
	}

	if tlsEnabled {
		if tlsCertFile == "" || tlsKeyFile == "" {
			log.Fatal("TLS_CERT_FILE and TLS_KEY_FILE are required when TLS_ENABLED=true")
		}
		server.TLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	go func() {
		if tlsEnabled {
			log.Printf("Starting HTTPS server on https://localhost:%s", httpPort)
			if err := server.ListenAndServeTLS(tlsCertFile, tlsKeyFile); err != http.ErrServerClosed {
				log.Fatalf("HTTPS server error: %v", err)
			}
		} else {
			log.Printf("Starting HTTP server on http://localhost:%s", httpPort)
			if err := server.ListenAndServe(); err != http.ErrServerClosed {
				log.Fatalf("HTTP server error: %v", err)
			}
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
  init           Initialize the history database and user
  export [path]  Export changes to a zipped CSV file (includes cluster_id)
  (none)         Run the cluster history server

Environment Variables:
  DATABASE_URL          CockroachDB connection string (required)
                        For 'init': admin connection to create database/user
                        For server/export: connection to monitored cluster

  HISTORY_DATABASE_URL  Connection to history database (required for server/export)

  POLL_INTERVAL         Collection interval (default: 15m)
  RETENTION             Data retention period, e.g., 720h for 30 days (default: unlimited)
  HTTP_PORT             Web server port (default: 8080)

  For 'init' command:
  HISTORY_DB_NAME       Database name to create (default: cluster_history)
  HISTORY_USERNAME      Username to create (default: history_user)
  HISTORY_PASSWORD      Password for the new user (optional in insecure mode)

Security:
  AUTH_ENABLED          Enable authentication (default: false)
  AUTH_USERNAME         Username for Basic Auth (default: admin)
  AUTH_PASSWORD         Password for Basic Auth (required if AUTH_ENABLED=true)
  AUTH_API_KEYS         Comma-separated API keys for X-API-Key header auth
  AUTH_PUBLIC_PATHS     Comma-separated public paths (default: /health)

  TLS_ENABLED           Enable HTTPS (default: false)
  TLS_CERT_FILE         Path to TLS certificate file
  TLS_KEY_FILE          Path to TLS private key file

  RATE_LIMIT_ENABLED    Enable rate limiting (default: false)
  RATE_LIMIT_RPS        Requests per second per IP (default: 10)
  RATE_LIMIT_BURST      Burst capacity (default: 20)

  REDACT_SENSITIVE      Redact sensitive setting values (default: false)
  REDACT_PATTERNS       Additional patterns to redact (comma-separated)
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

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		b, err := strconv.ParseBool(value)
		if err != nil {
			log.Printf("Invalid bool for %s: %v, using default", key, err)
			return defaultValue
		}
		return b
	}
	return defaultValue
}

func getEnvFloat(key string, defaultValue float64) float64 {
	if value := os.Getenv(key); value != "" {
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			log.Printf("Invalid float for %s: %v, using default", key, err)
			return defaultValue
		}
		return f
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		i, err := strconv.Atoi(value)
		if err != nil {
			log.Printf("Invalid int for %s: %v, using default", key, err)
			return defaultValue
		}
		return i
	}
	return defaultValue
}
