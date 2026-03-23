package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"crdb-cluster-history/auth"
	"crdb-cluster-history/cmd"
	"crdb-cluster-history/collector"
	"crdb-cluster-history/config"
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
	fs := flag.NewFlagSet("export", flag.ExitOnError)
	exportAll := fs.Bool("all", false, "Export all clusters")
	clusterID := fs.String("cluster", "", "Cluster ID to export")
	fs.StringVar(clusterID, "c", "", "Cluster ID to export (shorthand)")
	fs.BoolVar(exportAll, "a", false, "Export all clusters (shorthand)")
	fs.Parse(os.Args[2:])

	historyURL := os.Getenv("HISTORY_DATABASE_URL")
	if historyURL == "" {
		log.Fatal("HISTORY_DATABASE_URL environment variable is required")
	}

	outputPath := fs.Arg(0) // first non-flag argument

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cfg := cmd.ExportConfig{
		HistoryURL: historyURL,
		OutputPath: outputPath,
		ClusterID:  *clusterID,
		ExportAll:  *exportAll,
	}

	if err := cmd.RunExport(ctx, cfg); err != nil {
		log.Fatalf("Export failed: %v", err)
	}
}

func runInit() {
	adminURL := os.Getenv("DATABASE_URL")
	if adminURL == "" {
		log.Fatal("DATABASE_URL environment variable is required (admin connection)")
	}

	dbName := config.GetEnvDefault("HISTORY_DB_NAME", "cluster_history")
	username := config.GetEnvDefault("HISTORY_USERNAME", "history_user")
	password := os.Getenv("HISTORY_PASSWORD")

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
	cfg, err := config.LoadAuto()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}
	logClusterConfig(cfg)

	authCfg := setupAuth()
	rateLimiter := setupRateLimiter()
	redactor := setupRedactor()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rateLimiter.StartCleanup(ctx)

	store, err := storage.New(ctx, cfg.HistoryDatabaseURL)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}
	defer store.Close()

	webServer, err := web.New(store,
		web.WithRedactor(redactor),
		web.WithClusters(cfg.Clusters),
		web.WithDefaultClusterID(cfg.Clusters[0].ID),
	)
	if err != nil {
		log.Fatalf("Failed to initialize web server: %v", err)
	}

	startCollectors(ctx, cfg, store)

	tlsEnabled := getEnvBool("TLS_ENABLED", false)
	handler := setupMiddleware(webServer.Handler(), authCfg, rateLimiter, tlsEnabled)
	server := newHTTPServer(cfg.HTTPPort, handler, tlsEnabled)

	go startServer(server, tlsEnabled, cfg.HTTPPort)
	awaitShutdown(server, cancel)
}

func logClusterConfig(cfg *config.Config) {
	if len(cfg.Clusters) > 1 {
		slog.Info("Multi-cluster mode", "clusters", len(cfg.Clusters))
		for _, c := range cfg.Clusters {
			slog.Info("Cluster configured", "name", c.Name, "id", c.ID)
		}
	} else {
		slog.Info("Single-cluster mode", "cluster", cfg.Clusters[0].ID)
	}
}

func setupAuth() auth.Config {
	authEnabled := getEnvBool("AUTH_ENABLED", false)
	authCfg := auth.Config{
		Enabled:     authEnabled,
		Username:    config.GetEnvDefault("AUTH_USERNAME", "admin"),
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
		slog.Info("Authentication enabled", "user", authCfg.Username)
	}

	return authCfg
}

func setupRateLimiter() *web.RateLimiter {
	enabled := getEnvBool("RATE_LIMIT_ENABLED", false)
	rl := web.NewRateLimiter(web.RateLimiterConfig{
		Enabled:           enabled,
		RequestsPerSecond: getEnvFloat("RATE_LIMIT_RPS", 10),
		Burst:             getEnvInt("RATE_LIMIT_BURST", 20),
		TrustProxy:        getEnvBool("TRUST_PROXY", false),
	})
	if enabled {
		slog.Info("Rate limiting enabled", "rps", getEnvFloat("RATE_LIMIT_RPS", 10), "burst", getEnvInt("RATE_LIMIT_BURST", 20))
	}
	return rl
}

func setupRedactor() *storage.Redactor {
	redactCfg := storage.RedactorConfig{
		Enabled:            getEnvBool("REDACT_SENSITIVE", false),
		AdditionalPatterns: os.Getenv("REDACT_PATTERNS"),
	}
	redactor := storage.NewRedactor(redactCfg)
	if redactCfg.Enabled {
		slog.Info("Sensitive data redaction enabled")
	}
	return redactor
}

func startCollectors(ctx context.Context, cfg *config.Config, store *storage.Store) {
	if len(cfg.Clusters) > 1 {
		manager, err := collector.NewManager(ctx, cfg, store)
		if err != nil {
			log.Fatalf("Failed to initialize collector manager: %v", err)
		}
		go func() {
			<-ctx.Done()
			manager.Close()
		}()
		go manager.Start(ctx)
	} else {
		cluster := cfg.Clusters[0]
		coll, err := collector.New(ctx, cluster.ID, cluster.DatabaseURL, store, cfg.PollInterval.Duration())
		if err != nil {
			log.Fatalf("Failed to initialize collector: %v", err)
		}
		if cfg.Retention.Duration() > 0 {
			coll.WithRetention(cfg.Retention.Duration())
			slog.Info("Data retention configured", "retention", cfg.Retention.Duration())
		}
		go func() {
			<-ctx.Done()
			coll.Close()
		}()
		go coll.Start(ctx)
	}
}

func setupMiddleware(handler http.Handler, authCfg auth.Config, rateLimiter *web.RateLimiter, tlsEnabled bool) http.Handler {
	return web.ChainMiddleware(
		handler,
		auth.Middleware(authCfg),
		rateLimiter.Middleware,
		web.SecurityHeaders(tlsEnabled),
	)
}

func newHTTPServer(port string, handler http.Handler, tlsEnabled bool) *http.Server {
	server := &http.Server{
		Addr:              ":" + port,
		Handler:           handler,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
	}

	if tlsEnabled {
		tlsCertFile := os.Getenv("TLS_CERT_FILE")
		tlsKeyFile := os.Getenv("TLS_KEY_FILE")
		if tlsCertFile == "" || tlsKeyFile == "" {
			log.Fatal("TLS_CERT_FILE and TLS_KEY_FILE are required when TLS_ENABLED=true")
		}
		server.TLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	return server
}

func startServer(server *http.Server, tlsEnabled bool, port string) {
	if tlsEnabled {
		slog.Info("Starting HTTPS server", "port", port)
		if err := server.ListenAndServeTLS(os.Getenv("TLS_CERT_FILE"), os.Getenv("TLS_KEY_FILE")); err != http.ErrServerClosed {
			log.Fatalf("HTTPS server error: %v", err)
		}
	} else {
		slog.Info("Starting HTTP server", "port", port)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}
}

func awaitShutdown(server *http.Server, cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	slog.Info("Shutting down")
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

Export Flags:
  --all, -a              Export all clusters
  --cluster, -c ID       Cluster ID to export

Configuration:
  The server can be configured via a YAML file or environment variables.
  Configuration is loaded in this order:
  1. CLUSTERS_CONFIG env var (path to YAML config file)
  2. clusters.yaml in current directory
  3. Environment variables (single-cluster mode)

Environment Variables:
  DATABASE_URL          CockroachDB connection string (required)
  HISTORY_DATABASE_URL  Connection to history database (required for server/export)
  POLL_INTERVAL         Collection interval (default: 15m)
  RETENTION             Data retention period, e.g., 720h for 30 days (default: unlimited)
  HTTP_PORT             Web server port (default: 8080)

Security:
  AUTH_ENABLED          Enable authentication (default: false)
  AUTH_USERNAME          Username for Basic Auth (default: admin)
  AUTH_PASSWORD          Password for Basic Auth (required if AUTH_ENABLED=true)
  AUTH_API_KEYS          Comma-separated API keys
  TLS_ENABLED           Enable HTTPS (default: false)
  TLS_CERT_FILE         Path to TLS certificate file
  TLS_KEY_FILE          Path to TLS private key file
  RATE_LIMIT_ENABLED    Enable rate limiting (default: false)
  RATE_LIMIT_RPS        Requests per second per IP (default: 10)
  RATE_LIMIT_BURST      Burst capacity (default: 20)
  REDACT_SENSITIVE      Redact sensitive values (default: false)
  REDACT_PATTERNS       Additional patterns to redact (comma-separated)
`, os.Args[0])
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		b, err := strconv.ParseBool(value)
		if err != nil {
			slog.Warn("Invalid bool value, using default", "key", key, "error", err)
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
			slog.Warn("Invalid float value, using default", "key", key, "error", err)
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
			slog.Warn("Invalid int value, using default", "key", key, "error", err)
			return defaultValue
		}
		return i
	}
	return defaultValue
}
