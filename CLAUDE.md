# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Test Commands

```bash
# Build
go build -o crdb-cluster-history .

# Build with version
go build -ldflags "-X main.Version=1.0.0" -o crdb-cluster-history .

# Run all tests (requires running CockroachDB)
# Tests automatically create a dedicated test database (cluster_history_test)
# Use -p 1 to avoid serialization conflicts between parallel tests
DATABASE_URL="postgresql://root@localhost:26257/defaultdb?sslmode=disable" \
go test -p 1 -race -v ./...

# Run tests with coverage
DATABASE_URL="postgresql://root@localhost:26257/defaultdb?sslmode=disable" \
go test -p 1 -race -coverprofile=coverage.out ./...

# Run a single test
DATABASE_URL="postgresql://root@localhost:26257/defaultdb?sslmode=disable" \
go test -race -v -run TestCollect ./collector/

# View coverage report
go tool cover -func=coverage.out
```

## Development Workflow — Red/Green TDD

All changes must follow Red/Green Test-Driven Development:

1. **Red** — Write a failing test first that describes the desired behavior. Run it and confirm it fails.
2. **Green** — Write the minimal production code to make the test pass. Run the test and confirm it passes.
3. **Refactor** — Clean up the code while keeping tests green. No behavior changes without a failing test first.

Rules:
- Never write production code without a failing test that demands it.
- Each Red/Green cycle should be small — one behavior or edge case at a time.
- Run the relevant test(s) after each step to verify the expected red or green result.
- For bug fixes: first write a test that reproduces the bug (red), then fix it (green).

## Architecture

The service monitors a CockroachDB cluster by periodically querying `SHOW CLUSTER SETTINGS` and storing snapshots in a separate history database. Changes between snapshots are detected and displayed via a web UI.

**Data flow:** Monitored CockroachDB → Collector (periodic) → History CockroachDB → Web Server

**Key packages:**
- `collector/` - Periodic collection using `pgxpool`, queries `SHOW CLUSTER SETTINGS` (6 columns: variable, value, setting_type, description, default_value, origin), tracks database version, supports data retention/cleanup. Manager handles multiple collectors for multi-cluster mode.
- `storage/` - CockroachDB operations using `pgxpool`, change detection between snapshots, stores setting descriptions, metadata table for cluster_id and database_version, version tracking per change, annotations support, sensitive value redaction
- `web/` - HTTP server with embedded HTML templates, security middleware (auth, rate limiting, headers). Features: real-time search filter, download CSV, dark/light mode, description tooltips, cluster selector, time-based comparison
- `auth/` - Authentication middleware supporting Basic Auth and API keys, configurable public paths
- `config/` - YAML configuration loading for multi-cluster mode, environment variable fallback, validation
- `cmd/init.go` - Init command to create history database and user with least-privilege permissions, auto-detects insecure mode, optionally grants VIEWCLUSTERMETADATA to source monitoring user
- `cmd/export.go` - CLI export command to export changes to zipped CSV with cluster_id and version

**Two database connections:**
- `DATABASE_URL` - The cluster being monitored (read-only access needed)
- `HISTORY_DATABASE_URL` - Separate database for storing history (read/write)

**Security - Least Privilege Model:**
The `init` command creates a history user with minimal required privileges:
- **Database level:** `CONNECT`, `CREATE` (CREATE needed for initial schema migration)
- **Table level:** `SELECT`, `INSERT`, `UPDATE`, `DELETE` only (via default privileges)
- **System level (optional):** `VIEWCLUSTERMETADATA` on the source monitoring user (when `SOURCE_USERNAME` is set)
- **NOT granted:** `DROP`, `ALTER`, admin privileges, or full database ownership

This ensures the history user can only perform data operations on its tables and cannot drop the database, modify schema after creation, or perform administrative actions.

**Environment variables:**
- `CLUSTERS_CONFIG` - Path to YAML config file for multi-cluster mode
- `POLL_INTERVAL` - Collection interval (default: 15m)
- `RETENTION` - Data retention period, e.g., 720h for 30 days (default: unlimited)
- `HTTP_PORT` - Web server port (default: 8080)
- `AUTH_ENABLED`, `AUTH_USERNAME`, `AUTH_PASSWORD`, `AUTH_API_KEYS` - Authentication settings
- `TLS_ENABLED`, `TLS_CERT_FILE`, `TLS_KEY_FILE` - HTTPS/TLS settings
- `RATE_LIMIT_ENABLED`, `RATE_LIMIT_RPS`, `RATE_LIMIT_BURST` - Rate limiting
- `REDACT_SENSITIVE`, `REDACT_PATTERNS` - Sensitive value redaction
- `SOURCE_USERNAME` - Source cluster monitoring user to grant `VIEWCLUSTERMETADATA` (init only, optional)
- `HISTORY_ADMIN_URL` - Admin connection to history cluster (tests only, defaults to `DATABASE_URL`)

## CLI Commands

```bash
./crdb-cluster-history           # Run the server
./crdb-cluster-history init      # Initialize history database and user
./crdb-cluster-history export    # Export changes to zipped CSV
./crdb-cluster-history --version # Show version
./crdb-cluster-history --help    # Show usage
```

## Running Locally

### With Docker Compose (easiest)

```bash
docker-compose up -d        # Start everything
docker-compose logs -f      # View logs
docker-compose down         # Stop
docker-compose down -v      # Stop and remove data
```

### Without Docker

```bash
# Initialize history database (one-time)
DATABASE_URL="postgresql://root@localhost:26257/defaultdb?sslmode=disable" ./crdb-cluster-history init

# Run the service
DATABASE_URL="postgresql://root@localhost:26257/defaultdb?sslmode=disable" \
HISTORY_DATABASE_URL="postgresql://history_user@localhost:26257/cluster_history?sslmode=disable" \
./crdb-cluster-history

# Export data
DATABASE_URL="postgresql://root@localhost:26257/defaultdb?sslmode=disable" \
HISTORY_DATABASE_URL="postgresql://history_user@localhost:26257/cluster_history?sslmode=disable" \
./crdb-cluster-history export
```

**Endpoints:**
- `/` - Main dashboard (changes table with search, download, cluster selector)
- `/compare` - Side-by-side cluster comparison page
- `/fleet` - Multi-cluster configuration drift analysis matrix
- `/history` - Time-based snapshot comparison page
- `/health` - Health check endpoint
- `/export` - Download changes as zipped CSV
- `/api/clusters` - List configured clusters (JSON)
- `/api/cluster-settings` - Get current settings for a cluster (JSON)
- `/api/compare` - Compare settings between clusters (JSON)
- `/api/snapshots` - List snapshots for a cluster (JSON)
- `/api/compare-snapshots` - Compare two snapshots (JSON)
- `/api/annotations` - Create annotation (POST)
- `/api/annotations/{id}` - Get/update/delete annotation (GET/PUT/DELETE)
