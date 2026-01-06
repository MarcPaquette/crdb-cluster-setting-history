# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Test Commands

```bash
# Build
go build -o crdb-cluster-history .

# Build with version
go build -ldflags "-X main.Version=1.0.0" -o crdb-cluster-history .

# Run all tests (requires running CockroachDB)
# Use -p 1 to avoid serialization conflicts between parallel tests
DATABASE_URL="postgresql://root@localhost:26257/defaultdb?sslmode=disable" \
HISTORY_DATABASE_URL="postgresql://history_test_user@localhost:26257/cluster_history_test?sslmode=disable" \
go test -p 1 -v ./...

# Run tests with coverage
DATABASE_URL="postgresql://root@localhost:26257/defaultdb?sslmode=disable" \
HISTORY_DATABASE_URL="postgresql://history_test_user@localhost:26257/cluster_history_test?sslmode=disable" \
go test -p 1 -coverprofile=coverage.out ./...

# Run a single test
go test -v -run TestCollect ./collector/

# View coverage report
go tool cover -func=coverage.out
```

## Architecture

The service monitors a CockroachDB cluster by periodically querying `SHOW CLUSTER SETTINGS` and storing snapshots in a separate history database. Changes between snapshots are detected and displayed via a web UI.

**Data flow:** Monitored CockroachDB → Collector (periodic) → History CockroachDB → Web Server

**Key packages:**
- `collector/` - Periodic collection using `pgxpool`, queries `SHOW CLUSTER SETTINGS` (6 columns: variable, value, setting_type, description, default_value, origin), tracks database version, supports data retention/cleanup
- `storage/` - CockroachDB operations using `pgxpool`, change detection between snapshots, stores setting descriptions, metadata table for cluster_id and database_version, version tracking per change
- `web/` - HTTP server with embedded HTML template, endpoints: `/` (dashboard), `/health` (health check), `/export` (CSV download). Features: real-time search filter, download CSV button, dark/light mode, description tooltips, version column
- `cmd/init.go` - Init command to create history database and user, auto-detects insecure mode
- `cmd/export.go` - CLI export command to export changes to zipped CSV with cluster_id and version

**Two database connections:**
- `DATABASE_URL` - The cluster being monitored (read-only access needed)
- `HISTORY_DATABASE_URL` - Separate database for storing history (read/write)

**Environment variables:**
- `POLL_INTERVAL` - Collection interval (default: 15m)
- `RETENTION` - Data retention period, e.g., 720h for 30 days (default: unlimited)
- `HTTP_PORT` - Web server port (default: 8080)

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
- http://localhost:8080 - Web UI (changes table with search, download, version column, dark/light mode)
- http://localhost:8080/health - Health check endpoint
- http://localhost:8080/export - Download changes as zipped CSV
