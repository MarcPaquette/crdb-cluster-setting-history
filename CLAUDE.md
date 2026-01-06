# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Test Commands

```bash
# Build
go build -o cluster-history .

# Run all tests (requires running CockroachDB)
DATABASE_URL="postgresql://root@localhost:26257/defaultdb?sslmode=disable" \
HISTORY_DATABASE_URL="postgresql://history_test_user@localhost:26257/cluster_history_test?sslmode=disable" \
go test -v ./...

# Run tests with coverage
DATABASE_URL="postgresql://root@localhost:26257/defaultdb?sslmode=disable" \
HISTORY_DATABASE_URL="postgresql://history_test_user@localhost:26257/cluster_history_test?sslmode=disable" \
go test -coverprofile=coverage.out ./...

# Run a single test
go test -v -run TestCollect ./collector/

# View coverage report
go tool cover -func=coverage.out
```

## Architecture

The service monitors a CockroachDB cluster by periodically querying `SHOW CLUSTER SETTINGS` and storing snapshots in a separate history database. Changes between snapshots are detected and displayed via a web UI.

**Data flow:** Monitored CockroachDB → Collector (periodic) → History CockroachDB → Web Server

**Key packages:**
- `collector/` - Periodic collection using `pgx`, queries `SHOW CLUSTER SETTINGS` (6 columns: variable, value, setting_type, description, default_value, origin)
- `storage/` - CockroachDB operations using `pgxpool`, change detection between snapshots
- `web/` - HTTP server with embedded HTML template, displays changes table
- `cmd/` - Init command to create history database and user, auto-detects insecure mode

**Two database connections:**
- `DATABASE_URL` - The cluster being monitored (read-only access needed)
- `HISTORY_DATABASE_URL` - Separate database for storing history (read/write)

## Running Locally

```bash
# Initialize history database (one-time)
DATABASE_URL="postgresql://root@localhost:26257/defaultdb?sslmode=disable" ./cluster-history init

# Run the service
DATABASE_URL="postgresql://root@localhost:26257/defaultdb?sslmode=disable" \
HISTORY_DATABASE_URL="postgresql://history_user@localhost:26257/cluster_history?sslmode=disable" \
./cluster-history
```

Web UI available at http://localhost:8080
