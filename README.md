# CockroachDB Cluster Settings History

A Go service that periodically collects CockroachDB cluster settings and tracks changes over time via a web interface.

![CockroachDB Cluster Settings History](crdb-cluster-history-preview.png)

## Table of Contents

- [Quick Start](#quick-start)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Docker](#docker)
- [Configuration](#configuration)
- [Security](#security)
- [Architecture](#architecture)
- [Web Endpoints](#web-endpoints)
- [Contributing](#contributing)
- [License](#license)

## Quick Start

### 1. Initialize the history database

This creates a dedicated database and user for storing settings history:

```bash
# Connect with admin privileges
export DATABASE_URL="postgresql://root@localhost:26257/defaultdb?sslmode=disable"

# For secure clusters, set a password
export HISTORY_PASSWORD="your_secure_password"

# Run initialization
./crdb-cluster-history init
```

The init command will:
- Create the `cluster_history` database
- Create the `history_user` user
- Grant necessary privileges
- Detect insecure mode automatically (skips password in insecure mode)

### 2. Run the service

```bash
# Connection to the cluster being monitored
export DATABASE_URL="postgresql://root@localhost:26257/defaultdb?sslmode=disable"

# Connection to the history database
export HISTORY_DATABASE_URL="postgresql://history_user@localhost:26257/cluster_history?sslmode=disable"

# Start the service
./crdb-cluster-history
```

Open http://localhost:8080 to view the changes dashboard.

### 3. Export data (optional)

Export all changes to a zipped CSV file:

```bash
./crdb-cluster-history export

# Or specify output path
./crdb-cluster-history export my-export.zip
```

The export includes the cluster ID from `crdb_internal.cluster_id()`.

## Features

- Periodically collects `SHOW CLUSTER SETTINGS` from a CockroachDB cluster
- Stores snapshots in a separate CockroachDB database for history
- Detects and records changes (modified, added, removed settings)
- Tracks database version at the time of each change
- **Annotations**: Add notes to changes explaining why settings were modified (e.g., "Increased buffer size due to OOM - JIRA-1234")
- Web UI displays a table of changes with timestamps, version, and old/new values
- Real-time search filter to quickly find settings
- Download CSV button to export changes directly from the web UI
- Hover over setting names to see their descriptions
- Displays cluster ID and database version in the header
- Configurable polling interval (1 minute to monthly)
- Configurable data retention with automatic cleanup
- CLI export command for scripted exports
- Dark/light mode based on system preference
- Health check endpoint for monitoring
- Supports both secure and insecure CockroachDB clusters

## Prerequisites

- Go 1.21+
- CockroachDB cluster (the one being monitored)
- Access to create a database and user for storing history

## Docker

### Quick Start with Docker Compose

The easiest way to run the service locally:

```bash
docker-compose up -d
```

This starts:
- CockroachDB single-node cluster
- Initializes the history database
- Runs the crdb-cluster-history service

Open http://localhost:8080 to view the dashboard.

### Build Docker Image

```bash
# Build image
docker build -t crdb-cluster-history .

# Build with version
docker build --build-arg VERSION=1.0.0 -t crdb-cluster-history:1.0.0 .

# Run container (connect to external CockroachDB)
docker run -d \
  -e DATABASE_URL="postgresql://root@host.docker.internal:26257/defaultdb?sslmode=disable" \
  -e HISTORY_DATABASE_URL="postgresql://history_user@host.docker.internal:26257/cluster_history?sslmode=disable" \
  -p 8080:8080 \
  crdb-cluster-history

# For Podman, use host.containers.internal instead of host.docker.internal
```

### Podman

The Docker commands also work with Podman:

```bash
# Build
podman build -t crdb-cluster-history .

# Compose (requires podman-compose or Podman 4.1+)
podman-compose up -d
# or
podman compose up -d
```

## Configuration

### Environment Variables

| Variable | Command | Description | Default |
|----------|---------|-------------|---------|
| `DATABASE_URL` | all | CockroachDB connection string. For `init`: admin connection. For server/export: monitored cluster | required |
| `HISTORY_DATABASE_URL` | server, export | Connection to history database | required |
| `POLL_INTERVAL` | server | How often to collect settings (Go duration) | `15m` |
| `RETENTION` | server | Data retention period (e.g., `720h` for 30 days) | unlimited |
| `HTTP_PORT` | server | Web server port | `8080` |
| `HISTORY_DB_NAME` | init | Database name to create | `cluster_history` |
| `HISTORY_USERNAME` | init | Username to create | `history_user` |
| `HISTORY_PASSWORD` | init | Password for user (optional in insecure mode) | - |

### Security Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `AUTH_ENABLED` | Enable authentication | `false` |
| `AUTH_USERNAME` | Username for Basic Auth | `admin` |
| `AUTH_PASSWORD` | Password for Basic Auth (required if AUTH_ENABLED=true) | - |
| `AUTH_API_KEYS` | Comma-separated API keys for X-API-Key header auth | - |
| `AUTH_PUBLIC_PATHS` | Comma-separated paths that don't require auth | `/health` |
| `TLS_ENABLED` | Enable HTTPS | `false` |
| `TLS_CERT_FILE` | Path to TLS certificate file | - |
| `TLS_KEY_FILE` | Path to TLS private key file | - |
| `RATE_LIMIT_ENABLED` | Enable rate limiting | `false` |
| `RATE_LIMIT_RPS` | Requests per second per IP | `10` |
| `RATE_LIMIT_BURST` | Burst capacity | `20` |
| `REDACT_SENSITIVE` | Redact sensitive setting values | `false` |
| `REDACT_PATTERNS` | Additional patterns to redact (comma-separated) | - |

### Poll Interval Examples

```bash
export POLL_INTERVAL="1m"    # Every minute
export POLL_INTERVAL="15m"   # Every 15 minutes (default)
export POLL_INTERVAL="1h"    # Every hour
export POLL_INTERVAL="24h"   # Daily
export POLL_INTERVAL="720h"  # Monthly (30 days)
```

## Security

### Secure Deployment

For production deployments, enable security features:

```bash
# Enable authentication
export AUTH_ENABLED=true
export AUTH_USERNAME=admin
export AUTH_PASSWORD=your_secure_password

# Optional: Enable TLS
export TLS_ENABLED=true
export TLS_CERT_FILE=/path/to/cert.pem
export TLS_KEY_FILE=/path/to/key.pem

# Optional: Enable rate limiting
export RATE_LIMIT_ENABLED=true

# Optional: Redact sensitive settings
export REDACT_SENSITIVE=true
```

### Secure Docker Deployment

Use `docker-compose.secure.yml` for production:

```bash
# Generate CockroachDB certificates
./scripts/generate-certs.sh

# Create .env file with passwords
cp .env.example .env
# Edit .env to set secure passwords

# Start with secure configuration
docker-compose -f docker-compose.secure.yml up -d
```

### Security Features

- **Authentication**: HTTP Basic Auth and API key support
- **HTTPS/TLS**: Optional TLS encryption for web traffic
- **Security Headers**: CSP, X-Frame-Options, HSTS, etc.
- **Rate Limiting**: Per-IP request rate limiting
- **Sensitive Data Redaction**: Automatically redacts passwords, secrets, keys, and tokens

## Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────┐
│  CockroachDB    │────▶│  Collector   │────▶│  CockroachDB    │
│  (monitored)    │     │  (periodic)  │     │  (history db)   │
└─────────────────┘     └──────────────┘     └─────────────────┘
                                                    │
                                                    ▼
                                            ┌─────────────┐
                                            │  Web Server │
                                            │  (diff UI)  │
                                            └─────────────┘
```

### Components

- **Collector**: Periodically queries `SHOW CLUSTER SETTINGS` and stores snapshots, tracks database version
- **Storage**: Manages history database, detects changes between snapshots, stores metadata (cluster ID, version)
- **Web Server**: Displays changes with search filter, download button, and version tracking

### Database Schema

```sql
-- Snapshots of settings at a point in time
CREATE TABLE snapshots (
    id SERIAL PRIMARY KEY,
    collected_at TIMESTAMPTZ NOT NULL
);

-- Individual settings within each snapshot
CREATE TABLE settings (
    id SERIAL PRIMARY KEY,
    snapshot_id INT REFERENCES snapshots(id) ON DELETE CASCADE,
    variable TEXT NOT NULL,
    value TEXT NOT NULL,
    setting_type TEXT,
    description TEXT
);

-- Detected changes between snapshots
CREATE TABLE changes (
    id SERIAL PRIMARY KEY,
    detected_at TIMESTAMPTZ NOT NULL,
    variable TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT,
    description TEXT,
    version TEXT  -- Database version at time of change (e.g., "v25.4.2")
);

-- User annotations/comments on changes
CREATE TABLE annotations (
    id SERIAL PRIMARY KEY,
    change_id INT NOT NULL UNIQUE REFERENCES changes(id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    created_by TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by TEXT,
    updated_at TIMESTAMPTZ
);

-- Key-value metadata (cluster_id, database_version, etc.)
CREATE TABLE metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
```

## Web Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Main dashboard with changes table, search, and download button |
| `/health` | GET | Health check endpoint (returns "ok" if database is accessible) |
| `/export` | GET | Download changes as zipped CSV file |
| `/api/annotations` | POST | Create a new annotation for a change |
| `/api/annotations/{id}` | GET | Retrieve an annotation |
| `/api/annotations/{id}` | PUT | Update an annotation |
| `/api/annotations/{id}` | DELETE | Delete an annotation |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for build instructions, development setup, and release process.

## License

MIT
