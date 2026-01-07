# Contributing to CockroachDB Cluster Settings History

## Build

```bash
go build -o crdb-cluster-history .

# Or with version info
go build -ldflags "-X main.Version=1.0.0" -o crdb-cluster-history .
```

## Development

### Run Tests

Tests require a running CockroachDB instance.

```bash
# Set up test database
export DATABASE_URL="postgresql://root@localhost:26257/defaultdb?sslmode=disable"
export HISTORY_DATABASE_URL="postgresql://history_test_user@localhost:26257/cluster_history_test?sslmode=disable"

# Run all tests (use -p 1 to avoid serialization conflicts between parallel tests)
go test -p 1 -v ./...

# Run with coverage
go test -p 1 -coverprofile=coverage.out ./...
go tool cover -func=coverage.out

# Run a single test
go test -v -run TestCollect ./collector/
```

### Project Structure

```
crdb-cluster-history/
├── main.go              # Entry point, CLI handling
├── cmd/
│   ├── init.go          # Database/user initialization
│   └── export.go        # CLI export to CSV/zip
├── collector/
│   └── collector.go     # Periodic settings collection
├── storage/
│   └── store.go         # CockroachDB storage operations
├── web/
│   ├── server.go        # HTTP server (/, /health, /export endpoints)
│   └── templates/
│       └── index.html   # Web UI (search, download, dark/light mode)
└── *_test.go            # Tests
```

## Creating a Release

Releases are automated via GitHub Actions. When you push a tag starting with `v`, the workflow builds binaries for multiple platforms and creates a GitHub release.

### Steps to Create a Release

1. **Ensure all changes are committed and pushed to main**

2. **Create and push a version tag**:
   ```bash
   git tag v1.0.0
   git push origin v1.0.0
   ```

3. **GitHub Actions will automatically**:
   - Build binaries for:
     - `linux-amd64`
     - `linux-arm64`
     - `darwin-arm64`
   - Create a GitHub release with the tag name
   - Attach all built binaries to the release
   - Generate release notes from commit history

### Versioning

This project uses [Semantic Versioning](https://semver.org/):
- **MAJOR**: Breaking changes
- **MINOR**: New features, backward compatible
- **PATCH**: Bug fixes, backward compatible

### Building a Release Locally

To build a release binary locally with version info embedded:

```bash
# Build for current platform
go build -ldflags "-X main.Version=v1.0.0" -o crdb-cluster-history .

# Cross-compile for Linux
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
  go build -ldflags "-X main.Version=v1.0.0" -o crdb-cluster-history-linux-amd64 .
```

Verify the version:
```bash
./crdb-cluster-history --version
```
