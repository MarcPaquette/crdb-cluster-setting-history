#!/usr/bin/env bash
set -euo pipefail

# === Constants ===
CONTAINER_PREFIX="crdb-fleet-test"
CONFIG_FILE="clusters-fleet-test.yaml"
BINARY="./crdb-cluster-history"
APP_PORT="8080"
SQL_PORT_BASE=26300
HTTP_PORT_BASE=8090
DEFAULT_NUM_CLUSTERS=5

VERSIONS=(
    "latest-v26.1"
    "latest-v25.4"
    "latest-v24.3"
    "latest-v24.1"
    "latest-v23.2"
)

RUNTIME=""

# === Logging ===
log_info()  { printf '\033[0;32m[INFO]\033[0m  %s\n' "$*"; }
log_warn()  { printf '\033[0;33m[WARN]\033[0m  %s\n' "$*"; }
log_error() { printf '\033[0;31m[ERROR]\033[0m %s\n' "$*" >&2; }
log_step()  { printf '\033[0;34m[%s]\033[0m %s\n' "$1" "$2"; }

# === Container Runtime Detection ===
detect_runtime() {
    if command -v docker &>/dev/null && docker info &>/dev/null 2>&1; then
        RUNTIME="docker"
    elif command -v podman &>/dev/null && podman info &>/dev/null 2>&1; then
        RUNTIME="podman"
    else
        log_error "Neither docker nor podman is available or running"
        log_error "Start Docker Desktop or install podman, then retry"
        exit 1
    fi
    log_info "Using container runtime: $RUNTIME"
}

# === Port Check ===
check_port() {
    local port=$1
    if command -v lsof &>/dev/null && lsof -i ":${port}" -sTCP:LISTEN &>/dev/null 2>&1; then
        log_error "Port $port is already in use"
        exit 1
    fi
}

# === Container Lifecycle ===
stop_containers() {
    local containers
    containers=$($RUNTIME ps -a --filter "name=${CONTAINER_PREFIX}" --format '{{.Names}}' 2>/dev/null || true)
    if [ -n "$containers" ]; then
        log_info "Removing fleet test containers..."
        echo "$containers" | xargs $RUNTIME rm -f 2>/dev/null || true
    fi
}

pull_images() {
    local num_clusters=$1
    local pulled=()

    log_step "2/7" "Pulling CockroachDB images..."
    for i in $(seq 0 $((num_clusters - 1))); do
        local version="${VERSIONS[$((i % ${#VERSIONS[@]}))]}"

        # Skip if already pulled in this run
        local already=false
        for p in "${pulled[@]+"${pulled[@]}"}"; do
            if [ "$p" = "$version" ]; then already=true; break; fi
        done
        if $already; then continue; fi

        if ! $RUNTIME image inspect "cockroachdb/cockroach:${version}" &>/dev/null; then
            log_info "Pulling cockroachdb/cockroach:${version}..."
            if ! $RUNTIME pull "cockroachdb/cockroach:${version}"; then
                log_error "Failed to pull cockroachdb/cockroach:${version}"
                log_error "Check available tags at https://hub.docker.com/r/cockroachdb/cockroach/tags"
                exit 1
            fi
        else
            log_info "cockroachdb/cockroach:${version} already available"
        fi
        pulled+=("$version")
    done
}

start_containers() {
    local num_clusters=$1

    # Clean up any previous run
    stop_containers
    sleep 3  # allow ports to be released

    log_step "3/7" "Starting $num_clusters CockroachDB containers..."
    for i in $(seq 0 $((num_clusters - 1))); do
        local version="${VERSIONS[$((i % ${#VERSIONS[@]}))]}"
        local name="${CONTAINER_PREFIX}-${i}"
        local sql_port=$((SQL_PORT_BASE + i))
        local http_port=$((HTTP_PORT_BASE + i))

        check_port "$sql_port"
        check_port "$http_port"

        log_info "Starting $name ($version) — SQL :$sql_port, UI :$http_port"

        $RUNTIME run -d \
            --name "$name" \
            --memory=512m \
            -p "${sql_port}:26257" \
            -p "${http_port}:8080" \
            "cockroachdb/cockroach:${version}" \
            start-single-node --insecure \
            --cache=64MiB --max-sql-memory=64MiB \
            >/dev/null
    done
}

wait_for_clusters() {
    local num_clusters=$1
    local max_wait=90

    log_step "4/7" "Waiting for clusters to be healthy..."
    for i in $(seq 0 $((num_clusters - 1))); do
        local sql_port=$((SQL_PORT_BASE + i))
        local http_port=$((HTTP_PORT_BASE + i))
        local name="${CONTAINER_PREFIX}-${i}"
        local waited=0

        # Wait for HTTP health endpoint
        while ! curl -sf "http://localhost:${http_port}/health" &>/dev/null; do
            if [ $waited -ge $max_wait ]; then
                log_error "$name HTTP health check failed after ${max_wait}s"
                log_error "Check logs: $RUNTIME logs $name"
                exit 1
            fi
            sleep 2
            waited=$((waited + 2))
        done

        # Wait for SQL port to be reachable from the host (not inside container)
        while ! (echo >/dev/tcp/localhost/$sql_port) 2>/dev/null; do
            if [ $waited -ge $max_wait ]; then
                log_error "$name SQL port $sql_port not reachable from host after ${max_wait}s"
                log_error "Check logs: $RUNTIME logs $name"
                exit 1
            fi
            sleep 2
            waited=$((waited + 2))
        done

        log_info "$name ready (${waited}s)"
    done
}

# === Config Generation ===
generate_config() {
    local num_clusters=$1

    log_step "5/7" "Generating ${CONFIG_FILE}..."

    cat > "$CONFIG_FILE" <<YAML_HEADER
# Auto-generated by fleet-test.sh — do not edit
# $(date)

history_database_url: "postgresql://history_user@localhost:${SQL_PORT_BASE}/cluster_history?sslmode=disable"
poll_interval: 30s
http_port: "${APP_PORT}"

clusters:
YAML_HEADER

    for i in $(seq 0 $((num_clusters - 1))); do
        local version="${VERSIONS[$((i % ${#VERSIONS[@]}))]}"
        local sql_port=$((SQL_PORT_BASE + i))

        cat >> "$CONFIG_FILE" <<YAML_CLUSTER
  - name: "CockroachDB ${version} (cluster-${i})"
    id: "cluster-${i}"
    database_url: "postgresql://root@localhost:${sql_port}/defaultdb?sslmode=disable"
YAML_CLUSTER
    done

    log_info "Config written to $CONFIG_FILE"
}

# === Init ===
run_init() {
    log_step "6/7" "Initializing history database on cluster-0..."
    local max_attempts=5
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if DATABASE_URL="postgresql://root@localhost:${SQL_PORT_BASE}/defaultdb?sslmode=disable" \
            "$BINARY" init 2>&1; then
            return 0
        fi

        if [ $attempt -lt $max_attempts ]; then
            log_warn "Init attempt $attempt/$max_attempts failed, retrying in 5s..."
            sleep 5
        fi
        attempt=$((attempt + 1))
    done

    log_error "Init failed after $max_attempts attempts"
    exit 1
}

# === Migration Verification ===
verify_migration() {
    log_step "6b/7" "Verifying migration created all tables..."
    local sql_port=$SQL_PORT_BASE
    local expected_tables="schema_migrations snapshots settings changes metadata annotations"

    for table in $expected_tables; do
        local result
        result=$($RUNTIME exec "${CONTAINER_PREFIX}-0" cockroach sql --insecure \
            --database=cluster_history -e \
            "SELECT count(*) FROM information_schema.tables WHERE table_name = '$table'" \
            --format=csv 2>/dev/null | tail -1)
        if [ "$result" != "1" ]; then
            log_error "Table $table not found after migration"
            exit 1
        fi
    done
    log_info "All tables verified: $expected_tables"
}

# === Server ===
start_server() {
    log_step "7/7" "Starting server..."
    echo ""
    CLUSTERS_CONFIG="$CONFIG_FILE" exec "$BINARY"
}

# === Cleanup ===
cleanup() {
    echo ""
    log_info "Shutting down..."
    stop_containers
    rm -f "$CONFIG_FILE"
    log_info "Fleet test environment cleaned up"
}

# === Main ===
main() {
    local subcommand="${1:-start}"
    local num_clusters="${2:-$DEFAULT_NUM_CLUSTERS}"

    # Resolve to project root (script lives in scripts/)
    cd "$(dirname "$0")/.."

    case "$subcommand" in
        start)
            detect_runtime

            log_step "1/7" "Building Go binary..."
            go build -o "$BINARY" .
            log_info "Build successful"

            pull_images "$num_clusters"
            start_containers "$num_clusters"
            trap cleanup EXIT INT TERM
            wait_for_clusters "$num_clusters"
            generate_config "$num_clusters"
            run_init
            verify_migration

            echo ""
            echo "  Fleet comparison:  http://localhost:${APP_PORT}/fleet"
            echo "  Cluster compare:   http://localhost:${APP_PORT}/compare"
            echo "  Dashboard:         http://localhost:${APP_PORT}/"
            echo ""
            echo "  Clusters:"
            for i in $(seq 0 $((num_clusters - 1))); do
                local version="${VERSIONS[$((i % ${#VERSIONS[@]}))]}"
                local sql_port=$((SQL_PORT_BASE + i))
                local http_port=$((HTTP_PORT_BASE + i))
                echo "    cluster-${i}: ${version} (SQL: ${sql_port}, UI: http://localhost:${http_port})"
            done
            echo ""
            echo "  Press Ctrl+C to stop"
            echo ""

            start_server
            ;;
        stop)
            detect_runtime
            stop_containers
            rm -f "$CONFIG_FILE"
            log_info "Fleet test environment stopped"
            ;;
        status)
            detect_runtime
            $RUNTIME ps --filter "name=${CONTAINER_PREFIX}" --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"
            ;;
        *)
            echo "Usage: $(basename "$0") [start|stop|status] [num_clusters]"
            echo ""
            echo "Commands:"
            echo "  start [N]   Start N CockroachDB clusters and the server (default: $DEFAULT_NUM_CLUSTERS)"
            echo "  stop        Stop all fleet test containers and clean up"
            echo "  status      Show status of fleet test containers"
            exit 1
            ;;
    esac
}

main "$@"
