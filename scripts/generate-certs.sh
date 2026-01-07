#!/bin/bash
# Generate certificates for secure CockroachDB deployment
# This script creates a CA and node/client certificates

set -e

CERTS_DIR="${1:-./certs}"

echo "Generating certificates in: $CERTS_DIR"
mkdir -p "$CERTS_DIR"

# Check if cockroach CLI is available
if ! command -v cockroach &> /dev/null; then
    echo "Error: cockroach CLI not found"
    echo "Install it from: https://www.cockroachlabs.com/docs/stable/install-cockroachdb.html"
    echo ""
    echo "Or use Docker:"
    echo "  docker run --rm -v \$(pwd)/certs:/certs cockroachdb/cockroach:v23.2.0 cert create-ca --certs-dir=/certs --ca-key=/certs/ca.key"
    exit 1
fi

# Create CA certificate
echo "Creating CA certificate..."
cockroach cert create-ca \
    --certs-dir="$CERTS_DIR" \
    --ca-key="$CERTS_DIR/ca.key"

# Create node certificate (for CockroachDB server)
echo "Creating node certificate..."
cockroach cert create-node \
    localhost \
    cockroachdb \
    127.0.0.1 \
    --certs-dir="$CERTS_DIR" \
    --ca-key="$CERTS_DIR/ca.key"

# Create root client certificate (for admin operations)
echo "Creating root client certificate..."
cockroach cert create-client \
    root \
    --certs-dir="$CERTS_DIR" \
    --ca-key="$CERTS_DIR/ca.key"

# Set permissions
chmod 600 "$CERTS_DIR"/*.key
chmod 644 "$CERTS_DIR"/*.crt

echo ""
echo "Certificates generated successfully!"
echo ""
echo "Files created:"
ls -la "$CERTS_DIR"
echo ""
echo "Next steps:"
echo "  1. Copy .env.example to .env and set secure passwords"
echo "  2. Run: docker-compose -f docker-compose.secure.yml up -d"
