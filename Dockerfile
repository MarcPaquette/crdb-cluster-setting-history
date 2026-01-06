# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Install git for go mod download (some dependencies may need it)
RUN apk add --no-cache git

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-X main.Version=${VERSION}" -o crdb-cluster-history .

# Runtime stage
FROM alpine:3.19

WORKDIR /app

# Add ca-certificates for HTTPS connections and tzdata for timezones
RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN adduser -D -g '' appuser
USER appuser

# Copy binary from builder
COPY --from=builder /app/crdb-cluster-history .

# Expose default port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

ENTRYPOINT ["./crdb-cluster-history"]
