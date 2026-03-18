# TODO — Code Review Remediation

Findings from a Staff/Principal Engineer-level code review, organized by priority.
Each item is scoped to be tackled in a single session.

---

## Phase 1: Critical Security

- [x] **Add `http.MaxBytesReader` on JSON-decoding endpoints**
  - `web/server.go` — `json.NewDecoder(r.Body).Decode(&req)` reads the entire body with no size limit. An attacker can POST a multi-GB body to `/api/annotations` and exhaust memory.
  - Fix: wrap with `r.Body = http.MaxBytesReader(w, r.Body, 1<<20)` (1 MB limit) before decoding.

- [x] **Add rate limiter cleanup to prevent memory leak**
  - `web/middleware.go` — the `visitors` map grows unboundedly; every unique IP creates an entry that is never cleaned up. An attacker can exhaust memory by sending requests from many IPs or spoofing `X-Forwarded-For`.
  - Fix: add a background goroutine that periodically evicts entries not seen in 5 minutes, or use a bounded LRU cache.

- [x] **Use pgx error codes instead of string matching**
  - `web/server.go` — `strings.Contains(errStr, "foreign key")` is fragile and locale-dependent. Database error messages can change between versions.
  - Fix: use `pgconn.PgError.Code` to check for specific PostgreSQL error conditions (23503 for FK violation, 23505 for unique violation).

---

## Phase 2: Testability Foundation

- [x] **Define consumer-side interfaces for Store dependencies**
  - `storage/store.go`, `web/server.go`, `collector/collector.go` — `Store` is a concrete struct with 15+ public methods. `Collector` and web `Server` depend directly on `*storage.Store`. Zero interfaces exist, making unit testing without a real database impossible.
  - Fix: define narrow interfaces at each consumer site (e.g., `ChangeReader` in web, `SettingStore` in collector) containing only the methods that consumer needs.

- [x] **Add `t.Parallel()` to all pure unit tests**
  - `auth/`, `config/`, `web/middleware_test.go` — 40+ independent unit tests run serially despite having no shared state.
  - Fix: add `t.Parallel()` to each test function (and subtests) that doesn't require database access.

- [x] **Add unit tests for untested helper functions**
  - `main.go` — `getEnvBool`, `getEnvFloat`, `getEnvInt` (used for security config) are never tested.
  - `collector/collector.go` — `getShortVersion()` regex could silently fail.
  - `cmd/export.go` — the `--all` flag multi-cluster export path is never exercised.
  - Fix: add table-driven unit tests for each.

- [x] **Use unique test cluster IDs to enable parallel test packages**
  - `integration_test.go` and `collector/collector_test.go` both use `const testClusterID = "test-cluster"`, writing to the same test database. This is why `-p 1` is required.
  - Fix: use unique IDs per test (e.g., `t.Name() + "-" + uuid.NewString()[:8]`) or separate test databases per package.

- [x] **Add `-race` flag to test instructions**
  - `CLAUDE.md` test commands don't include `-race`. Race conditions in the rate limiter's `sync.RWMutex`, the collector manager's map access, and concurrent database operations may go undetected.
  - Fix: add `-race` to all `go test` commands in CLAUDE.md.

---

## Phase 3: Architecture Improvements

- [x] **Break up monolithic `runServer()` function**
  - `main.go` — this ~170-line function handles config loading, auth setup, rate limiting, redaction, context creation, storage init, web server init, collector init, middleware chain, HTTP server creation, TLS setup, goroutine launch, and signal handling.
  - Fix: extract into composable helpers like `newHTTPServer()`, `setupMiddleware()`, `setupCollectors()`.

- [x] **Extract duplicated comparison logic**
  - `web/server.go` — `handleAPICompare` and `handleAPICompareSnapshots` contain nearly identical map-diff logic (iterate two `map[string]Setting` maps finding only-in-A, only-in-B, and different entries, then sort).
  - Fix: extract `compareSettings(a, b map[string]Setting) CompareResult`.

- [ ] **Use proper migration tooling instead of startup DDL**
  - `storage/store.go` — `initSchema()` runs 10+ DDL statements including `ALTER TABLE` and `CREATE INDEX` on every application startup. In multi-replica deployments, multiple instances will race on these concurrently. No migration versioning, rollback, or locking.
  - Fix: use golang-migrate or goose with a version-tracked migration table and advisory locking.

- [x] **Adopt `log/slog` for structured logging**
  - All packages — everything uses `log.Printf` with no log levels, no structured fields, no correlation IDs. The `[%s]` cluster prefix in collector logs is ad-hoc.
  - Fix: use `log/slog` (stdlib since Go 1.21) for structured, leveled logging.

- [x] **Replace hand-rolled CLI parsing with `flag` or `cobra`**
  - `main.go` — export command manually parses `os.Args` with a for-loop. `--cluster` at end of args silently does nothing; no `--help` for subcommands.
  - Fix: use `flag` (stdlib) or `cobra` for proper subcommand handling.

- [x] **Consolidate duplicated helper functions**
  - `getEnv()` in `main.go` duplicates `getEnvDefault()` in `config/config.go`. `parseDurationEnv()` in `config/config.go` duplicates `getEnvDuration()` in `main.go`. Test helpers (`getTestDB`/`getTestURLs`/`getHistoryURL`) are repeated in 5 test files.
  - Fix: consolidate into a single `internal/envutil` package or similar.

- [x] **Fix Dockerfile Go version to match go.mod**
  - `Dockerfile` uses `golang:1.21-alpine` but `go.mod` specifies a different version. These are inconsistent.
  - Fix: align the Dockerfile base image with the go.mod Go version.

- [x] **Replace hardcoded limits with named constants or config**
  - `web/server.go` — `100` (index page changes), `100000` (export limit), `100` (default snapshot list), `1000` (max snapshot limit) are scattered magic numbers.
  - Fix: define named constants or make them configurable via environment variables.

---

## Phase 4: Hardening

- [ ] **Add `TRUST_PROXY` configuration flag**
  - `web/middleware.go` — `getClientIP()` unconditionally trusts `X-Forwarded-For` and `X-Real-IP` headers. Without a reverse proxy that strips/sets these, a client can spoof their IP to bypass rate limiting.
  - Fix: add a `TRUST_PROXY` env var. When disabled, use `r.RemoteAddr` only.

- [ ] **Validate cluster IDs against configured clusters**
  - `web/server.go` — `getClusterID()` accepts any string from query params. While SQL injection is prevented by parameterized queries, this violates defense-in-depth.
  - Fix: validate against `s.clusters` before use; return 404 for unknown clusters.

- [ ] **Fix CSP `unsafe-inline` or extract inline scripts**
  - `web/middleware.go` — `script-src 'self' 'unsafe-inline'` effectively negates CSP's XSS protection.
  - Fix: use nonce-based CSP or extract all JavaScript to external files.

- [ ] **Limit export query size or implement streaming**
  - `web/server.go` — `GetChanges(ctx, clusterID, 100000)` loads up to 100K changes into memory before writing the zip. For long-running clusters, this could use significant memory.
  - Fix: implement cursor-based pagination or stream results directly to the zip writer.

- [ ] **Fix `GetSnapshotByID` double query**
  - `storage/store.go` — first checks `EXISTS`, then queries settings. Two round-trips when one would suffice.
  - Fix: query settings directly and check if the result set is empty.

- [ ] **Use connection pool ping for health check**
  - `web/server.go` — health endpoint runs `GetChanges(clusterID, 1)` which queries the changes table. A `SELECT 1` or pool ping would be cheaper and more appropriate.
  - Fix: replace with `pool.Ping(ctx)` or `SELECT 1`.

- [ ] **Fix `WriteChangesCSV` redundant cluster ID parameter**
  - `storage/store.go` — takes a `clusterID` parameter but each `Change` already has a `ClusterID` field. The CSV column could differ from the change's actual cluster ID.
  - Fix: use `change.ClusterID` from each record instead of the separate parameter.

- [ ] **Fix Collector Manager mutex inconsistency**
  - `collector/manager.go` — `Start()` iterates `m.collectors` without holding any lock, but `GetCollector()`, `ClusterIDs()`, and `Close()` all acquire locks. Currently safe because `Start()` is called once before concurrent access, but inconsistent locking will confuse future maintainers.
  - Fix: hold `RLock` in `Start()` for consistency, or document the single-threaded initialization contract.
