package collector

import (
	"context"
	"log"
	"regexp"
	"time"

	"crdb-cluster-history/storage"

	"github.com/jackc/pgx/v5/pgxpool"
)

// versionRegex extracts the version number (e.g., "v25.4.2") from the full version string
var versionRegex = regexp.MustCompile(`v\d+\.\d+\.\d+`)

type Collector struct {
	pool      *pgxpool.Pool
	store     *storage.Store
	clusterID string        // Config cluster ID (e.g., "prod", "staging")
	interval  time.Duration
	retention time.Duration
}

func New(ctx context.Context, clusterID, connString string, store *storage.Store, interval time.Duration) (*Collector, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, err
	}
	// Verify the connection works
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Collector{
		pool:      pool,
		store:     store,
		clusterID: clusterID,
		interval:  interval,
		retention: 0, // No cleanup by default
	}, nil
}

// ClusterID returns the cluster ID for this collector.
func (c *Collector) ClusterID() string {
	return c.clusterID
}

func (c *Collector) Close() {
	c.pool.Close()
}

// WithRetention sets the data retention period. Data older than this will be cleaned up.
func (c *Collector) WithRetention(retention time.Duration) *Collector {
	c.retention = retention
	return c
}

func (c *Collector) Start(ctx context.Context) {
	// Run immediately on start
	c.collectAndCleanup(ctx)

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.collectAndCleanup(ctx)
		}
	}
}

func (c *Collector) collectAndCleanup(ctx context.Context) {
	if err := c.collect(ctx); err != nil {
		log.Printf("[%s] Collection error: %v", c.clusterID, err)
	}

	if c.retention > 0 {
		if err := c.cleanup(ctx); err != nil {
			log.Printf("[%s] Cleanup error: %v", c.clusterID, err)
		}
	}
}

// Collect triggers an immediate collection. Useful for testing or manual triggers.
func (c *Collector) Collect(ctx context.Context) error {
	return c.collect(ctx)
}

func (c *Collector) cleanup(ctx context.Context) error {
	snapshots, err := c.store.CleanupOldSnapshots(ctx, c.clusterID, c.retention)
	if err != nil {
		return err
	}
	changes, err := c.store.CleanupOldChanges(ctx, c.clusterID, c.retention)
	if err != nil {
		return err
	}
	if snapshots > 0 || changes > 0 {
		log.Printf("[%s] Cleanup: removed %d snapshots, %d changes", c.clusterID, snapshots, changes)
	}
	return nil
}

func (c *Collector) collect(ctx context.Context) error {
	log.Printf("[%s] Collecting cluster settings...", c.clusterID)

	// Fetch and store source cluster ID and version (only updates if changed)
	if err := c.updateSourceClusterID(ctx); err != nil {
		log.Printf("[%s] Warning: failed to update source cluster ID: %v", c.clusterID, err)
	}
	if err := c.updateDatabaseVersion(ctx); err != nil {
		log.Printf("[%s] Warning: failed to update database version: %v", c.clusterID, err)
	}

	// Get the short version for storing with changes
	shortVersion := c.getShortVersion(ctx)

	rows, err := c.pool.Query(ctx, "SHOW CLUSTER SETTINGS")
	if err != nil {
		return err
	}
	defer rows.Close()

	var settings []storage.Setting
	for rows.Next() {
		var s storage.Setting
		var defaultValue, origin string
		// SHOW CLUSTER SETTINGS returns: variable, value, setting_type, description, default_value, origin
		if err := rows.Scan(&s.Variable, &s.Value, &s.SettingType, &s.Description, &defaultValue, &origin); err != nil {
			return err
		}
		settings = append(settings, s)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	if err := c.store.SaveSnapshot(ctx, c.clusterID, settings, shortVersion); err != nil {
		return err
	}

	log.Printf("[%s] Collected %d settings", c.clusterID, len(settings))
	return nil
}

// getShortVersion returns the short version string (e.g., "v25.4.2") from the database
func (c *Collector) getShortVersion(ctx context.Context) string {
	var fullVersion string
	err := c.pool.QueryRow(ctx, "SELECT version()").Scan(&fullVersion)
	if err != nil {
		return ""
	}
	match := versionRegex.FindString(fullVersion)
	if match != "" {
		return match
	}
	return fullVersion
}

func (c *Collector) updateSourceClusterID(ctx context.Context) error {
	var sourceClusterID string
	err := c.pool.QueryRow(ctx, "SELECT crdb_internal.cluster_id()::TEXT").Scan(&sourceClusterID)
	if err != nil {
		return err
	}
	return c.store.SetSourceClusterID(ctx, c.clusterID, sourceClusterID)
}

func (c *Collector) updateDatabaseVersion(ctx context.Context) error {
	var version string
	err := c.pool.QueryRow(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return err
	}
	return c.store.SetDatabaseVersion(ctx, c.clusterID, version)
}
