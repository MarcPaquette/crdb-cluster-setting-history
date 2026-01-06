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
	interval  time.Duration
	retention time.Duration
}

func New(ctx context.Context, connString string, store *storage.Store, interval time.Duration) (*Collector, error) {
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
		interval:  interval,
		retention: 0, // No cleanup by default
	}, nil
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
		log.Printf("Collection error: %v", err)
	}

	if c.retention > 0 {
		if err := c.cleanup(ctx); err != nil {
			log.Printf("Cleanup error: %v", err)
		}
	}
}

func (c *Collector) cleanup(ctx context.Context) error {
	snapshots, err := c.store.CleanupOldSnapshots(ctx, c.retention)
	if err != nil {
		return err
	}
	changes, err := c.store.CleanupOldChanges(ctx, c.retention)
	if err != nil {
		return err
	}
	if snapshots > 0 || changes > 0 {
		log.Printf("Cleanup: removed %d snapshots, %d changes", snapshots, changes)
	}
	return nil
}

func (c *Collector) collect(ctx context.Context) error {
	log.Printf("Collecting cluster settings...")

	// Fetch and store cluster ID and version (only updates if changed)
	if err := c.updateClusterID(ctx); err != nil {
		log.Printf("Warning: failed to update cluster ID: %v", err)
	}
	if err := c.updateDatabaseVersion(ctx); err != nil {
		log.Printf("Warning: failed to update database version: %v", err)
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

	if err := c.store.SaveSnapshot(ctx, settings, shortVersion); err != nil {
		return err
	}

	log.Printf("Collected %d settings", len(settings))
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

func (c *Collector) updateClusterID(ctx context.Context) error {
	var clusterID string
	err := c.pool.QueryRow(ctx, "SELECT crdb_internal.cluster_id()::TEXT").Scan(&clusterID)
	if err != nil {
		return err
	}
	return c.store.SetClusterID(ctx, clusterID)
}

func (c *Collector) updateDatabaseVersion(ctx context.Context) error {
	var version string
	err := c.pool.QueryRow(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return err
	}
	return c.store.SetDatabaseVersion(ctx, version)
}
