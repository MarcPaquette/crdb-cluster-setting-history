package collector

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"crdb-cluster-history/config"
)

// Manager manages multiple collectors for different clusters.
type Manager struct {
	collectors map[string]*Collector
	store      Store
	mu         sync.RWMutex
}

// NewManager creates a new collector manager for multi-cluster monitoring.
func NewManager(ctx context.Context, cfg *config.Config, store Store) (*Manager, error) {
	m := &Manager{
		collectors: make(map[string]*Collector),
		store:      store,
	}

	for _, cluster := range cfg.Clusters {
		collector, err := New(ctx, cluster.ID, cluster.DatabaseURL, store, cfg.PollInterval.Duration())
		if err != nil {
			// Clean up any collectors we already created
			m.Close()
			return nil, fmt.Errorf("failed to create collector for cluster %s: %w", cluster.ID, err)
		}

		// Apply retention if configured
		if cfg.Retention.Duration() > 0 {
			collector.WithRetention(cfg.Retention.Duration())
		}

		m.collectors[cluster.ID] = collector
		slog.Info("Created collector", "cluster", cluster.ID, "name", cluster.Name)
	}

	return m, nil
}

// Start starts all collectors. This method blocks until the context is cancelled.
func (m *Manager) Start(ctx context.Context) {
	m.mu.RLock()
	var wg sync.WaitGroup

	for clusterID, collector := range m.collectors {
		wg.Add(1)
		go func(id string, c *Collector) {
			defer wg.Done()
			slog.Info("Starting collector", "cluster", id)
			c.Start(ctx)
			slog.Info("Stopped collector", "cluster", id)
		}(clusterID, collector)
	}
	m.mu.RUnlock()

	wg.Wait()
}

// Close closes all collectors.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id, collector := range m.collectors {
		collector.Close()
		slog.Info("Closed collector", "cluster", id)
	}
}

// GetCollector returns a specific collector by cluster ID.
func (m *Manager) GetCollector(clusterID string) (*Collector, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	c, ok := m.collectors[clusterID]
	return c, ok
}

// ClusterIDs returns a list of all cluster IDs being managed.
func (m *Manager) ClusterIDs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids := make([]string, 0, len(m.collectors))
	for id := range m.collectors {
		ids = append(ids, id)
	}
	return ids
}

// Collect triggers an immediate collection for all collectors concurrently.
// Useful for testing or manual trigger.
func (m *Manager) Collect(ctx context.Context) error {
	m.mu.RLock()
	var wg sync.WaitGroup
	errs := make([]error, len(m.collectors))
	i := 0
	for _, c := range m.collectors {
		wg.Add(1)
		go func(idx int, coll *Collector) {
			defer wg.Done()
			errs[idx] = coll.Collect(ctx)
		}(i, c)
		i++
	}
	m.mu.RUnlock()
	wg.Wait()

	var collectErrs []error
	for _, err := range errs {
		if err != nil {
			collectErrs = append(collectErrs, err)
		}
	}
	if len(collectErrs) > 0 {
		return fmt.Errorf("collection errors: %v", collectErrs)
	}
	return nil
}
