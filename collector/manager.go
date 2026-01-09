package collector

import (
	"context"
	"fmt"
	"log"
	"sync"

	"crdb-cluster-history/config"
	"crdb-cluster-history/storage"
)

// Manager manages multiple collectors for different clusters.
type Manager struct {
	collectors map[string]*Collector
	store      *storage.Store
	mu         sync.RWMutex
}

// NewManager creates a new collector manager for multi-cluster monitoring.
func NewManager(ctx context.Context, cfg *config.Config, store *storage.Store) (*Manager, error) {
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
		log.Printf("Created collector for cluster %s (%s)", cluster.Name, cluster.ID)
	}

	return m, nil
}

// Start starts all collectors. This method blocks until the context is cancelled.
func (m *Manager) Start(ctx context.Context) {
	var wg sync.WaitGroup

	for clusterID, collector := range m.collectors {
		wg.Add(1)
		go func(id string, c *Collector) {
			defer wg.Done()
			log.Printf("Starting collector for cluster %s", id)
			c.Start(ctx)
			log.Printf("Stopped collector for cluster %s", id)
		}(clusterID, collector)
	}

	wg.Wait()
}

// Close closes all collectors.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id, collector := range m.collectors {
		collector.Close()
		log.Printf("Closed collector for cluster %s", id)
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

// Collect triggers an immediate collection for all collectors.
// Useful for testing or manual trigger.
func (m *Manager) Collect(ctx context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var errs []error
	for _, collector := range m.collectors {
		if err := collector.Collect(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("collection errors: %v", errs)
	}
	return nil
}
