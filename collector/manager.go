package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"crdb-cluster-history/config"
)

type Manager struct {
	collectors map[string]*Collector
	mu         sync.RWMutex
}

func NewManager(ctx context.Context, cfg *config.Config, store Store) (*Manager, error) {
	m := &Manager{
		collectors: make(map[string]*Collector),
	}

	retention := cfg.Retention.Duration()
	for _, cluster := range cfg.Clusters {
		collector, err := New(ctx, cluster.ID, cluster.DatabaseURL, store, cfg.PollInterval.Duration())
		if err != nil {
			m.Close()
			return nil, fmt.Errorf("failed to create collector for cluster %s: %w", cluster.ID, err)
		}

		if retention > 0 {
			collector.WithRetention(retention)
		}

		m.collectors[cluster.ID] = collector
		slog.Info("Created collector", "cluster", cluster.ID, "name", cluster.Name)
	}

	return m, nil
}

func (m *Manager) Start(ctx context.Context) {
	m.mu.RLock()
	var wg sync.WaitGroup

	for clusterID, collector := range m.collectors {
		wg.Add(1)
		go func() {
			defer wg.Done()
			slog.Info("Starting collector", "cluster", clusterID)
			collector.Start(ctx)
			slog.Info("Stopped collector", "cluster", clusterID)
		}()
	}
	m.mu.RUnlock()

	wg.Wait()
}

func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id, collector := range m.collectors {
		collector.Close()
		slog.Info("Closed collector", "cluster", id)
	}
}

func (m *Manager) GetCollector(clusterID string) (*Collector, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	c, ok := m.collectors[clusterID]
	return c, ok
}

func (m *Manager) ClusterIDs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids := make([]string, 0, len(m.collectors))
	for id := range m.collectors {
		ids = append(ids, id)
	}
	return ids
}

func (m *Manager) Collect(ctx context.Context) error {
	m.mu.RLock()
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	for _, c := range m.collectors {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.Collect(ctx); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}()
	}
	m.mu.RUnlock()
	wg.Wait()

	return errors.Join(errs...)
}
