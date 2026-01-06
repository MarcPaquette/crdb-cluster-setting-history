package collector

import (
	"context"
	"log"
	"time"

	"cluster-history/storage"

	"github.com/jackc/pgx/v5"
)

type Collector struct {
	connString string
	store      *storage.Store
	interval   time.Duration
}

func New(connString string, store *storage.Store, interval time.Duration) *Collector {
	return &Collector{
		connString: connString,
		store:      store,
		interval:   interval,
	}
}

func (c *Collector) Start(ctx context.Context) {
	// Run immediately on start
	if err := c.collect(ctx); err != nil {
		log.Printf("Collection error: %v", err)
	}

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.collect(ctx); err != nil {
				log.Printf("Collection error: %v", err)
			}
		}
	}
}

func (c *Collector) collect(ctx context.Context) error {
	log.Printf("Collecting cluster settings...")

	conn, err := pgx.Connect(ctx, c.connString)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, "SHOW CLUSTER SETTINGS")
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

	if err := c.store.SaveSnapshot(ctx, settings); err != nil {
		return err
	}

	log.Printf("Collected %d settings", len(settings))
	return nil
}
