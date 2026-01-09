package cmd

import (
	"archive/zip"
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"crdb-cluster-history/storage"

	"github.com/jackc/pgx/v5"
)

type ExportConfig struct {
	SourceURL  string // Connection to source database (for getting cluster ID)
	HistoryURL string // Connection to history database
	OutputPath string // Output file path (empty for default)
	ClusterID  string // Specific cluster ID to export (empty for all)
	ExportAll  bool   // Export all clusters (creates one CSV per cluster)
}

func RunExport(ctx context.Context, cfg ExportConfig) error {
	// Connect to history database
	log.Println("Connecting to history database...")
	store, err := storage.New(ctx, cfg.HistoryURL)
	if err != nil {
		return fmt.Errorf("failed to connect to history database: %w", err)
	}
	defer store.Close()

	// Determine output path
	outputPath := cfg.OutputPath
	if outputPath == "" {
		outputPath = fmt.Sprintf("crdb-cluster-history-export-%s.zip", time.Now().Format("20060102-150405"))
	}

	// Create zip file
	zipFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer zipFile.Close()

	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	// Determine which clusters to export
	var clusterIDs []string
	if cfg.ClusterID != "" {
		// Export specific cluster
		clusterIDs = []string{cfg.ClusterID}
	} else if cfg.ExportAll {
		// Export all clusters from database
		clusterIDs, err = store.ListClusters(ctx)
		if err != nil {
			return fmt.Errorf("failed to list clusters: %w", err)
		}
		if len(clusterIDs) == 0 {
			log.Println("No clusters found in database")
			return nil
		}
		log.Printf("Found %d clusters to export", len(clusterIDs))
	} else {
		// Default: try to get cluster ID from source database, fall back to "default"
		if cfg.SourceURL != "" {
			clusterID, err := getSourceClusterID(ctx, cfg.SourceURL)
			if err != nil {
				log.Printf("Could not get cluster ID from source: %v, using 'default'", err)
				clusterIDs = []string{"default"}
			} else {
				// Get the config cluster ID that maps to this source cluster ID
				// For now, just use "default" since we're exporting from history
				log.Printf("Source cluster ID: %s", clusterID)
				clusterIDs = []string{"default"}
			}
		} else {
			clusterIDs = []string{"default"}
		}
	}

	totalChanges := 0
	for _, clusterID := range clusterIDs {
		changes, err := store.GetChanges(ctx, clusterID, 100000)
		if err != nil {
			log.Printf("Warning: failed to get changes for cluster %s: %v", clusterID, err)
			continue
		}

		if len(changes) == 0 {
			log.Printf("No changes for cluster %s", clusterID)
			continue
		}

		// Get source cluster ID for this config cluster ID (if available)
		sourceClusterID, err := store.GetSourceClusterID(ctx, clusterID)
		if err != nil || sourceClusterID == "" {
			sourceClusterID = clusterID
		}

		// Create CSV file inside zip
		csvFileName := fmt.Sprintf("crdb-cluster-history-%s.csv", sourceClusterID)
		csvWriter, err := zipWriter.Create(csvFileName)
		if err != nil {
			return fmt.Errorf("failed to create CSV in zip for cluster %s: %w", clusterID, err)
		}

		// Write CSV
		if err := storage.WriteChangesCSV(csvWriter, sourceClusterID, changes); err != nil {
			return fmt.Errorf("failed to write CSV for cluster %s: %w", clusterID, err)
		}

		log.Printf("Exported %d changes for cluster %s", len(changes), clusterID)
		totalChanges += len(changes)
	}

	if totalChanges == 0 {
		log.Println("No changes to export")
		// Remove empty zip file
		zipWriter.Close()
		zipFile.Close()
		os.Remove(outputPath)
		return nil
	}

	log.Printf("Exported %d total changes to %s", totalChanges, outputPath)
	return nil
}

// getSourceClusterID connects to the source database and gets the cluster ID
func getSourceClusterID(ctx context.Context, sourceURL string) (string, error) {
	conn, err := pgx.Connect(ctx, sourceURL)
	if err != nil {
		return "", fmt.Errorf("failed to connect to source database: %w", err)
	}
	defer conn.Close(ctx)

	var clusterID string
	err = conn.QueryRow(ctx, "SELECT crdb_internal.cluster_id()::TEXT").Scan(&clusterID)
	if err != nil {
		return "", fmt.Errorf("failed to get cluster ID: %w", err)
	}
	return clusterID, nil
}
