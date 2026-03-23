package cmd

import (
	"archive/zip"
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"crdb-cluster-history/storage"
)

type ExportConfig struct {
	HistoryURL string // Connection to history database
	OutputPath string // Output file path (empty for default)
	ClusterID  string // Specific cluster ID to export (empty for all)
	ExportAll  bool   // Export all clusters (creates one CSV per cluster)
}

func RunExport(ctx context.Context, cfg ExportConfig) error {
	// Connect to history database
	slog.Info("Connecting to history database")
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
			slog.Info("No clusters found in database")
			return nil
		}
		slog.Info("Found clusters to export", "count", len(clusterIDs))
	} else {
		clusterIDs = []string{"default"}
	}

	totalChanges := 0
	for _, clusterID := range clusterIDs {
		// Get source cluster ID for this config cluster ID (if available)
		sourceClusterID, err := store.GetSourceClusterID(ctx, clusterID)
		if err != nil || sourceClusterID == "" {
			sourceClusterID = clusterID
		}

		// Create CSV file inside zip
		csvFileName := fmt.Sprintf("crdb-cluster-history-%s.csv", sourceClusterID)
		csvFile, err := zipWriter.Create(csvFileName)
		if err != nil {
			return fmt.Errorf("failed to create CSV in zip for cluster %s: %w", clusterID, err)
		}

		// Stream changes directly to CSV
		csvWriter := storage.NewCSVChangeWriter(csvFile)
		if err := csvWriter.WriteHeader(); err != nil {
			return fmt.Errorf("failed to write CSV header for cluster %s: %w", clusterID, err)
		}

		count := 0
		err = store.StreamChanges(ctx, clusterID, func(c storage.Change) error {
			count++
			return csvWriter.WriteChange(c)
		})
		if err != nil {
			return fmt.Errorf("failed to stream changes for cluster %s: %w", clusterID, err)
		}
		csvWriter.Flush()
		if err := csvWriter.Error(); err != nil {
			return fmt.Errorf("CSV error for cluster %s: %w", clusterID, err)
		}

		if count == 0 {
			slog.Info("No changes for cluster", "cluster", clusterID)
		} else {
			slog.Info("Exported changes for cluster", "cluster", clusterID, "count", count)
		}
		totalChanges += count
	}

	if totalChanges == 0 {
		slog.Info("No changes to export")
		// Remove empty zip file (deferred closes handle the writers)
		if err := os.Remove(outputPath); err != nil {
			slog.Warn("Failed to remove empty export file", "path", outputPath, "error", err)
		}
		return nil
	}

	slog.Info("Export completed", "total_changes", totalChanges, "output", outputPath)
	return nil
}
