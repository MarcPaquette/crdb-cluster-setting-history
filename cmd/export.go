package cmd

import (
	"archive/zip"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"cluster-history/storage"

	"github.com/jackc/pgx/v5"
)

type ExportConfig struct {
	SourceURL  string
	HistoryURL string
	OutputPath string
}

func RunExport(ctx context.Context, cfg ExportConfig) error {
	// Get cluster ID from source database
	log.Println("Connecting to source database to get cluster ID...")
	conn, err := pgx.Connect(ctx, cfg.SourceURL)
	if err != nil {
		return fmt.Errorf("failed to connect to source database: %w", err)
	}

	var clusterID string
	err = conn.QueryRow(ctx, "SELECT crdb_internal.cluster_id()::TEXT").Scan(&clusterID)
	conn.Close(ctx)
	if err != nil {
		return fmt.Errorf("failed to get cluster ID: %w", err)
	}
	log.Printf("Cluster ID: %s", clusterID)

	// Connect to history database
	log.Println("Connecting to history database...")
	store, err := storage.New(ctx, cfg.HistoryURL)
	if err != nil {
		return fmt.Errorf("failed to connect to history database: %w", err)
	}
	defer store.Close()

	// Get all changes (use a large limit)
	changes, err := store.GetChanges(ctx, 100000)
	if err != nil {
		return fmt.Errorf("failed to get changes: %w", err)
	}
	log.Printf("Found %d changes to export", len(changes))

	if len(changes) == 0 {
		log.Println("No changes to export")
		return nil
	}

	// Determine output path
	outputPath := cfg.OutputPath
	if outputPath == "" {
		outputPath = fmt.Sprintf("cluster-history-export-%s.zip", time.Now().Format("20060102-150405"))
	}

	// Create zip file
	zipFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer zipFile.Close()

	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	// Create CSV file inside zip
	csvFileName := fmt.Sprintf("cluster-history-%s.csv", clusterID)
	csvWriter, err := zipWriter.Create(csvFileName)
	if err != nil {
		return fmt.Errorf("failed to create CSV in zip: %w", err)
	}

	// Write CSV
	if err := writeChangesCSV(csvWriter, clusterID, changes); err != nil {
		return fmt.Errorf("failed to write CSV: %w", err)
	}

	log.Printf("Exported %d changes to %s", len(changes), outputPath)
	return nil
}

func writeChangesCSV(w io.Writer, clusterID string, changes []storage.Change) error {
	csvWriter := csv.NewWriter(w)
	defer csvWriter.Flush()

	// Write header
	header := []string{"cluster_id", "detected_at", "variable", "old_value", "new_value", "description"}
	if err := csvWriter.Write(header); err != nil {
		return err
	}

	// Write rows
	for _, c := range changes {
		row := []string{
			clusterID,
			c.DetectedAt.Format(time.RFC3339),
			c.Variable,
			c.OldValue,
			c.NewValue,
			c.Description,
		}
		if err := csvWriter.Write(row); err != nil {
			return err
		}
	}

	return csvWriter.Error()
}
