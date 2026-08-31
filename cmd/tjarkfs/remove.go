package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
)

var removeCmd = &cobra.Command{
	Use:   "remove <file-id>",
	Short: "Datei und nicht mehr referenzierte Chunks löschen",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		a, err := newApp()
		if err != nil {
			return err
		}
		defer a.Close()

		id := domain.FileID(args[0])

		// Delete() ist by Design idempotent (unbekannte ID = No-Op).
		// Für die CLI prüfen wir vorab und geben eine klare
		// „nicht gefunden“-Meldung aus, statt still zu verschwinden.
		if _, err := a.repository.Get(id); err != nil {
			if errors.Is(err, domain.ErrFileNotFound) {
				return fmt.Errorf("datei nicht gefunden: %s", id)
			}
			return fmt.Errorf("get file: %w", err)
		}

		if err := a.deleteSvc.Delete(id); err != nil {
			return fmt.Errorf("delete: %w", err)
		}

		fmt.Fprintf(os.Stdout, "Deleted: %s\n", id)
		return nil
	},
}
