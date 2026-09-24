package main

import (
	"fmt"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
)

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "Alle Dateien im Store anzeigen",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		a, err := newApp()
		if err != nil {
			return err
		}
		defer a.Close()

		files, err := a.list.List()
		if err != nil {
			return fmt.Errorf("list: %w", err)
		}

		if len(files) == 0 {
			fmt.Fprintln(os.Stdout, "Keine Dateien im Store.")
			return nil
		}

		// Alphabetisch nach Name sortieren für stabile Ausgabe.
		sort.Slice(files, func(i, j int) bool {
			return files[i].Name < files[j].Name
		})

		full, _ := cmd.Flags().GetBool("full")

		// Spaltenausrichtung über tabwriter.
		tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "ID\tNAME\tSIZE\tCHUNKS")
		for _, f := range files {
			fmt.Fprintf(tw, "%s\t%s\t%d\t%d\n",
				displayID(f.ID, full), f.Name, f.Size, len(f.Chunks))
		}
		tw.Flush()
		return nil
	},
}

func init() {
	listCmd.Flags().Bool("full", false, "vollständige File-IDs anzeigen")
}

// displayID kürzt File-IDs (erste 8 Zeichen) für die Lesbarkeit.
// download und remove verlangen weiterhin die vollständige ID.
func displayID(id domain.FileID, full bool) string {
	if full || len(id) <= 8 {
		return string(id)
	}
	return string(id[:8]) + "…"
}
