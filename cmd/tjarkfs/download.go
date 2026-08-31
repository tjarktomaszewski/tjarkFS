package main

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"github.com/tjarktomaszewski/tjarkFS/internal/domain"
)

var downloadCmd = &cobra.Command{
	Use:   "download <file-id> [output]",
	Short: "Datei aus ihren Chunks rekonstruieren",
	Args:  cobra.RangeArgs(1, 2),
	RunE: func(cmd *cobra.Command, args []string) error {
		a, err := newApp()
		if err != nil {
			return err
		}
		defer a.Close()

		id := domain.FileID(args[0])

		// Zielselektor: stdout, sofern kein Pfad angegeben, sonst neue Datei.
		var target io.Writer = os.Stdout
		if len(args) == 2 {
			out, err := os.Create(args[1])
			if err != nil {
				return fmt.Errorf("create output file: %w", err)
			}
			defer out.Close()
			target = out
		}

		// Statuszeile nach stderr - stdout trägt nur den Dateicontent.
		file, err := a.repository.Get(id)
		if err != nil {
			return fmt.Errorf("get file: %w", err)
		}

		if err := a.download.Download(id, target); err != nil {
			return fmt.Errorf("download: %w", err)
		}

		fmt.Fprintf(os.Stderr, "Downloaded: %s\n", file.Name)
		return nil
	},
}
