package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

var uploadCmd = &cobra.Command{
	Use:   "upload <datei>",
	Short: "Datei in Chunks aufteilen und hochladen",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		a, err := newApp()
		if err != nil {
			return err
		}
		defer a.Close()

		// Die Datei wird gestreamt, statt in den Speicher geladen.
		input, err := os.Open(args[0])
		if err != nil {
			return fmt.Errorf("open file: %w", err)
		}
		defer input.Close()

		// Name: Dateiname aus dem Pfad, optional per --name überschreibbar.
		name := filepath.Base(args[0])
		if v, _ := cmd.Flags().GetString("name"); v != "" {
			name = v
		}

		file, err := a.upload.Upload(input, name)
		if err != nil {
			return fmt.Errorf("upload: %w", err)
		}

		fmt.Fprintf(os.Stdout, "Uploaded: %s\n", file.Name)
		fmt.Fprintf(os.Stdout, "ID: %s\n", file.ID)
		fmt.Fprintf(os.Stdout, "Size: %d\n", file.Size)
		fmt.Fprintf(os.Stdout, "Chunks: %d\n", len(file.Chunks))
		return nil
	},
}

func init() {
	uploadCmd.Flags().String("name", "", "Name, unter dem die Datei gespeichert wird")
}
