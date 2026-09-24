package main

import (
	"fmt"
	"os"
)

func main() {
	if err := newRootCmd().Execute(); err != nil {
		// Die Root-Command hat SilenceErrors: true gesetzt, daher druckt
		// Cobra den Fehler nicht selbst - das übernehmen wir hier.
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}