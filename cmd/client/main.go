package main

import (
	"fmt"
	"os"
)

func main() {
	root, err := newRootCmd()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if err := root.Execute(); err != nil {
		// Root command has SilenceErrors: true, cobra does not print error
		// we do it here
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
