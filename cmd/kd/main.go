package main

import (
	"context"
	"os"

	"kingdom/internal/cli"
	"kingdom/internal/processguardian"
)

func main() {
	if exitCode, handled := processguardian.DispatchHiddenMode(os.Args); handled {
		os.Exit(exitCode)
	}
	os.Exit(cli.Execute(context.Background(), os.Stdout, os.Stderr, os.Args[1:]))
}
