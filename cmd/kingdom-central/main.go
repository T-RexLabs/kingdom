package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"kingdom/internal/central/daemon"
	"kingdom/internal/processguardian"
)

func main() { os.Exit(run()) }
func run() int {
	if exitCode, handled := processguardian.DispatchHiddenMode(os.Args); handled {
		return exitCode
	}
	path := flag.String("config", "", "central service configuration file")
	flag.Parse()
	if *path == "" || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "Usage: kingdom-central --config <file>")
		return 2
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if e := daemon.Run(ctx, *path, os.Stdout); e != nil {
		fmt.Fprintf(os.Stderr, "Kingdom Central stopped: %v\n", e)
		return 1
	}
	return 0
}
