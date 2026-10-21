package main

import (
	"context"
	"fmt"
	"kingdom/internal/workspacetransfer"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if len(os.Args) != 2 || os.Args[1] != "serve-v1" {
		fmt.Fprintln(os.Stderr, "unsupported workspace helper command")
		os.Exit(2)
	}
	job, err := workspacetransfer.DecodeJob(os.Stdin)
	if err == nil {
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()
		err = workspacetransfer.Run(ctx, job, os.Stdout)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "workspace transfer failed")
		os.Exit(1)
	}
}
