package main

import (
	"context"
	"fmt"
	"kingdom/internal/filetoolhelper"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if len(os.Args) != 2 || os.Args[1] != "run-v1" {
		fmt.Fprintln(os.Stderr, "unsupported file helper command")
		os.Exit(2)
	}
	job, e := filetoolhelper.DecodeJob(os.Stdin)
	if e == nil {
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()
		e = filetoolhelper.Run(ctx, job, os.Stdout)
	}
	if e != nil {
		fmt.Fprintln(os.Stderr, "file operation could not be verified")
		os.Exit(1)
	}
}
