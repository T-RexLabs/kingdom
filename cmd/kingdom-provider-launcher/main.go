package main

import (
	"context"
	"fmt"
	"kingdom/internal/providerlauncher"
	"os"
)

func main() {
	if len(os.Args) != 2 || os.Args[1] != providerlauncher.CommandName {
		fmt.Fprintln(os.Stderr, "unsupported provider launcher command")
		os.Exit(2)
	}
	job, e := providerlauncher.DecodeJob(os.Stdin)
	if e == nil {
		e = providerlauncher.Run(context.Background(), job, os.Stdout)
	}
	if e != nil {
		fmt.Fprintln(os.Stderr, "provider launcher failed")
		os.Exit(1)
	}
}
