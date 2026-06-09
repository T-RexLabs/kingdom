package main

import (
	"context"
	"os"

	"kingdom/internal/cli"
)

func main() {
	os.Exit(cli.Execute(context.Background(), os.Stdout, os.Stderr, os.Args[1:]))
}
