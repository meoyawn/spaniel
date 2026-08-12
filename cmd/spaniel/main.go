package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/meoyawn/spaniel"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	databasePath := flag.String("db", "", "SQLite database path")
	formatValue := flag.String("format", "gcx", "JSON format: gcx or jaeger")
	flag.Parse()
	if flag.NArg() != 1 {
		return fmt.Errorf("spaniel requires exactly one trace ID, got %d arguments", flag.NArg())
	}
	format, err := spaniel.NewOutputFormatFromValue(*formatValue)
	if err != nil {
		return err
	}
	encoded, err := spaniel.ReadTraceJSON(context.Background(), *databasePath, format, flag.Arg(0))
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	if _, err := os.Stdout.Write(encoded); err != nil {
		return fmt.Errorf("write %s trace JSON: %w", format.String(), err)
	}
	return nil
}
