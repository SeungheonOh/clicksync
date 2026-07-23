package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/clicksync-project/clickout/internal/app"
	"github.com/clicksync-project/clickout/internal/cli"
	chstore "github.com/clicksync-project/clickout/internal/clickhouse"
)

func main() {
	invocation, err := cli.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		fmt.Fprintln(os.Stderr, cli.Usage())
		os.Exit(2)
	}
	if invocation.Command == "help" {
		fmt.Fprintln(os.Stdout, cli.Usage())
		return
	}

	config, err := chstore.ConfigFromEnv()
	if err != nil {
		fmt.Fprintln(os.Stderr, "clickout:", err)
		os.Exit(1)
	}
	store, err := chstore.Open(config)
	if err != nil {
		fmt.Fprintln(os.Stderr, "clickout:", err)
		os.Exit(1)
	}
	defer store.Close()
	if err := store.Ping(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "clickout:", err)
		os.Exit(1)
	}
	result, err := app.New(store).Execute(context.Background(), invocation)
	if err != nil {
		fmt.Fprintln(os.Stderr, "clickout:", err)
		os.Exit(1)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, "clickout:", err)
		os.Exit(1)
	}
}
