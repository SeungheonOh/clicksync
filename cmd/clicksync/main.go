package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"clicksync/internal/config"
	"clicksync/internal/store"
	"clicksync/internal/writerlock"
)

var buildID = "development"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(os.Args[1:]); err != nil {
		logger.Error("clicksync failed", "error", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: clicksync migrate|sync|status|peers|storage|lease")
	}
	if args[0] == "peers" {
		cfg, err := config.PeersFromEnv()
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{
			"network_magic": cfg.NetworkMagic,
			"peers":         cfg.Peers,
			"corroboration": cfg.Corroboration,
		})
	}
	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch args[0] {
	case "migrate":
		lock, err := writerlock.Acquire(cfg.LockPath, cfg.WriterCoordination)
		if err != nil {
			return err
		}
		defer lock.Release()
		db, err := store.Open(cfg)
		if err != nil {
			return err
		}
		defer db.Close()
		ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		if err := db.Ping(ctx); err != nil {
			return err
		}
		return db.Migrate(ctx)
	case "lease":
		lock, err := writerlock.Acquire(cfg.LockPath, cfg.WriterCoordination)
		if err != nil {
			return err
		}
		defer lock.Release()
		return json.NewEncoder(os.Stdout).Encode(map[string]any{
			"coordination": "single-host-flock",
			"path":         lock.Path(),
			"build":        buildID,
			"remote":       "unsupported",
		})
	case "sync":
		lock, err := writerlock.Acquire(cfg.LockPath, cfg.WriterCoordination)
		if err != nil {
			return err
		}
		defer lock.Release()
		return fmt.Errorf("continuous sync implementation is not yet wired")
	case "status", "storage":
		return fmt.Errorf("%s requires the publication store implementation", args[0])
	default:
		return fmt.Errorf("unknown ingestion command %q", args[0])
	}
}
