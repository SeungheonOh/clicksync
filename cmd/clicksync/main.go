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
	"clicksync/internal/ingest"
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
		return errors.New("usage: clicksync migrate|sync|status|peers|writer")
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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch args[0] {
	case "migrate":
		cfg, err := config.DatabaseFromEnv()
		if err != nil {
			return err
		}
		writer := config.WriterSettingsFromEnv()
		lock, err := writerlock.Acquire(writer.LockPath, writer.WriterCoordination)
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
	case "sync":
		cfg, err := config.FromEnv()
		if err != nil {
			return err
		}
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
		if err := db.Ping(ctx); err != nil {
			return err
		}
		logger := slog.New(slog.NewJSONHandler(
			os.Stderr,
			&slog.HandlerOptions{Level: slog.LevelInfo},
		))
		return ingest.RunSync(ctx, cfg, db, lock, buildID, logger)
	case "status":
		cfg, err := config.DatabaseFromEnv()
		if err != nil {
			return err
		}
		db, err := store.Open(cfg)
		if err != nil {
			return err
		}
		defer db.Close()
		if err := db.Ping(ctx); err != nil {
			return err
		}
		identity, found, err := db.LoadManifestIdentityIfExists(ctx)
		if err != nil {
			return err
		}
		if !found {
			return json.NewEncoder(os.Stdout).Encode(map[string]any{
				"initialized": false,
			})
		}
		snapshot, err := db.CommittedSnapshot(ctx)
		if err != nil {
			return err
		}
		tip, err := db.CommittedTip(ctx, snapshot)
		if err != nil {
			return err
		}
		tipJSON := map[string]any{"origin": tip.Origin}
		if !tip.Origin {
			tipJSON["slot"] = tip.Slot
			tipJSON["hash"] = fmt.Sprintf("%x", tip.Hash)
			tipJSON["block_number"] = tip.BlockNumber
			tipJSON["is_byron_ebb"] = tip.IsByronEBB
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{
			"initialized":         true,
			"dataset_id":          fmt.Sprintf("%x", identity.DatasetID),
			"network_name":        identity.NetworkName,
			"network_magic":       identity.NetworkMagic,
			"complete_history":    identity.CompleteHistory,
			"committed_event_seq": snapshot,
			"tip":                 tipJSON,
		})
	case "writer":
		cfg, err := config.DatabaseFromEnv()
		if err != nil {
			return err
		}
		writer := config.WriterSettingsFromEnv()
		db, err := store.Open(cfg)
		if err != nil {
			return err
		}
		defer db.Close()
		if err := db.Ping(ctx); err != nil {
			return err
		}
		audit, found, err := db.LatestWriterAudit(ctx)
		if err != nil {
			return err
		}
		var auditJSON any
		if found {
			auditJSON = map[string]any{
				"dataset_id":     fmt.Sprintf("%x", audit.DatasetID),
				"revision":       audit.Revision,
				"owner_id":       fmt.Sprintf("%x", audit.OwnerID),
				"state":          audit.State,
				"heartbeat_at":   audit.HeartbeatAt,
				"released_at":    audit.ReleasedAt,
				"release_reason": audit.ReleaseReason,
			}
		}
		return json.NewEncoder(os.Stdout).Encode(map[string]any{
			"coordination": writer.WriterCoordination,
			"supported":    writer.WriterCoordination == "single-host-flock",
			"lock_path":    writer.LockPath,
			"audit_only":   true,
			"found":        found,
			"audit":        auditJSON,
		})
	default:
		return fmt.Errorf("unknown ingestion command %q", args[0])
	}
}
