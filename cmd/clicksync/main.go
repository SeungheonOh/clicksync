package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cardano-clicksync/internal/config"
	"cardano-clicksync/internal/model"
	"cardano-clicksync/internal/store"
	"cardano-clicksync/internal/syncer"
	"cardano-clicksync/internal/writerlock"
)

var buildID = "development"

const usage = "usage: clicksync migrate|sync|status"

func main() {
	logger := newLogger()
	if err := run(os.Args[1:], os.Stdout, logger); err != nil {
		logger.Error("clicksync failed", "error", err)
		os.Exit(1)
	}
}

func run(args []string, output *os.File, logger *slog.Logger) error {
	if len(args) != 1 {
		return errors.New(usage)
	}
	switch args[0] {
	case "migrate", "sync", "status":
	default:
		return fmt.Errorf("unknown command %q; %s", args[0], usage)
	}
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()
	return runCommand(ctx, args[0], output, logger)
}

func runCommand(
	ctx context.Context,
	command string,
	output *os.File,
	logger *slog.Logger,
) error {
	if output == nil {
		return errors.New("command output is required")
	}
	if logger == nil {
		return errors.New("command logger is required")
	}
	switch command {
	case "migrate":
		cfg, err := config.DatabaseFromEnv()
		if err != nil {
			return err
		}
		db, err := openAndPing(ctx, cfg)
		if err != nil {
			return err
		}
		defer db.Close()
		migrationCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		if err := db.Migrate(migrationCtx); err != nil {
			return fmt.Errorf("migrate ClickHouse: %w", err)
		}
		logger.Info("ClickHouse migration complete")
		return nil

	case "sync":
		cfg, err := config.SyncFromEnv()
		if err != nil {
			return err
		}
		lock, err := writerlock.Acquire(cfg.LockPath)
		if err != nil {
			return err
		}
		db, err := openAndPing(ctx, cfg.Database)
		if err != nil {
			return errors.Join(err, lock.Release())
		}
		closeOwned := func() error {
			return errors.Join(db.Close(), lock.Release())
		}
		identity, err := db.Initialize(ctx, store.DatasetConfig{
			NetworkMagic: cfg.NetworkMagic,
			NetworkName:  cfg.NetworkName,
			Start:        storePoint(cfg.Start),
			SourceBuild:  effectiveBuildID(),
		})
		if err != nil {
			return errors.Join(
				fmt.Errorf("initialize dataset: %w", err),
				closeOwned(),
			)
		}
		logger.Info(
			"dataset ready",
			"dataset_id", identity.DatasetID.String(),
			"network", identity.NetworkName,
			"network_magic", identity.NetworkMagic,
		)
		err = (syncer.Runner{
			Config:  cfg,
			Store:   db,
			Lock:    lock,
			Logger:  logger,
			Metrics: &syncer.Metrics{},
		}).Run(ctx)
		if errors.Is(err, syncer.ErrShutdownTimeout) {
			// The runner may still have an in-flight operation that ignored
			// cancellation. main exits immediately after this error; retaining
			// both resources avoids releasing the writer fence underneath it.
			return err
		}
		return errors.Join(err, closeOwned())

	case "status":
		database, err := config.DatabaseFromEnv()
		if err != nil {
			return err
		}
		depth, err := config.RollbackDepthFromEnv()
		if err != nil {
			return err
		}
		db, err := openAndPing(ctx, database)
		if err != nil {
			return err
		}
		defer db.Close()
		state, found, err := db.Inspect(ctx, depth)
		if err != nil {
			return fmt.Errorf("inspect dataset: %w", err)
		}
		if !found {
			return json.NewEncoder(output).Encode(statusOutput{
				Initialized: false,
			})
		}
		return json.NewEncoder(output).Encode(statusFromState(state))
	default:
		return fmt.Errorf("unknown command %q; %s", command, usage)
	}
}

func openAndPing(
	ctx context.Context,
	cfg config.Database,
) (*store.DB, error) {
	db, err := store.Open(cfg)
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.Ping(pingCtx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

type pointOutput struct {
	Origin      bool   `json:"origin"`
	Slot        uint64 `json:"slot,omitempty"`
	Hash        string `json:"hash,omitempty"`
	BlockNumber uint64 `json:"block_number,omitempty"`
	IsByronEBB  bool   `json:"is_byron_ebb,omitempty"`
}

type statusOutput struct {
	Initialized    bool          `json:"initialized"`
	DatasetID      string        `json:"dataset_id,omitempty"`
	SchemaHash     string        `json:"schema_hash,omitempty"`
	NetworkName    string        `json:"network_name,omitempty"`
	NetworkMagic   uint32        `json:"network_magic,omitempty"`
	Start          pointOutput   `json:"start,omitempty"`
	Snapshot       uint64        `json:"snapshot"`
	Tip            pointOutput   `json:"tip,omitempty"`
	CanonicalDepth int           `json:"canonical_depth"`
	Intersections  []pointOutput `json:"intersections,omitempty"`
	CreatedAt      time.Time     `json:"created_at,omitempty"`
	SourceBuild    string        `json:"source_build,omitempty"`
}

func statusFromState(state store.State) statusOutput {
	intersections := make([]pointOutput, len(state.Intersections))
	for index, point := range state.Intersections {
		intersections[index] = pointForOutput(point)
	}
	return statusOutput{
		Initialized:    true,
		DatasetID:      state.Dataset.DatasetID.String(),
		SchemaHash:     hex.EncodeToString(state.Dataset.SchemaHash[:]),
		NetworkName:    state.Dataset.NetworkName,
		NetworkMagic:   state.Dataset.NetworkMagic,
		Start:          pointForOutput(state.Dataset.Start),
		Snapshot:       state.Snapshot,
		Tip:            pointForOutput(state.Tip),
		CanonicalDepth: len(state.Canonical),
		Intersections:  intersections,
		CreatedAt:      state.Dataset.CreatedAt,
		SourceBuild:    state.Dataset.SourceBuild,
	}
}

func pointForOutput(point store.Point) pointOutput {
	ret := pointOutput{Origin: point.Origin}
	if point.Origin {
		return ret
	}
	ret.Slot = point.Slot
	ret.Hash = hex.EncodeToString(point.Hash[:])
	ret.BlockNumber = point.BlockNumber
	ret.IsByronEBB = point.IsByronEBB
	return ret
}

func storePoint(point config.Point) store.Point {
	return store.Point{
		Origin:      point.Origin,
		Slot:        point.Slot,
		Hash:        model.Hash32(point.Hash),
		BlockNumber: point.BlockNumber,
		IsByronEBB:  point.IsByronEBB,
	}
}

func effectiveBuildID() string {
	if buildID == "" {
		return "development"
	}
	return buildID
}

func newLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(
		os.Stderr,
		&slog.HandlerOptions{Level: slog.LevelInfo},
	))
}
