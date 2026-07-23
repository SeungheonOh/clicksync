package clickhouse

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/clicksync-project/clickout/internal/metrics"
	"github.com/clicksync-project/clickout/internal/model"
)

var (
	ErrNotFound       = errors.New("not found at captured snapshot")
	ErrConflictingRow = errors.New("multiple active rows violate chain uniqueness")
	ErrInvalidDataset = errors.New("invalid dataset manifest")
	ErrResourceLimit  = errors.New("ClickHouse query resource limit exceeded")
)

type ResourceLimitError struct {
	Phase string
	Cause error
}

func (err *ResourceLimitError) Error() string {
	if err.Cause == nil {
		return fmt.Sprintf("%s: %v", err.Phase, ErrResourceLimit)
	}
	return fmt.Sprintf("%s: %v: %v", err.Phase, ErrResourceLimit, err.Cause)
}

func (err *ResourceLimitError) Unwrap() error {
	return err.Cause
}

func (err *ResourceLimitError) Is(target error) bool {
	return target == ErrResourceLimit
}

type Config struct {
	Addresses    []string
	Database     string
	Username     string
	Password     string
	Secure       bool
	QueryTimeout time.Duration
}

func ConfigFromEnv() (Config, error) {
	config := Config{
		Addresses:    []string{"127.0.0.1:9000"},
		Database:     "clicksync",
		Username:     "default",
		Password:     os.Getenv("CLICKOUT_CLICKHOUSE_PASSWORD"),
		QueryTimeout: 30 * time.Second,
	}
	if value := strings.TrimSpace(os.Getenv("CLICKOUT_CLICKHOUSE_ADDR")); value != "" {
		config.Addresses = strings.Split(value, ",")
	}
	if value := strings.TrimSpace(os.Getenv("CLICKOUT_CLICKHOUSE_DATABASE")); value != "" {
		config.Database = value
	}
	if value := strings.TrimSpace(os.Getenv("CLICKOUT_CLICKHOUSE_USERNAME")); value != "" {
		config.Username = value
	}
	if value := strings.TrimSpace(os.Getenv("CLICKOUT_CLICKHOUSE_SECURE")); value != "" {
		secure, err := strconv.ParseBool(value)
		if err != nil {
			return Config{}, fmt.Errorf("CLICKOUT_CLICKHOUSE_SECURE: %w", err)
		}
		config.Secure = secure
	}
	if value := strings.TrimSpace(os.Getenv("CLICKOUT_QUERY_TIMEOUT")); value != "" {
		timeout, err := time.ParseDuration(value)
		if err != nil || timeout <= 0 || timeout > 30*time.Second {
			return Config{}, errors.New("CLICKOUT_QUERY_TIMEOUT must be between 1ns and 30s")
		}
		config.QueryTimeout = timeout
	}
	if len(config.Addresses) == 0 {
		return Config{}, errors.New("at least one ClickHouse address is required")
	}
	for _, address := range config.Addresses {
		if strings.TrimSpace(address) == "" {
			return Config{}, errors.New("ClickHouse addresses cannot be empty")
		}
	}
	if !validIdentifier(config.Database) {
		return Config{}, errors.New("CLICKOUT_CLICKHOUSE_DATABASE must be an identifier")
	}
	return config, nil
}

func validIdentifier(value string) bool {
	if value == "" {
		return false
	}
	for index, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			char == '_' || (index > 0 && char >= '0' && char <= '9') {
			continue
		}
		return false
	}
	return true
}

type Store struct {
	conn         driver.Conn
	queryTimeout time.Duration
}

func Open(config Config) (*Store, error) {
	var tlsConfig *tls.Config
	if config.Secure {
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	conn, err := ch.Open(&ch.Options{
		Addr: config.Addresses,
		Auth: ch.Auth{
			Database: config.Database,
			Username: config.Username,
			Password: config.Password,
		},
		TLS:             tlsConfig,
		DialTimeout:     5 * time.Second,
		MaxOpenConns:    4,
		MaxIdleConns:    2,
		ConnMaxLifetime: 10 * time.Minute,
		Compression: &ch.Compression{
			Method: ch.CompressionLZ4,
		},
		Settings: ch.Settings{
			"min_table_rows_to_use_projection_index": 0,
		},
	})
	if err != nil {
		return nil, err
	}
	return &Store{conn: conn, queryTimeout: config.QueryTimeout}, nil
}

func (store *Store) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return store.conn.Ping(ctx)
}

func (store *Store) Close() error {
	return store.conn.Close()
}

func (store *Store) instrument(parent context.Context, name string) (context.Context, func()) {
	return store.instrumentPhase(parent, name, defaultPhaseLimits())
}

type phaseLimits struct {
	MaxRowsToRead  uint64
	MaxBytesToRead uint64
	MaxResultRows  uint64
	MaxResultBytes uint64
	MaxMemoryUsage uint64
}

const (
	mebibyte              = uint64(1024 * 1024)
	defaultMaxRowsToRead  = uint64(2_000_000)
	defaultMaxBytesToRead = 512 * mebibyte
	defaultMaxResultRows  = uint64(100_001)
	defaultMaxResultBytes = 256 * mebibyte
	defaultMaxMemoryUsage = 512 * mebibyte
)

func defaultPhaseLimits() phaseLimits {
	return phaseLimits{
		MaxRowsToRead:  defaultMaxRowsToRead,
		MaxBytesToRead: defaultMaxBytesToRead,
		MaxResultRows:  defaultMaxResultRows,
		MaxResultBytes: defaultMaxResultBytes,
		MaxMemoryUsage: defaultMaxMemoryUsage,
	}
}

func candidatePhaseLimits(resultRows uint64) phaseLimits {
	value := defaultPhaseLimits()
	value.MaxResultRows = atLeastOne(resultRows)
	value.MaxResultBytes = 64 * mebibyte
	return value
}

func hydrationPhaseLimits(resultRows uint64) phaseLimits {
	value := defaultPhaseLimits()
	value.MaxResultRows = atLeastOne(resultRows)
	return value
}

func resultPhaseLimits(resultRows uint64) phaseLimits {
	value := defaultPhaseLimits()
	value.MaxRowsToRead = 1
	value.MaxBytesToRead = 1 * mebibyte
	value.MaxResultRows = atLeastOne(resultRows)
	value.MaxResultBytes = 1 * mebibyte
	value.MaxMemoryUsage = 64 * mebibyte
	return value
}

func atLeastOne(value uint64) uint64 {
	if value == 0 {
		return 1
	}
	return value
}

func settingsForPhase(value phaseLimits, timeout time.Duration) ch.Settings {
	return ch.Settings{
		"join_use_nulls":                      1,
		"max_execution_time":                  uint64(timeout / time.Second),
		"max_rows_to_read":                    value.MaxRowsToRead,
		"max_bytes_to_read":                   value.MaxBytesToRead,
		"read_overflow_mode":                  "throw",
		"max_result_rows":                     value.MaxResultRows,
		"max_result_bytes":                    value.MaxResultBytes,
		"result_overflow_mode":                "throw",
		"max_memory_usage":                    value.MaxMemoryUsage,
		"memory_overcommit_ratio_denominator": 0,
	}
}

func (store *Store) instrumentPhase(
	parent context.Context,
	name string,
	limits phaseLimits,
) (context.Context, func()) {
	started := time.Now()
	var rows atomic.Uint64
	var bytes atomic.Uint64
	var serverElapsed atomic.Int64
	ctx, cancel := context.WithTimeout(parent, store.queryTimeout)
	queryCtx := ch.Context(ctx,
		ch.WithSettings(settingsForPhase(limits, store.queryTimeout)),
		ch.WithProgress(func(progress *ch.Progress) {
			rows.Add(progress.Rows)
			bytes.Add(progress.Bytes)
			elapsed := progress.Elapsed.Nanoseconds()
			for {
				current := serverElapsed.Load()
				if elapsed <= current || serverElapsed.CompareAndSwap(current, elapsed) {
					break
				}
			}
		}),
	)
	return queryCtx, func() {
		cancel()
		metrics.Add(parent, metrics.Query{
			Name:          name,
			ReadRows:      rows.Load(),
			ReadBytes:     bytes.Load(),
			ServerElapsed: time.Duration(serverElapsed.Load()),
			WallElapsed:   time.Since(started),
		})
	}
}

func mapQueryError(phase string, err error) error {
	if err == nil {
		return nil
	}
	message := strings.ToLower(err.Error())
	for _, marker := range []string{
		"max_rows_to_read",
		"max_bytes_to_read",
		"max_result_rows",
		"max_result_bytes",
		"memory limit",
		"too many rows",
		"too many bytes",
		"limit for result exceeded",
	} {
		if strings.Contains(message, marker) {
			return &ResourceLimitError{Phase: phase, Cause: err}
		}
	}
	return err
}

func (store *Store) Snapshot(ctx context.Context, point model.AtPoint) (model.Snapshot, error) {
	var event uint64
	var count uint64
	var publicationWatermark uint64
	queryCtx, finish := store.instrument(ctx, "snapshot_event")
	if point.Tip {
		err := store.conn.QueryRow(queryCtx, snapshotTipSQL).Scan(
			&event,
			&publicationWatermark,
			&count,
		)
		finish()
		if err != nil {
			return model.Snapshot{}, err
		}
		if event != 0 && count != 1 {
			return model.Snapshot{}, ErrInvalidDataset
		}
	} else if point.BlockHash != nil {
		var tip uint64
		var commitCount uint64
		err := store.conn.QueryRow(
			queryCtx,
			snapshotAtBlockSQL,
			hashArgument(*point.BlockHash),
		).Scan(&count, &event, &publicationWatermark, &tip, &commitCount)
		finish()
		if err != nil {
			return model.Snapshot{}, err
		}
		if count == 0 {
			return model.Snapshot{}, ErrNotFound
		}
		if event > tip {
			return model.Snapshot{}, ErrNotFound
		}
		if event != 0 && commitCount != 1 {
			return model.Snapshot{}, ErrInvalidDataset
		}
	} else if point.Event != nil {
		finish()
		event = *point.Event
		var tip uint64
		queryCtx, finish = store.instrument(ctx, "snapshot_watermark")
		err := store.conn.QueryRow(
			queryCtx,
			snapshotPinnedSQL,
			event,
			event,
			event,
		).Scan(&tip, &count, &publicationWatermark)
		finish()
		if err != nil {
			return model.Snapshot{}, err
		}
		if event > tip || (event != 0 && count == 0) {
			return model.Snapshot{}, ErrNotFound
		}
		if event != 0 && count != 1 {
			return model.Snapshot{}, ErrInvalidDataset
		}
	} else {
		finish()
		return model.Snapshot{}, errors.New("snapshot point must be tip or a block hash")
	}

	var manifestRows uint64
	var complete bool
	var trust string
	queryCtx, finish = store.instrument(ctx, "dataset_manifest")
	err := store.conn.QueryRow(queryCtx, manifestSQL).Scan(&manifestRows, &complete, &trust)
	finish()
	if err != nil {
		return model.Snapshot{}, err
	}
	snapshot := model.Snapshot{
		Event:                event,
		PublicationWatermark: publicationWatermark,
		CompleteHistory:      complete,
		TrustMode:            trust,
	}
	if manifestRows == 0 || !snapshot.Valid() {
		return model.Snapshot{}, ErrInvalidDataset
	}
	return snapshot, nil
}
