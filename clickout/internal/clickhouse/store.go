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
)

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
	started := time.Now()
	var rows atomic.Uint64
	var bytes atomic.Uint64
	var serverElapsed atomic.Int64
	ctx, cancel := context.WithTimeout(parent, store.queryTimeout)
	queryCtx := ch.Context(ctx,
		ch.WithSettings(ch.Settings{
			"join_use_nulls":     1,
			"max_execution_time": uint64(store.queryTimeout / time.Second),
		}),
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

func (store *Store) Snapshot(ctx context.Context, point model.AtPoint) (model.Snapshot, error) {
	var event uint64
	var count uint64
	queryCtx, finish := store.instrument(ctx, "snapshot_event")
	if point.Tip {
		err := store.conn.QueryRow(queryCtx, snapshotTipSQL).Scan(&event)
		finish()
		if err != nil {
			return model.Snapshot{}, err
		}
	} else if point.BlockHash != nil {
		err := store.conn.QueryRow(queryCtx, snapshotAtBlockSQL, hashArgument(*point.BlockHash)).Scan(&count, &event)
		finish()
		if err != nil {
			return model.Snapshot{}, err
		}
		if count == 0 {
			return model.Snapshot{}, ErrNotFound
		}
	} else if point.Event != nil {
		var tip uint64
		err := store.conn.QueryRow(queryCtx, snapshotPinnedSQL, *point.Event).Scan(&tip, &count)
		finish()
		if err != nil {
			return model.Snapshot{}, err
		}
		if *point.Event > tip || (*point.Event != 0 && count == 0) {
			return model.Snapshot{}, ErrNotFound
		}
		event = *point.Event
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
		Event:           event,
		CompleteHistory: complete,
		TrustMode:       trust,
	}
	if manifestRows == 0 || !snapshot.Valid() {
		return model.Snapshot{}, ErrInvalidDataset
	}
	return snapshot, nil
}
