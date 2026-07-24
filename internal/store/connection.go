package store

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"

	"cardano-clicksync/internal/config"
)

const defaultOpenConnections = 16

type batch interface {
	Append(...any) error
	Send() error
	Abort() error
}

type rows interface {
	Next() bool
	Scan(...any) error
	Err() error
	Close() error
}

type row interface {
	Scan(...any) error
}

type connection interface {
	Exec(context.Context, string, ...any) error
	PrepareBatch(context.Context, string) (batch, error)
	Query(context.Context, string, ...any) (rows, error)
	QueryRow(context.Context, string, ...any) row
	Ping(context.Context) error
	Close() error
}

type nativeConnection struct {
	conn clickhouse.Conn
}

func (c nativeConnection) Exec(ctx context.Context, query string, args ...any) error {
	return c.conn.Exec(ctx, query, args...)
}

func (c nativeConnection) PrepareBatch(ctx context.Context, query string) (batch, error) {
	return c.conn.PrepareBatch(ctx, query)
}

func (c nativeConnection) Query(ctx context.Context, query string, args ...any) (rows, error) {
	return c.conn.Query(ctx, query, args...)
}

func (c nativeConnection) QueryRow(ctx context.Context, query string, args ...any) row {
	return c.conn.QueryRow(ctx, query, args...)
}

func (c nativeConnection) Ping(ctx context.Context) error {
	return c.conn.Ping(ctx)
}

func (c nativeConnection) Close() error {
	return c.conn.Close()
}

type DB struct {
	conn     connection
	writerID uuid.UUID
	now      func() time.Time

	initializeMu sync.Mutex
	operationMu  sync.Mutex
	identity     *DatasetIdentity
	allocator    *Allocator
}

func Open(cfg config.Database) (*DB, error) {
	openConnections := cfg.OpenConn
	if openConnections == 0 {
		openConnections = defaultOpenConnections
	}
	options := clickHouseOptions(cfg, openConnections)
	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("open ClickHouse: %w", err)
	}
	return newDB(nativeConnection{conn: conn}), nil
}

func clickHouseOptions(cfg config.Database, openConnections int) *clickhouse.Options {
	return &clickhouse.Options{
		Addr: []string{net.JoinHostPort(cfg.Host, strconv.Itoa(int(cfg.Port)))},
		Auth: clickhouse.Auth{
			// Queries are fully qualified and the migration must be able to
			// create clicksync before it exists.
			Database: "default",
			Username: cfg.User,
			Password: cfg.Password,
		},
		Protocol: clickhouse.Native,
		Settings: clickhouse.Settings{
			"async_insert":          0,
			"wait_for_async_insert": 1,
		},
		Compression:     &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		DialTimeout:     10 * time.Second,
		MaxOpenConns:    openConnections,
		MaxIdleConns:    openConnections,
		ConnMaxLifetime: time.Hour,
	}
}

func newDB(conn connection) *DB {
	return &DB{
		conn:     conn,
		writerID: uuid.New(),
		now: func() time.Time {
			return time.Now().UTC().Truncate(time.Microsecond)
		},
	}
}

func (d *DB) Ping(ctx context.Context) error {
	if d == nil || d.conn == nil {
		return errorsNewNilDB()
	}
	if err := d.conn.Ping(ctx); err != nil {
		return fmt.Errorf("ping ClickHouse: %w", err)
	}
	return nil
}

func (d *DB) Close() error {
	if d == nil || d.conn == nil {
		return nil
	}
	return d.conn.Close()
}

func errorsNewNilDB() error {
	return fmt.Errorf("nil ClickHouse store")
}
