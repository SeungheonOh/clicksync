package store

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"clicksync/internal/config"
	"clicksync/migrations"
)

type DB struct {
	conn       clickhouse.Conn
	manifestMu sync.Mutex
}

func Open(cfg config.Config) (*DB, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{net.JoinHostPort(cfg.ClickHouseHost, fmt.Sprint(cfg.ClickHousePort))},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: cfg.ClickHouseUser,
			Password: cfg.ClickHousePassword,
		},
		Protocol:    clickhouse.Native,
		DialTimeout: 10 * time.Second,
		Settings: clickhouse.Settings{
			"join_use_nulls":                         1,
			"min_table_rows_to_use_projection_index": 0,
		},
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	})
	if err != nil {
		return nil, fmt.Errorf("open ClickHouse: %w", err)
	}
	return &DB{conn: conn}, nil
}

func (d *DB) Close() error {
	if d == nil || d.conn == nil {
		return nil
	}
	return d.conn.Close()
}

func (d *DB) Ping(ctx context.Context) error {
	if err := d.conn.Ping(ctx); err != nil {
		return fmt.Errorf("ping ClickHouse: %w", err)
	}
	return nil
}

func (d *DB) Migrate(ctx context.Context) error {
	statements, err := splitSQL(migrations.Initial)
	if err != nil {
		return err
	}
	for index, statement := range statements {
		if err := d.conn.Exec(ctx, statement); err != nil {
			return fmt.Errorf("migration statement %d: %w", index+1, err)
		}
	}
	return nil
}

func splitSQL(source string) ([]string, error) {
	var statements []string
	var current strings.Builder
	var quote rune
	lineComment := false
	blockComment := false
	runes := []rune(source)
	for i := 0; i < len(runes); i++ {
		ch := runes[i]
		next := rune(0)
		if i+1 < len(runes) {
			next = runes[i+1]
		}
		switch {
		case lineComment:
			if ch == '\n' {
				lineComment = false
				current.WriteRune(ch)
			}
		case blockComment:
			if ch == '*' && next == '/' {
				blockComment = false
				i++
			}
		case quote != 0:
			current.WriteRune(ch)
			if ch == '\\' && i+1 < len(runes) {
				i++
				current.WriteRune(runes[i])
			} else if ch == quote {
				if i+1 < len(runes) && runes[i+1] == quote {
					i++
					current.WriteRune(runes[i])
				} else {
					quote = 0
				}
			}
		case ch == '-' && next == '-':
			lineComment = true
			i++
		case ch == '/' && next == '*':
			blockComment = true
			i++
		case ch == '\'' || ch == '"' || ch == '`':
			quote = ch
			current.WriteRune(ch)
		case ch == ';':
			if value := strings.TrimSpace(current.String()); value != "" {
				statements = append(statements, value)
			}
			current.Reset()
		default:
			current.WriteRune(ch)
		}
	}
	if quote != 0 || blockComment {
		return nil, fmt.Errorf("unterminated SQL token")
	}
	if value := strings.TrimSpace(current.String()); value != "" {
		statements = append(statements, value)
	}
	return statements, nil
}
