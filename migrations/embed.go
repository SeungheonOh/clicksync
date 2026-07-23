// Package migrations embeds the sole ClickHouse schema.
package migrations

import _ "embed"

// Initial is the complete, idempotent schema migration.
//
//go:embed 001_initial.sql
var Initial string
