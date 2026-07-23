// Package migrations embeds the sole ClickHouse schema.
package migrations

import (
	"crypto/sha256"
	_ "embed"
)

// Initial is the complete, idempotent schema migration.
//
//go:embed 001_initial.sql
var Initial string

// ContractDescriptor is the canonical, non-recursive content identity for a
// freshly created Clicksync dataset. It deliberately describes the semantic
// storage/query contract instead of hashing the DDL that stores the hash.
//
//go:embed schema_contract.txt
var ContractDescriptor string

// ContractHash is persisted as immutable manifest identity. A changed
// descriptor is a different dataset contract, not a compatibility version.
var ContractHash = sha256.Sum256([]byte(ContractDescriptor))
