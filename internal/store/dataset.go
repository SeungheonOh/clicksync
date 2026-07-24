package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"cardano-clicksync/internal/model"
	"cardano-clicksync/migrations"
)

const loadDatasetSQL = `
SELECT
    dataset_id,
    schema_hash,
    network_magic,
    network_name,
    start_origin,
    start_slot,
    start_hash,
    start_block_number,
    start_is_byron_ebb,
    created_at,
    source_build
FROM clicksync.dataset
ORDER BY dataset_id, created_at`

const insertDatasetSQL = `INSERT INTO clicksync.dataset
(
    dataset_id, schema_hash, network_magic, network_name, start_origin,
    start_slot, start_hash, start_block_number, start_is_byron_ebb,
    created_at, source_build
)`

func (d *DB) Initialize(
	ctx context.Context,
	config DatasetConfig,
) (DatasetIdentity, error) {
	if d == nil || d.conn == nil {
		return DatasetIdentity{}, errorsNewNilDB()
	}
	if err := validateDatasetConfig(config); err != nil {
		return DatasetIdentity{}, err
	}

	d.initializeMu.Lock()
	defer d.initializeMu.Unlock()
	if d.identity != nil {
		if err := identityMatchesConfig(*d.identity, config); err != nil {
			return DatasetIdentity{}, err
		}
		return *d.identity, nil
	}

	rows, err := d.loadDataset(ctx)
	if err != nil {
		return DatasetIdentity{}, err
	}
	var identity DatasetIdentity
	if len(rows) == 0 {
		identity = DatasetIdentity{
			DatasetID:    uuid.New(),
			SchemaHash:   model.Hash32(migrations.SchemaHash),
			NetworkMagic: config.NetworkMagic,
			NetworkName:  config.NetworkName,
			Start:        config.Start,
			CreatedAt:    d.now(),
			SourceBuild:  config.SourceBuild,
		}
		insertErr := d.insertDataset(ctx, identity)
		rows, readErr := d.loadDataset(ctx)
		if readErr != nil {
			if insertErr != nil {
				return DatasetIdentity{}, errors.Join(
					fmt.Errorf("insert immutable dataset: %w", insertErr),
					fmt.Errorf("resolve immutable dataset insert: %w", readErr),
				)
			}
			return DatasetIdentity{}, fmt.Errorf("read initialized dataset: %w", readErr)
		}
		if len(rows) == 0 {
			if insertErr != nil {
				return DatasetIdentity{}, fmt.Errorf("insert immutable dataset: %w", insertErr)
			}
			return DatasetIdentity{}, errors.New("dataset row is absent after acknowledged insert")
		}
		for _, row := range rows {
			if !sameDatasetIdentity(identity, row) {
				return DatasetIdentity{}, errors.New(
					"dataset initialization has conflicting physical rows",
				)
			}
		}
		// A lost response is resolved by exact row equality.
	} else {
		identity = rows[0]
		for _, row := range rows[1:] {
			if !sameDatasetIdentity(identity, row) {
				return DatasetIdentity{}, errors.New(
					"dataset contains conflicting immutable identities",
				)
			}
		}
	}
	if err := identityMatchesConfig(identity, config); err != nil {
		return DatasetIdentity{}, err
	}
	allocator, err := d.loadAllocator(ctx)
	if err != nil {
		return DatasetIdentity{}, err
	}
	d.identity = &identity
	d.allocator = allocator
	return identity, nil
}

func (d *DB) loadDataset(ctx context.Context) ([]DatasetIdentity, error) {
	result, err := d.conn.Query(ctx, loadDatasetSQL)
	if err != nil {
		return nil, fmt.Errorf("read immutable dataset: %w", err)
	}
	defer result.Close()
	var identities []DatasetIdentity
	for result.Next() {
		var (
			identity    DatasetIdentity
			schemaHash  []byte
			startSlot   *uint64
			startHash   []byte
			startNumber *uint64
		)
		if err := result.Scan(
			&identity.DatasetID,
			&schemaHash,
			&identity.NetworkMagic,
			&identity.NetworkName,
			&identity.Start.Origin,
			&startSlot,
			&startHash,
			&startNumber,
			&identity.Start.IsByronEBB,
			&identity.CreatedAt,
			&identity.SourceBuild,
		); err != nil {
			return nil, fmt.Errorf("scan immutable dataset: %w", err)
		}
		convertedSchema, err := hash32(schemaHash)
		if err != nil {
			return nil, fmt.Errorf("dataset schema hash: %w", err)
		}
		identity.SchemaHash = convertedSchema
		if identity.Start.Origin {
			if startSlot != nil || len(startHash) != 0 || startNumber != nil ||
				identity.Start.IsByronEBB {
				return nil, errors.New("dataset Origin start has invalid nullable fields")
			}
		} else {
			if startSlot == nil || startNumber == nil {
				return nil, errors.New("dataset start point is incomplete")
			}
			convertedStart, err := hash32(startHash)
			if err != nil {
				return nil, fmt.Errorf("dataset start hash: %w", err)
			}
			identity.Start.Slot = *startSlot
			identity.Start.Hash = convertedStart
			identity.Start.BlockNumber = *startNumber
		}
		identity.CreatedAt = identity.CreatedAt.UTC().Truncate(time.Microsecond)
		identities = append(identities, identity)
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("iterate immutable dataset: %w", err)
	}
	return identities, nil
}

func (d *DB) insertDataset(
	ctx context.Context,
	identity DatasetIdentity,
) error {
	batch, err := d.conn.PrepareBatch(ctx, insertDatasetSQL)
	if err != nil {
		return err
	}
	var startSlot, startHash, startNumber any
	if !identity.Start.Origin {
		startSlot = identity.Start.Slot
		startHash = bytes32(identity.Start.Hash)
		startNumber = identity.Start.BlockNumber
	}
	if err := batch.Append(
		identity.DatasetID,
		bytes32(identity.SchemaHash),
		identity.NetworkMagic,
		identity.NetworkName,
		identity.Start.Origin,
		startSlot,
		startHash,
		startNumber,
		identity.Start.IsByronEBB,
		identity.CreatedAt,
		identity.SourceBuild,
	); err != nil {
		_ = batch.Abort()
		return err
	}
	return batch.Send()
}

func validateDatasetConfig(config DatasetConfig) error {
	switch {
	case config.NetworkMagic == 0:
		return errors.New("dataset network magic must be non-zero")
	case strings.TrimSpace(config.NetworkName) == "":
		return errors.New("dataset network name is empty")
	case strings.TrimSpace(config.SourceBuild) == "":
		return errors.New("dataset source build is empty")
	case !validPoint(config.Start):
		return errors.New("dataset start point has invalid shape")
	default:
		return nil
	}
}

func identityMatchesConfig(
	identity DatasetIdentity,
	config DatasetConfig,
) error {
	if err := identityUsesCurrentSchema(identity); err != nil {
		return err
	}
	if identity.NetworkMagic != config.NetworkMagic ||
		identity.NetworkName != config.NetworkName ||
		identity.Start != config.Start {
		return errors.New("configured dataset identity conflicts with immutable dataset")
	}
	return nil
}

func identityUsesCurrentSchema(identity DatasetIdentity) error {
	if identity.SchemaHash != model.Hash32(migrations.SchemaHash) {
		return fmt.Errorf(
			"dataset schema hash %x differs from executable schema %x",
			identity.SchemaHash,
			migrations.SchemaHash,
		)
	}
	return nil
}

func sameDatasetIdentity(left, right DatasetIdentity) bool {
	return left.DatasetID == right.DatasetID &&
		bytes.Equal(left.SchemaHash[:], right.SchemaHash[:]) &&
		left.NetworkMagic == right.NetworkMagic &&
		left.NetworkName == right.NetworkName &&
		left.Start == right.Start &&
		left.CreatedAt.Equal(right.CreatedAt) &&
		left.SourceBuild == right.SourceBuild
}
