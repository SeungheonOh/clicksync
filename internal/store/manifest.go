package store

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"

	"clicksync/internal/model"
	"clicksync/internal/publication"
)

type ManifestSeed struct {
	DatasetID              [16]byte
	NetworkMagic           uint32
	NetworkName            string
	ByronGenesisID         model.Hash32
	ByronGenesisJSONHash   model.Hash32
	ShelleyGenesisID       model.Hash32
	ShelleyGenesisJSONHash model.Hash32
	Start                  publication.Point
	WriterID               [16]byte
	WriterBuild            string
	SourceBuild            string
	CreatedAt              time.Time
	OriginGenesis          *OriginGenesisProof
}

type OriginGenesisProof struct {
	ByronJSONHash   model.Hash32
	ShelleyJSONHash model.Hash32
	AVVMEntries     uint32
	InitialSupply   uint64
}

type ManifestIdentity struct {
	DatasetID              [16]byte
	NetworkMagic           uint32
	NetworkName            string
	ByronGenesisID         model.Hash32
	ByronGenesisJSONHash   model.Hash32
	ShelleyGenesisID       model.Hash32
	ShelleyGenesisJSONHash model.Hash32
	Start                  publication.Point
	GenesisSeeded          bool
	CompleteHistory        bool
}

type GenesisPublication struct {
	PublicationID    uint64
	FactsDigest      model.Hash32
	TransactionCount uint32
	OutputCount      uint32
	InitialSupply    uint64
}

const (
	mainnetMagic               = uint32(764824073)
	mainnetByronGenesisIDHex   = "5f20df933584822601f9e3f8c024eb5eb252fe8cefb24d1317dc3d432e940ebb"
	mainnetByronGenesisJSONHex = "dbbdaeab0ea4ea58225892d8b1294f178b417f4a9d1ed3bbf629c40d8f74e86b"
	mainnetShelleyGenesisIDHex = "1a3be38bcbb7911969283716ad7aa550250226b76a61fc51cc9a9a35d9276d81"
	mainnetAVVMEntries         = uint32(14505)
	mainnetInitialSupply       = uint64(31112484745000000)
)

func MainnetGenesisIdentity() (
	byronID model.Hash32,
	byronJSON model.Hash32,
	shelleyID model.Hash32,
	shelleyJSON model.Hash32,
) {
	byronID = mustManifestHash(mainnetByronGenesisIDHex)
	byronJSON = mustManifestHash(mainnetByronGenesisJSONHex)
	shelleyID = mustManifestHash(mainnetShelleyGenesisIDHex)
	shelleyJSON = shelleyID
	return
}

func validatePinnedMainnet(seed ManifestSeed) error {
	byronID, byronJSON, shelleyID, shelleyJSON := MainnetGenesisIdentity()
	if seed.NetworkName != "mainnet" ||
		seed.NetworkMagic != mainnetMagic ||
		seed.ByronGenesisID != byronID ||
		seed.ByronGenesisJSONHash != byronJSON ||
		seed.ShelleyGenesisID != shelleyID ||
		seed.ShelleyGenesisJSONHash != shelleyJSON {
		return errors.New("dataset identity does not match the pinned mainnet network/genesis tuple")
	}
	return nil
}

func validateOriginGenesisProof(seed ManifestSeed) error {
	if !seed.Start.Origin || seed.OriginGenesis == nil {
		return errors.New("fresh Origin startup requires verified official genesis inputs")
	}
	proof := *seed.OriginGenesis
	if proof.ByronJSONHash != seed.ByronGenesisJSONHash ||
		proof.ShelleyJSONHash != seed.ShelleyGenesisJSONHash ||
		proof.AVVMEntries != mainnetAVVMEntries ||
		proof.InitialSupply != mainnetInitialSupply {
		return errors.New("Origin genesis proof does not match the pinned mainnet distribution")
	}
	return nil
}

func mustManifestHash(encoded string) model.Hash32 {
	decoded, err := hex.DecodeString(encoded)
	if err != nil || len(decoded) != 32 {
		panic("invalid compiled mainnet genesis identity")
	}
	var hash model.Hash32
	copy(hash[:], decoded)
	return hash
}

func (d *DB) InitializeManifest(
	ctx context.Context,
	seed ManifestSeed,
) error {
	if seed.DatasetID == ([16]byte{}) || seed.WriterID == ([16]byte{}) {
		return errors.New("manifest dataset and writer IDs must be non-zero")
	}
	if seed.NetworkMagic == 0 || seed.NetworkName == "" {
		return errors.New("manifest network identity is required")
	}
	if err := validatePinnedMainnet(seed); err != nil {
		return err
	}
	var rows uint64
	if err := d.conn.QueryRow(ctx, `SELECT count() FROM clicksync.dataset_manifest`).Scan(&rows); err != nil {
		return fmt.Errorf("count manifest rows: %w", err)
	}
	if rows != 0 {
		return d.validateManifestIdentity(ctx, seed)
	}
	createdAt := seed.CreatedAt.UTC()
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	startKind := "intersection"
	var startSlot, startHash, startBlockNumber any
	startIsByronEBB := false
	if seed.Start.Origin {
		startKind = "origin"
	} else {
		startSlot = seed.Start.Slot
		startHash = bytesOf32(seed.Start.Hash)
		startBlockNumber = seed.Start.BlockNumber
		startIsByronEBB = seed.Start.IsByronEBB
	}
	const query = `INSERT INTO clicksync.dataset_manifest
(
    manifest_key, revision, dataset_id, network_magic, network_name,
    byron_genesis_id, byron_genesis_json_hash, shelley_genesis_id,
    shelley_genesis_json_hash, start_kind, start_slot,
    start_hash, start_block_number, start_is_byron_ebb,
    genesis_seeded, complete_history, trust_mode,
    committed_event_seq, committed_tip_origin, committed_tip_slot,
    committed_tip_hash, committed_tip_block_number, committed_tip_is_byron_ebb,
    writer_id, writer_build,
    source_build, created_at, updated_at
)`
	batch, err := d.conn.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("prepare initial manifest: %w", err)
	}
	if err := batch.Append(
		uint8(1),
		uint64(1),
		uuid.UUID(seed.DatasetID),
		seed.NetworkMagic,
		seed.NetworkName,
		bytesOf32(seed.ByronGenesisID),
		bytesOf32(seed.ByronGenesisJSONHash),
		bytesOf32(seed.ShelleyGenesisID),
		bytesOf32(seed.ShelleyGenesisJSONHash),
		startKind,
		startSlot,
		startHash,
		startBlockNumber,
		startIsByronEBB,
		false,
		false,
		"peer_observed_structurally_verified",
		uint64(0),
		seed.Start.Origin,
		startSlot,
		startHash,
		startBlockNumber,
		startIsByronEBB,
		uuid.UUID(seed.WriterID),
		seed.WriterBuild,
		seed.SourceBuild,
		createdAt,
		createdAt,
	); err != nil {
		_ = batch.Abort()
		return fmt.Errorf("append initial manifest: %w", err)
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("insert initial manifest: %w", err)
	}
	return nil
}

// LoadOrCreateManifest is the startup contract for the sole writer. It
// generates dataset_id only while the held flock proves the manifest is truly
// empty, reuses the stored identity on restart, and reconciles the cache from
// authoritative adoption/rollback headers before returning.
func (d *DB) LoadOrCreateManifest(
	ctx context.Context,
	lock LockAssertion,
	seed ManifestSeed,
) (ManifestIdentity, error) {
	if lock == nil {
		return ManifestIdentity{}, errors.New("manifest startup requires the real writer flock")
	}
	if err := lock.AssertHeld(); err != nil {
		return ManifestIdentity{}, fmt.Errorf("manifest startup flock is not held: %w", err)
	}
	if seed.DatasetID != ([16]byte{}) {
		return ManifestIdentity{}, errors.New("startup dataset ID must be generated or loaded by Clicksync")
	}
	if seed.WriterID == ([16]byte{}) {
		return ManifestIdentity{}, errors.New("manifest startup writer ID is zero")
	}
	if err := validatePinnedMainnet(seed); err != nil {
		return ManifestIdentity{}, err
	}
	identity, found, err := d.LoadManifestIdentityIfExists(ctx)
	if err != nil {
		return ManifestIdentity{}, err
	}
	if !found {
		if seed.Start.Origin {
			if err := validateOriginGenesisProof(seed); err != nil {
				return ManifestIdentity{}, err
			}
		} else if seed.Start.Hash == (model.Hash32{}) {
			return ManifestIdentity{}, errors.New("partial-history startup point hash is zero")
		}
		datasetID, err := uuid.NewRandom()
		if err != nil {
			return ManifestIdentity{}, fmt.Errorf("generate dataset ID: %w", err)
		}
		copy(seed.DatasetID[:], datasetID[:])
		if err := lock.AssertHeld(); err != nil {
			return ManifestIdentity{}, fmt.Errorf("manifest creation flock was lost: %w", err)
		}
		if err := d.InitializeManifest(ctx, seed); err != nil {
			return ManifestIdentity{}, err
		}
	} else {
		if identity.NetworkMagic != seed.NetworkMagic ||
			identity.NetworkName != seed.NetworkName ||
			identity.ByronGenesisID != seed.ByronGenesisID ||
			identity.ByronGenesisJSONHash != seed.ByronGenesisJSONHash ||
			identity.ShelleyGenesisID != seed.ShelleyGenesisID ||
			identity.ShelleyGenesisJSONHash != seed.ShelleyGenesisJSONHash ||
			!configuredStartMatchesStored(seed.Start, identity.Start) {
			return ManifestIdentity{}, errors.New(
				"configured network/genesis/start point conflicts with stored dataset identity",
			)
		}
	}
	if err := lock.AssertHeld(); err != nil {
		return ManifestIdentity{}, fmt.Errorf("manifest reconciliation flock was lost: %w", err)
	}
	reconciledAt := seed.CreatedAt.UTC()
	if reconciledAt.IsZero() {
		reconciledAt = time.Now().UTC()
	}
	if err := d.ReconcileManifest(
		ctx,
		seed.WriterID,
		seed.WriterBuild,
		reconciledAt,
	); err != nil {
		return ManifestIdentity{}, err
	}
	return d.LoadManifestIdentity(ctx)
}

func configuredStartMatchesStored(configured, stored publication.Point) bool {
	if configured.Origin || stored.Origin {
		return configured.Origin == stored.Origin
	}
	return configured.Slot == stored.Slot && configured.Hash == stored.Hash
}

func (d *DB) validateManifestIdentity(ctx context.Context, seed ManifestSeed) error {
	const query = `
SELECT
    dataset_id, network_magic, network_name, byron_genesis_id,
    byron_genesis_json_hash, shelley_genesis_id, shelley_genesis_json_hash,
    start_kind, start_slot, start_hash, start_block_number, start_is_byron_ebb
FROM clicksync.dataset_manifest
ORDER BY revision DESC
LIMIT 1`
	var dataset uuid.UUID
	var magic uint32
	var network string
	var byronID, byronJSON, shelleyID, shelleyJSON []byte
	var startKind string
	var startSlot, startNumber *uint64
	var startHash []byte
	var startIsByronEBB bool
	if err := d.conn.QueryRow(ctx, query).Scan(
		&dataset,
		&magic,
		&network,
		&byronID,
		&byronJSON,
		&shelleyID,
		&shelleyJSON,
		&startKind,
		&startSlot,
		&startHash,
		&startNumber,
		&startIsByronEBB,
	); err != nil {
		return fmt.Errorf("read manifest identity: %w", err)
	}
	if dataset != uuid.UUID(seed.DatasetID) ||
		magic != seed.NetworkMagic ||
		network != seed.NetworkName ||
		!equalHashBytes(seed.ByronGenesisID, byronID) ||
		!equalHashBytes(seed.ByronGenesisJSONHash, byronJSON) ||
		!equalHashBytes(seed.ShelleyGenesisID, shelleyID) ||
		!equalHashBytes(seed.ShelleyGenesisJSONHash, shelleyJSON) ||
		!manifestStartMatches(
			seed.Start,
			startKind,
			startSlot,
			startHash,
			startNumber,
			startIsByronEBB,
		) {
		return errors.New("configured dataset/network/genesis identity conflicts with existing manifest")
	}
	return nil
}

func manifestStartMatches(
	expected publication.Point,
	kind string,
	slot *uint64,
	hash []byte,
	number *uint64,
	isByronEBB bool,
) bool {
	if expected.Origin {
		return kind == "origin" && slot == nil && len(hash) == 0 &&
			number == nil && !isByronEBB
	}
	return kind == "intersection" &&
		slot != nil && *slot == expected.Slot &&
		equalHashBytes(expected.Hash, hash) &&
		number != nil && *number == expected.BlockNumber &&
		isByronEBB == expected.IsByronEBB
}

func (d *DB) LoadManifestIdentity(ctx context.Context) (ManifestIdentity, error) {
	identity, found, err := d.LoadManifestIdentityIfExists(ctx)
	if err != nil {
		return ManifestIdentity{}, err
	}
	if !found {
		return ManifestIdentity{}, errors.New("dataset manifest is not initialized")
	}
	return identity, nil
}

// LoadManifestIdentityIfExists is the bootstrap-safe identity read. A fresh
// database returns found=false without relying on driver-specific no-row error
// text. Byte-identical lost-response duplicates of the latest revision are
// tolerated; conflicting latest rows fail closed.
func (d *DB) LoadManifestIdentityIfExists(
	ctx context.Context,
) (ManifestIdentity, bool, error) {
	const query = `
SELECT
    dataset_id, network_magic, network_name, byron_genesis_id,
    byron_genesis_json_hash, shelley_genesis_id, shelley_genesis_json_hash,
    start_kind, start_slot, start_hash, start_block_number, start_is_byron_ebb,
    genesis_seeded,
    complete_history
FROM clicksync.dataset_manifest
WHERE revision = (SELECT max(revision) FROM clicksync.dataset_manifest)`
	rows, err := d.conn.Query(ctx, query)
	if err != nil {
		return ManifestIdentity{}, false, fmt.Errorf("query manifest identity: %w", err)
	}
	defer rows.Close()
	var identities []ManifestIdentity
	for rows.Next() {
		identity, err := scanManifestIdentity(rows.Scan)
		if err != nil {
			return ManifestIdentity{}, false, err
		}
		identities = append(identities, identity)
	}
	if err := rows.Err(); err != nil {
		return ManifestIdentity{}, false, fmt.Errorf("iterate manifest identity: %w", err)
	}
	return uniqueManifestIdentity(identities)
}

func scanManifestIdentity(scan func(...any) error) (ManifestIdentity, error) {
	var (
		dataset                          uuid.UUID
		magic                            uint32
		network, startKind               string
		byronIDBytes, byronJSONBytes     []byte
		shelleyIDBytes, shelleyJSONBytes []byte
		startSlot, startNumber           *uint64
		startHashBytes                   []byte
		startIsByronEBB                  bool
		genesisSeeded, completeHistory   bool
	)
	if err := scan(
		&dataset,
		&magic,
		&network,
		&byronIDBytes,
		&byronJSONBytes,
		&shelleyIDBytes,
		&shelleyJSONBytes,
		&startKind,
		&startSlot,
		&startHashBytes,
		&startNumber,
		&startIsByronEBB,
		&genesisSeeded,
		&completeHistory,
	); err != nil {
		return ManifestIdentity{}, fmt.Errorf("scan manifest identity: %w", err)
	}
	byronID, err := hash32(byronIDBytes)
	if err != nil {
		return ManifestIdentity{}, err
	}
	byronJSON, err := hash32(byronJSONBytes)
	if err != nil {
		return ManifestIdentity{}, err
	}
	shelleyID, err := hash32(shelleyIDBytes)
	if err != nil {
		return ManifestIdentity{}, err
	}
	shelleyJSON, err := hash32(shelleyJSONBytes)
	if err != nil {
		return ManifestIdentity{}, err
	}
	start := publication.Point{Origin: true}
	if startKind == "intersection" {
		if startSlot == nil || startNumber == nil {
			return ManifestIdentity{}, errors.New("manifest intersection is incomplete")
		}
		startHash, err := hash32(startHashBytes)
		if err != nil {
			return ManifestIdentity{}, err
		}
		start = publication.Point{
			Slot:        *startSlot,
			Hash:        startHash,
			BlockNumber: *startNumber,
			IsByronEBB:  startIsByronEBB,
		}
	} else if startKind != "origin" {
		return ManifestIdentity{}, fmt.Errorf("unknown manifest start kind %q", startKind)
	} else if startSlot != nil || len(startHashBytes) != 0 ||
		startNumber != nil || startIsByronEBB {
		return ManifestIdentity{}, errors.New("manifest Origin boundary carries non-Origin metadata")
	}
	var datasetID [16]byte
	copy(datasetID[:], dataset[:])
	return ManifestIdentity{
		DatasetID:              datasetID,
		NetworkMagic:           magic,
		NetworkName:            network,
		ByronGenesisID:         byronID,
		ByronGenesisJSONHash:   byronJSON,
		ShelleyGenesisID:       shelleyID,
		ShelleyGenesisJSONHash: shelleyJSON,
		Start:                  start,
		GenesisSeeded:          genesisSeeded,
		CompleteHistory:        completeHistory,
	}, nil
}

func uniqueManifestIdentity(
	identities []ManifestIdentity,
) (ManifestIdentity, bool, error) {
	if len(identities) == 0 {
		return ManifestIdentity{}, false, nil
	}
	first := identities[0]
	for _, identity := range identities[1:] {
		if identity != first {
			return ManifestIdentity{}, false, errors.New(
				"latest dataset manifest revision has conflicting physical rows",
			)
		}
	}
	return first, true, nil
}

func (d *DB) GenesisState(ctx context.Context) (bool, bool, error) {
	identity, err := d.LoadManifestIdentity(ctx)
	if err != nil {
		return false, false, err
	}
	return identity.Start.Origin, identity.GenesisSeeded, nil
}

// RecoverGenesisPublication closes the crash window between the authoritative
// synthetic adoption and the manifest seed marker. It returns only one exact
// active mainnet distribution; orphan fact attempts are ignored.
func (d *DB) RecoverGenesisPublication(
	ctx context.Context,
	expectedDigest model.Hash32,
) (GenesisPublication, bool, error) {
	snapshot, err := d.CommittedSnapshot(ctx)
	if err != nil {
		return GenesisPublication{}, false, err
	}
	const query = `
SELECT
    publication_id,
    count(),
    countDistinct(facts_digest),
    any(facts_digest),
    any(transaction_count),
    any(input_count),
    any(output_count)
FROM clicksync.blocks
WHERE synthetic
GROUP BY publication_id`
	rows, err := d.conn.Query(ctx, query)
	if err != nil {
		return GenesisPublication{}, false, fmt.Errorf("query synthetic genesis candidates: %w", err)
	}
	type candidate struct {
		publication GenesisPublication
		rows        uint64
		variants    uint64
		inputs      uint32
	}
	candidates := make(map[uint64]candidate)
	var ids []uint64
	for rows.Next() {
		var item candidate
		var digestBytes []byte
		if err := rows.Scan(
			&item.publication.PublicationID,
			&item.rows,
			&item.variants,
			&digestBytes,
			&item.publication.TransactionCount,
			&item.inputs,
			&item.publication.OutputCount,
		); err != nil {
			rows.Close()
			return GenesisPublication{}, false, fmt.Errorf("scan synthetic genesis candidate: %w", err)
		}
		item.publication.FactsDigest, err = hash32(digestBytes)
		if err != nil {
			rows.Close()
			return GenesisPublication{}, false, err
		}
		item.publication.InitialSupply = mainnetInitialSupply
		candidates[item.publication.PublicationID] = item
		ids = append(ids, item.publication.PublicationID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return GenesisPublication{}, false, fmt.Errorf("iterate synthetic genesis candidates: %w", err)
	}
	rows.Close()
	if len(ids) == 0 {
		return GenesisPublication{}, false, nil
	}
	active, err := d.activeCandidatePublications(ctx, snapshot, ids)
	if err != nil {
		return GenesisPublication{}, false, err
	}
	if len(active) == 0 {
		return GenesisPublication{}, false, nil
	}
	if len(active) != 1 {
		return GenesisPublication{}, false, errors.New("multiple active synthetic genesis publications")
	}
	item := candidates[active[0]]
	if item.rows != 1 ||
		item.variants != 1 ||
		item.inputs != 0 ||
		item.publication.FactsDigest != expectedDigest ||
		item.publication.TransactionCount != mainnetAVVMEntries ||
		item.publication.OutputCount != mainnetAVVMEntries {
		return GenesisPublication{}, false, errors.New(
			"active synthetic genesis publication differs from the official distribution",
		)
	}
	if err := d.validateGenesisFacts(
		ctx,
		map[uint64]GenesisPublication{
			item.publication.PublicationID: item.publication,
		},
	); err != nil {
		return GenesisPublication{}, false, err
	}
	return item.publication, true, nil
}

func (d *DB) MarkGenesisSeeded(
	ctx context.Context,
	lock LockAssertion,
	expected []GenesisPublication,
	writerID [16]byte,
	writerBuild string,
	now time.Time,
) error {
	if lock == nil {
		return errors.New("genesis completion requires the real writer flock")
	}
	if len(expected) == 0 {
		return errors.New("genesis completion requires an exact non-empty publication bundle")
	}
	identity, err := d.LoadManifestIdentity(ctx)
	if err != nil {
		return err
	}
	if !identity.Start.Origin {
		return errors.New("partial-history dataset cannot be marked genesis-seeded")
	}
	ids := make([]uint64, 0, len(expected))
	expectedByID := make(map[uint64]GenesisPublication, len(expected))
	for _, item := range expected {
		if item.PublicationID == 0 {
			return errors.New("genesis publication ID is zero")
		}
		if item.TransactionCount == 0 ||
			item.OutputCount == 0 ||
			item.InitialSupply == 0 {
			return errors.New("genesis publication expected counts and supply must be non-zero")
		}
		if _, duplicate := expectedByID[item.PublicationID]; duplicate {
			return errors.New("duplicate expected genesis publication ID")
		}
		expectedByID[item.PublicationID] = item
		ids = append(ids, item.PublicationID)
	}
	const bundleQuery = `
SELECT
    publication_id,
    count(),
    countDistinct(tuple(facts_digest, synthetic)),
    any(facts_digest),
    min(synthetic),
    max(synthetic),
    any(transaction_count),
    any(input_count),
    any(output_count),
    countDistinct(tuple(transaction_count, input_count, output_count))
FROM clicksync.blocks
WHERE publication_id IN ?
GROUP BY publication_id`
	rows, err := d.conn.Query(ctx, bundleQuery, ids)
	if err != nil {
		return fmt.Errorf("query synthetic genesis bundle: %w", err)
	}
	seen := make(map[uint64]struct{}, len(ids))
	for rows.Next() {
		var publicationID uint64
		var rowCount uint64
		var variantCount uint64
		var digestBytes []byte
		var minimumSynthetic bool
		var maximumSynthetic bool
		var transactionCount, inputCount, outputCount uint32
		var countVariants uint64
		if err := rows.Scan(
			&publicationID,
			&rowCount,
			&variantCount,
			&digestBytes,
			&minimumSynthetic,
			&maximumSynthetic,
			&transactionCount,
			&inputCount,
			&outputCount,
			&countVariants,
		); err != nil {
			rows.Close()
			return err
		}
		digest, err := hash32(digestBytes)
		if err != nil {
			rows.Close()
			return err
		}
		want, expected := expectedByID[publicationID]
		if !expected ||
			rowCount != 1 || variantCount != 1 || countVariants != 1 ||
			!minimumSynthetic || !maximumSynthetic ||
			want.FactsDigest != digest ||
			transactionCount != want.TransactionCount ||
			inputCount != 0 ||
			outputCount != want.OutputCount {
			rows.Close()
			return errors.New("synthetic genesis publication has duplicate or conflicting physical block rows")
		}
		seen[publicationID] = struct{}{}
	}
	rows.Close()
	if len(seen) != len(expectedByID) {
		return errors.New("synthetic genesis publication bundle is incomplete")
	}
	snapshot, err := d.CommittedSnapshot(ctx)
	if err != nil {
		return err
	}
	var allSyntheticIDs []uint64
	if err := d.conn.QueryRow(
		ctx,
		`SELECT groupUniqArray(publication_id) FROM clicksync.blocks WHERE synthetic`,
	).Scan(&allSyntheticIDs); err != nil {
		return fmt.Errorf("read all synthetic genesis publications: %w", err)
	}
	active, err := d.activeCandidatePublications(ctx, snapshot, allSyntheticIDs)
	if err != nil {
		return err
	}
	activeSet := make(map[uint64]struct{}, len(active))
	for _, publicationID := range active {
		activeSet[publicationID] = struct{}{}
	}
	if len(activeSet) != len(expectedByID) {
		return errors.New("visible synthetic genesis publication set differs from the exact expected bundle")
	}
	for publicationID := range expectedByID {
		if _, active := activeSet[publicationID]; !active {
			return errors.New("expected synthetic genesis publication is not visible")
		}
	}
	for publicationID := range activeSet {
		if _, expected := expectedByID[publicationID]; !expected {
			return errors.New("unexpected visible synthetic genesis publication")
		}
	}
	if err := d.validateGenesisFacts(ctx, expectedByID); err != nil {
		return err
	}
	if err := lock.AssertHeld(); err != nil {
		return fmt.Errorf("writer flock was lost before genesis tip reconciliation: %w", err)
	}
	if err := d.PersistManifest(ctx, publication.ManifestUpdate{
		EventSeq:    snapshot,
		Tip:         publication.Point{Origin: true},
		WriterID:    writerID,
		WriterBuild: writerBuild,
		UpdatedAt:   now.UTC(),
	}); err != nil {
		return err
	}
	if err := lock.AssertHeld(); err != nil {
		return fmt.Errorf("writer flock was lost before genesis completion marker: %w", err)
	}
	const completeQuery = `
INSERT INTO clicksync.dataset_manifest
SELECT * REPLACE
(
    revision + 1 AS revision,
    true AS genesis_seeded,
    true AS complete_history,
    ? AS updated_at
)
FROM
(
    SELECT *
    FROM clicksync.dataset_manifest
    ORDER BY revision DESC
    LIMIT 1
)`
	if err := d.conn.Exec(ctx, completeQuery, now.UTC()); err != nil {
		return fmt.Errorf("mark exact genesis bundle complete: %w", err)
	}
	return nil
}

func (d *DB) validateGenesisFacts(
	ctx context.Context,
	expected map[uint64]GenesisPublication,
) error {
	ids := make([]uint64, 0, len(expected))
	for publicationID := range expected {
		ids = append(ids, publicationID)
	}
	const transactionQuery = `
SELECT
    publication_id,
    count(),
    uniqExact(tx_hash),
    countIf(flow_kind != 'genesis')
FROM clicksync.transactions
WHERE publication_id IN ?
GROUP BY publication_id`
	transactionRows, err := d.conn.Query(ctx, transactionQuery, ids)
	if err != nil {
		return fmt.Errorf("query exact genesis transactions: %w", err)
	}
	transactionSeen := make(map[uint64]struct{}, len(expected))
	for transactionRows.Next() {
		var publicationID, rows, distinct, wrongKind uint64
		if err := transactionRows.Scan(
			&publicationID,
			&rows,
			&distinct,
			&wrongKind,
		); err != nil {
			transactionRows.Close()
			return fmt.Errorf("scan exact genesis transactions: %w", err)
		}
		want, ok := expected[publicationID]
		if !ok ||
			rows != uint64(want.TransactionCount) ||
			distinct != rows ||
			wrongKind != 0 {
			transactionRows.Close()
			return fmt.Errorf("publication %d genesis transaction set differs", publicationID)
		}
		transactionSeen[publicationID] = struct{}{}
	}
	if err := transactionRows.Err(); err != nil {
		transactionRows.Close()
		return fmt.Errorf("iterate exact genesis transactions: %w", err)
	}
	transactionRows.Close()

	const outputQuery = `
SELECT
    publication_id,
    count(),
    uniqExact(tuple(tx_hash, output_index)),
    sum(lovelace),
    countIf(output_kind != 'genesis')
FROM clicksync.outputs
WHERE publication_id IN ?
GROUP BY publication_id`
	outputRows, err := d.conn.Query(ctx, outputQuery, ids)
	if err != nil {
		return fmt.Errorf("query exact genesis outputs: %w", err)
	}
	outputSeen := make(map[uint64]struct{}, len(expected))
	for outputRows.Next() {
		var publicationID, rows, distinct, supply, wrongKind uint64
		if err := outputRows.Scan(
			&publicationID,
			&rows,
			&distinct,
			&supply,
			&wrongKind,
		); err != nil {
			outputRows.Close()
			return fmt.Errorf("scan exact genesis outputs: %w", err)
		}
		want, ok := expected[publicationID]
		if !ok ||
			rows != uint64(want.OutputCount) ||
			distinct != rows ||
			supply != want.InitialSupply ||
			wrongKind != 0 {
			outputRows.Close()
			return fmt.Errorf("publication %d genesis output set/supply differs", publicationID)
		}
		outputSeen[publicationID] = struct{}{}
	}
	if err := outputRows.Err(); err != nil {
		outputRows.Close()
		return fmt.Errorf("iterate exact genesis outputs: %w", err)
	}
	outputRows.Close()

	var inputs uint64
	if err := d.conn.QueryRow(
		ctx,
		`SELECT count() FROM clicksync.inputs WHERE publication_id IN ?`,
		ids,
	).Scan(&inputs); err != nil {
		return fmt.Errorf("query genesis inputs: %w", err)
	}
	if inputs != 0 {
		return errors.New("synthetic genesis publications contain inputs")
	}
	if len(transactionSeen) != len(expected) || len(outputSeen) != len(expected) {
		return errors.New("synthetic genesis transaction/output set is incomplete")
	}
	return nil
}

func (d *DB) PersistManifest(ctx context.Context, update publication.ManifestUpdate) error {
	committed, err := d.CommittedSnapshot(ctx)
	if err != nil {
		return err
	}
	if update.EventSeq != committed {
		return fmt.Errorf(
			"manifest event %d does not equal committed snapshot %d",
			update.EventSeq,
			committed,
		)
	}
	var latestRevision uint64
	if err := d.conn.QueryRow(
		ctx,
		`SELECT max(revision) FROM clicksync.dataset_manifest`,
	).Scan(&latestRevision); err != nil {
		return fmt.Errorf("read manifest revision: %w", err)
	}
	if latestRevision == 0 {
		return errors.New("dataset manifest is not initialized")
	}
	if latestRevision == math.MaxUint64 {
		return errors.New("dataset manifest revision space exhausted")
	}
	var tipSlot, tipHash, tipNumber any
	tipIsByronEBB := false
	if !update.Tip.Origin {
		tipSlot = update.Tip.Slot
		tipHash = string(update.Tip.Hash[:])
		tipNumber = update.Tip.BlockNumber
		tipIsByronEBB = update.Tip.IsByronEBB
	}
	const query = `INSERT INTO clicksync.dataset_manifest
(
    manifest_key, revision, dataset_id, network_magic, network_name,
    byron_genesis_id, byron_genesis_json_hash, shelley_genesis_id,
    shelley_genesis_json_hash, start_kind, start_slot,
    start_hash, start_block_number, start_is_byron_ebb,
    genesis_seeded, complete_history, trust_mode,
    committed_event_seq, committed_tip_origin, committed_tip_slot,
    committed_tip_hash, committed_tip_block_number, committed_tip_is_byron_ebb,
    writer_id, writer_build,
    source_build, created_at, updated_at
)
SELECT
    manifest_key,
    revision + 1,
    dataset_id,
    network_magic,
    network_name,
    byron_genesis_id,
    byron_genesis_json_hash,
    shelley_genesis_id,
    shelley_genesis_json_hash,
    start_kind,
    start_slot,
    start_hash,
    start_block_number,
    start_is_byron_ebb,
    genesis_seeded,
    complete_history,
    trust_mode,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    source_build,
    created_at,
    ?
FROM
(
    SELECT *
    FROM clicksync.dataset_manifest
    ORDER BY revision DESC
    LIMIT 1
)`
	if err := d.conn.Exec(
		ctx,
		query,
		update.EventSeq,
		update.Tip.Origin,
		tipSlot,
		tipHash,
		tipNumber,
		tipIsByronEBB,
		uuid.UUID(update.WriterID),
		update.WriterBuild,
		update.UpdatedAt.UTC(),
	); err != nil {
		return fmt.Errorf("insert reconciled manifest: %w", err)
	}
	return nil
}

func (d *DB) ReconcileManifest(
	ctx context.Context,
	writerID [16]byte,
	writerBuild string,
	now time.Time,
) error {
	snapshot, err := d.CommittedSnapshot(ctx)
	if err != nil {
		return err
	}
	tip, err := d.committedTip(ctx, snapshot)
	if err != nil {
		return err
	}
	return d.PersistManifest(ctx, publication.ManifestUpdate{
		EventSeq:    snapshot,
		Tip:         tip,
		WriterID:    writerID,
		WriterBuild: writerBuild,
		UpdatedAt:   now.UTC(),
	})
}

func (d *DB) CommittedTip(
	ctx context.Context,
	snapshot uint64,
) (publication.Point, error) {
	return d.committedTip(ctx, snapshot)
}

func (d *DB) committedTip(ctx context.Context, snapshot uint64) (publication.Point, error) {
	if snapshot == 0 {
		return d.manifestStartPoint(ctx)
	}
	const rollbackQuery = `
SELECT
    any(rollback_to_origin),
    any(rollback_to_slot),
    any(rollback_to_hash),
    any(rollback_to_block_number),
    any(rollback_to_is_byron_ebb),
    uniqExact(tuple(
        rollback_to_origin,
        rollback_to_slot,
        rollback_to_hash,
        rollback_to_block_number,
        rollback_to_is_byron_ebb
    ))
FROM clicksync.rollbacks
WHERE event_seq = ?
HAVING count() > 0`
	var (
		origin     bool
		slot       *uint64
		hashBytes  []byte
		number     *uint64
		isByronEBB bool
		variants   uint64
	)
	err := d.conn.QueryRow(ctx, rollbackQuery, snapshot).
		Scan(&origin, &slot, &hashBytes, &number, &isByronEBB, &variants)
	if err == nil {
		if variants != 1 {
			return publication.Point{}, errors.New("committed rollback header has conflicting targets")
		}
		if origin {
			return publication.Point{Origin: true}, nil
		}
		if slot == nil || number == nil {
			return publication.Point{}, errors.New("committed rollback target is incomplete")
		}
		hash, err := hash32(hashBytes)
		if err != nil {
			return publication.Point{}, err
		}
		return publication.Point{
			Slot:        *slot,
			Hash:        hash,
			BlockNumber: *number,
			IsByronEBB:  isByronEBB,
		}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return publication.Point{}, fmt.Errorf("read rollback commit tip: %w", err)
	}
	const adoptionQuery = `
SELECT
    any(publication_id),
    any(slot),
    any(block_hash),
    any(block_number),
    any(is_byron_ebb),
    uniqExact(tuple(publication_id, slot, block_hash, block_number, is_byron_ebb))
FROM clicksync.chain_events
WHERE event_seq = ?
  AND event_kind = 'adoption'
HAVING count() > 0`
	var publicationID, blockNumber uint64
	if err := d.conn.QueryRow(ctx, adoptionQuery, snapshot).
		Scan(
			&publicationID,
			&slot,
			&hashBytes,
			&blockNumber,
			&isByronEBB,
			&variants,
		); err != nil {
		return publication.Point{}, fmt.Errorf("read adoption commit tip: %w", err)
	}
	if variants != 1 || slot == nil {
		return publication.Point{}, errors.New("committed adoption header is conflicting")
	}
	var synthetic bool
	if err := d.conn.QueryRow(
		ctx,
		`SELECT synthetic FROM clicksync.blocks WHERE publication_id = ? LIMIT 1`,
		publicationID,
	).Scan(&synthetic); err != nil {
		return publication.Point{}, fmt.Errorf("read adopted block type: %w", err)
	}
	if synthetic {
		return d.manifestStartPoint(ctx)
	}
	hash, err := hash32(hashBytes)
	if err != nil {
		return publication.Point{}, err
	}
	return publication.Point{
		Slot:        *slot,
		Hash:        hash,
		BlockNumber: blockNumber,
		IsByronEBB:  isByronEBB,
	}, nil
}

func (d *DB) manifestStartPoint(ctx context.Context) (publication.Point, error) {
	const query = `
SELECT start_kind, start_slot, start_hash, start_block_number, start_is_byron_ebb
FROM clicksync.dataset_manifest
ORDER BY revision DESC
LIMIT 1`
	var kind string
	var slot, number *uint64
	var hashBytes []byte
	var isByronEBB bool
	if err := d.conn.QueryRow(ctx, query).Scan(
		&kind,
		&slot,
		&hashBytes,
		&number,
		&isByronEBB,
	); err != nil {
		return publication.Point{}, fmt.Errorf("read manifest start point: %w", err)
	}
	if kind == "origin" {
		if slot != nil || len(hashBytes) != 0 || number != nil || isByronEBB {
			return publication.Point{}, errors.New("Origin manifest has a non-null start point")
		}
		return publication.Point{Origin: true}, nil
	}
	if kind != "intersection" || slot == nil || number == nil {
		return publication.Point{}, errors.New("intersection manifest start point is incomplete")
	}
	hash, err := hash32(hashBytes)
	if err != nil {
		return publication.Point{}, err
	}
	return publication.Point{
		Slot:        *slot,
		Hash:        hash,
		BlockNumber: *number,
		IsByronEBB:  isByronEBB,
	}, nil
}

func equalHashBytes(hash model.Hash32, value []byte) bool {
	if len(value) != len(hash) {
		return false
	}
	for index := range hash {
		if hash[index] != value[index] {
			return false
		}
	}
	return true
}
