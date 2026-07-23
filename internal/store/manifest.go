package store

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"clicksync/internal/model"
	"clicksync/internal/publication"
	"clicksync/migrations"
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
	SchemaContractHash     model.Hash32
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
	lock LockAssertion,
	seed ManifestSeed,
) error {
	if lock == nil {
		return errors.New("manifest initialization requires the real writer flock")
	}
	d.manifestMu.Lock()
	defer d.manifestMu.Unlock()

	if err := lock.AssertHeld(); err != nil {
		return fmt.Errorf("manifest initialization flock is not held: %w", err)
	}
	if seed.DatasetID == ([16]byte{}) || seed.WriterID == ([16]byte{}) {
		return errors.New("manifest dataset and writer IDs must be non-zero")
	}
	if seed.NetworkMagic == 0 || seed.NetworkName == "" {
		return errors.New("manifest network identity is required")
	}
	if err := validatePinnedMainnet(seed); err != nil {
		return err
	}
	_, found, err := d.loadLatestManifestRecord(ctx)
	if err != nil {
		return err
	}
	if found {
		return d.validateManifestIdentity(ctx, seed)
	}
	createdAt := seed.CreatedAt.UTC()
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	trustStatus := "unavailable"
	trustBasis := "partial_boundary"
	servable := false
	trustReason := "partial-history boundary awaits persisted bootstrap agreement"
	if seed.Start.Origin {
		trustBasis = "official_genesis"
		trustReason = "official genesis has not been seeded and verified"
	}
	writerID := seed.WriterID
	initial := manifestRecord{
		ManifestKey:            manifestKey,
		Revision:               1,
		TransitionKind:         "initialize",
		DatasetID:              seed.DatasetID,
		SchemaContractHash:     migrations.ContractHash,
		NetworkMagic:           seed.NetworkMagic,
		NetworkName:            seed.NetworkName,
		ByronGenesisID:         seed.ByronGenesisID,
		ByronGenesisJSONHash:   seed.ByronGenesisJSONHash,
		ShelleyGenesisID:       seed.ShelleyGenesisID,
		ShelleyGenesisJSONHash: seed.ShelleyGenesisJSONHash,
		Start:                  seed.Start,
		TrustMode:              "peer_observed_structurally_verified",
		TrustStatus:            trustStatus,
		TrustBasis:             trustBasis,
		CheckpointInterval:     manifestCheckpointBlocks,
		TrustReason:            trustReason,
		ServableFloor:          manifestHead{Point: seed.Start},
		ServableFloorPermanent: false,
		Physical:               manifestHead{Point: seed.Start},
		Effective:              manifestHead{Point: seed.Start},
		Servable:               servable,
		VisibilityGeneration:   1,
		WriterID:               &writerID,
		WriterBuild:            seed.WriterBuild,
		SourceBuild:            seed.SourceBuild,
		CreatedAt:              createdAt,
		UpdatedAt:              createdAt,
	}
	if err := lock.AssertHeld(); err != nil {
		return fmt.Errorf("manifest initialization flock was lost before append: %w", err)
	}
	return d.appendManifestRecord(ctx, initial)
}

// LoadOrCreateManifest is the startup contract for the sole writer. It
// generates dataset_id only while the held flock proves the manifest is truly
// empty, reuses the stored identity on restart, and repairs conservative
// physical-head lag from raw adoption/rollback markers before returning.
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
		if err := d.InitializeManifest(ctx, lock, seed); err != nil {
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
		lock,
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
	record, found, err := d.loadLatestManifestRecord(ctx)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("dataset manifest is not initialized")
	}
	if record.DatasetID != seed.DatasetID ||
		record.SchemaContractHash != migrations.ContractHash ||
		record.NetworkMagic != seed.NetworkMagic ||
		record.NetworkName != seed.NetworkName ||
		record.ByronGenesisID != seed.ByronGenesisID ||
		record.ByronGenesisJSONHash != seed.ByronGenesisJSONHash ||
		record.ShelleyGenesisID != seed.ShelleyGenesisID ||
		record.ShelleyGenesisJSONHash != seed.ShelleyGenesisJSONHash ||
		record.Start != seed.Start {
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
	record, found, err := d.loadLatestManifestRecord(ctx)
	if err != nil || !found {
		return ManifestIdentity{}, found, err
	}
	if record.SchemaContractHash != migrations.ContractHash {
		return ManifestIdentity{}, false, errors.New(
			"dataset manifest schema contract hash differs from this binary",
		)
	}
	return manifestIdentityFromRecord(record), true, nil
}

func manifestIdentityFromRecord(record manifestRecord) ManifestIdentity {
	return ManifestIdentity{
		DatasetID:              record.DatasetID,
		SchemaContractHash:     record.SchemaContractHash,
		NetworkMagic:           record.NetworkMagic,
		NetworkName:            record.NetworkName,
		ByronGenesisID:         record.ByronGenesisID,
		ByronGenesisJSONHash:   record.ByronGenesisJSONHash,
		ShelleyGenesisID:       record.ShelleyGenesisID,
		ShelleyGenesisJSONHash: record.ShelleyGenesisJSONHash,
		Start:                  record.Start,
		GenesisSeeded:          record.GenesisSeeded,
		CompleteHistory:        record.CompleteHistory,
	}
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
	snapshot, err := d.RawCommittedSnapshot(ctx)
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
	snapshot, err := d.RawCommittedSnapshot(ctx)
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
	if err := d.PersistManifest(ctx, lock, publication.ManifestUpdate{
		EventSeq:    snapshot,
		Tip:         publication.Point{Origin: true},
		Kind:        publication.ManifestGenesis,
		WriterID:    writerID,
		WriterBuild: writerBuild,
		UpdatedAt:   now.UTC(),
	}); err != nil {
		return err
	}
	if err := lock.AssertHeld(); err != nil {
		return fmt.Errorf("writer flock was lost before genesis completion marker: %w", err)
	}
	return d.transitionManifest(
		ctx,
		lock,
		"genesis_complete",
		now,
		func(latest manifestRecord) (bool, error) {
			if !latest.GenesisSeeded {
				return false, nil
			}
			if !latest.CompleteHistory ||
				latest.TrustStatus != "agreed" ||
				latest.TrustBasis != "official_genesis" ||
				!latest.Servable ||
				!latest.ServableFloorPermanent ||
				!latest.ServableFloor.Point.Origin ||
				latest.ServableFloor.EventSeq != 0 ||
				latest.Effective != latest.Physical {
				return false, errors.New(
					"genesis-seeded manifest carries conflicting trust/visibility state",
				)
			}
			return true, nil
		},
		func(next *manifestRecord) error {
			if !next.Start.Origin || !next.Physical.Point.Origin {
				return errors.New("official genesis completion requires an Origin manifest head")
			}
			next.GenesisSeeded = true
			next.CompleteHistory = true
			next.TrustStatus = "agreed"
			next.TrustBasis = "official_genesis"
			next.CheckID = nil
			next.AgreementGroup = nil
			next.CheckAttempt = 0
			next.CorroborationRequired = 0
			next.CorroborationConfirmed = 0
			next.Disagreement = false
			next.TrustReason = "official genesis distribution verified exactly"
			next.CheckStartedAt = nil
			next.CheckCompletedAt = nil
			next.Checked = nil
			agreed := next.Physical
			next.LastAgreed = &agreed
			agreedAt := manifestTime(now)
			next.LastAgreedAt = &agreedAt
			next.ServableFloor = manifestHead{Point: publication.Point{Origin: true}}
			next.ServableFloorPermanent = true
			if next.Effective != next.Physical || !next.Servable {
				next.VisibilityGeneration++
			}
			next.Effective = next.Physical
			next.Servable = true
			next.PrimarySuffix = 0
			writer := writerID
			next.WriterID = &writer
			next.WriterBuild = writerBuild
			return nil
		},
	)
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

func (d *DB) PersistManifest(
	ctx context.Context,
	authority publication.Lock,
	update publication.ManifestUpdate,
) error {
	switch update.Kind {
	case publication.ManifestAdoption:
		if update.RemoteAdoptions == 0 {
			return errors.New("remote adoption manifest update has zero adopted blocks")
		}
	case publication.ManifestRollback, publication.ManifestGenesis:
		if update.RemoteAdoptions != 0 {
			return fmt.Errorf("%s manifest update carries remote adoption count", update.Kind)
		}
	case publication.ManifestReconcile:
		if update.RemoteAdoptions != 0 {
			return errors.New("reconcile manifest update must derive remote adoptions from physical facts")
		}
	default:
		return fmt.Errorf("unknown manifest physical update kind %q", update.Kind)
	}
	committed, err := d.RawCommittedSnapshot(ctx)
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
	return d.transitionManifest(
		ctx,
		authority,
		"physical_"+string(update.Kind),
		update.UpdatedAt,
		func(latest manifestRecord) (bool, error) {
			if update.EventSeq < latest.Physical.EventSeq {
				return false, fmt.Errorf(
					"physical manifest event regressed from %d to %d",
					latest.Physical.EventSeq,
					update.EventSeq,
				)
			}
			if update.EventSeq == latest.Physical.EventSeq {
				if update.Tip != latest.Physical.Point {
					return false, errors.New(
						"same physical manifest event has a conflicting point",
					)
				}
				// The latest authoritative trust state is the intended
				// carry-forward. This is the lost-response/restart retry.
				return true, nil
			}
			if update.Kind == publication.ManifestReconcile {
				if err := validateManifestReconcileAdvance(latest, update); err != nil {
					return false, err
				}
			}
			return false, nil
		},
		func(next *manifestRecord) error {
			if next.PendingRollback != nil {
				return errors.New("ordinary physical-head append cannot bypass a pending rollback")
			}
			previous := next.Physical
			remoteAdoptions := update.RemoteAdoptions
			if update.Kind == publication.ManifestReconcile {
				remoteAdoptions, err = d.remoteAdoptionsBetween(
					ctx,
					previous.EventSeq,
					update.EventSeq,
				)
				if err != nil {
					return err
				}
			}
			visibilityDiscontinuity := update.Kind == publication.ManifestRollback
			if update.Kind == publication.ManifestReconcile {
				visibilityDiscontinuity, err = d.rawEventIsRollback(ctx, update.EventSeq)
				if err != nil {
					return err
				}
			}
			if err := applyPhysicalManifestUpdate(
				next,
				update,
				remoteAdoptions,
				visibilityDiscontinuity,
			); err != nil {
				return err
			}
			return nil
		},
	)
}

func validateManifestReconcileAdvance(
	latest manifestRecord,
	update publication.ManifestUpdate,
) error {
	if latest.PendingRollback != nil {
		if update.EventSeq == latest.PendingRollback.EventSeq &&
			update.Tip == latest.PendingRollback.To {
			return errors.New(
				"pending rollback reached its reserved raw event; specialized recovery is required",
			)
		}
		return errors.New("raw event does not match the pending rollback reservation")
	}
	if latest.TrustStatus == "checking" || latest.TrustStatus == "disputed" {
		return fmt.Errorf(
			"unexpected raw event %d advanced past a %s manifest barrier at %d",
			update.EventSeq,
			latest.TrustStatus,
			latest.Physical.EventSeq,
		)
	}
	return nil
}

func applyPhysicalManifestUpdate(
	next *manifestRecord,
	update publication.ManifestUpdate,
	remoteAdoptions uint64,
	visibilityDiscontinuity bool,
) error {
	previous := next.Physical
	next.Physical = manifestHead{EventSeq: update.EventSeq, Point: update.Tip}

	rollback := manifestPointBefore(update.Tip, previous.Point)
	if update.Kind == publication.ManifestRollback &&
		manifestPointBefore(previous.Point, update.Tip) {
		return errors.New("rollback manifest update moves the physical point forward")
	}
	if update.Kind == publication.ManifestAdoption && rollback {
		return errors.New("adoption manifest update moves the physical point backward")
	}
	if remoteAdoptions > manifestMaximumSuffix ||
		next.PrimarySuffix > manifestMaximumSuffix-remoteAdoptions {
		return fmt.Errorf(
			"physical suffix would exceed %d without a sampled check",
			manifestMaximumSuffix,
		)
	}
	next.PrimarySuffix += remoteAdoptions

	switch next.TrustStatus {
	case "agreed":
		next.Effective = next.Physical
		next.Servable = true
	case "unavailable":
		if next.Servable || next.LastAgreed != nil {
			next.Effective = next.Physical
			next.Servable = true
		}
	case "checking", "disputed":
		if manifestPointBefore(next.Physical.Point, next.Effective.Point) {
			next.Effective = next.Physical
		}
	default:
		return fmt.Errorf("unknown manifest trust status %q", next.TrustStatus)
	}
	if visibilityDiscontinuity {
		next.VisibilityGeneration++
	}
	writerID := update.WriterID
	next.WriterID = &writerID
	next.WriterBuild = update.WriterBuild
	return nil
}

func (d *DB) rawEventIsRollback(ctx context.Context, eventSeq uint64) (bool, error) {
	var count uint64
	if err := d.conn.QueryRow(
		ctx,
		`SELECT count() FROM clicksync.rollbacks WHERE event_seq = ?`,
		eventSeq,
	).Scan(&count); err != nil {
		return false, fmt.Errorf("classify raw manifest reconciliation event: %w", err)
	}
	if count > 1 {
		return false, errors.New("raw rollback event has duplicate physical headers")
	}
	return count == 1, nil
}

func manifestPointBefore(left, right publication.Point) bool {
	if left.Origin {
		return !right.Origin
	}
	if right.Origin {
		return false
	}
	if left.BlockNumber != right.BlockNumber {
		return left.BlockNumber < right.BlockNumber
	}
	return left.Slot < right.Slot
}

func (d *DB) remoteAdoptionsBetween(
	ctx context.Context,
	afterEvent uint64,
	throughEvent uint64,
) (uint64, error) {
	if throughEvent <= afterEvent {
		return 0, nil
	}
	const query = `
SELECT uniqExact(tuple(events.event_seq, events.publication_id))
FROM clicksync.chain_events AS events
INNER JOIN clicksync.blocks AS blocks
    ON blocks.publication_id = events.publication_id
WHERE events.event_kind = 'adoption'
  AND events.event_seq > ?
  AND events.event_seq <= ?
  AND NOT blocks.synthetic`
	var count uint64
	if err := d.conn.QueryRow(ctx, query, afterEvent, throughEvent).Scan(&count); err != nil {
		return 0, fmt.Errorf("count remote adoptions for manifest reconciliation: %w", err)
	}
	return count, nil
}

func (d *DB) ReconcileManifest(
	ctx context.Context,
	authority publication.Lock,
	writerID [16]byte,
	writerBuild string,
	now time.Time,
) error {
	snapshot, err := d.RawCommittedSnapshot(ctx)
	if err != nil {
		return err
	}
	tip, err := d.committedTip(ctx, snapshot)
	if err != nil {
		return err
	}
	return d.PersistManifest(ctx, authority, publication.ManifestUpdate{
		EventSeq:    snapshot,
		Tip:         tip,
		Kind:        publication.ManifestReconcile,
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
	record, found, err := d.loadLatestManifestRecord(ctx)
	if err != nil {
		return publication.Point{}, err
	}
	if !found {
		return publication.Point{}, errors.New("dataset manifest is not initialized")
	}
	return record.Start, nil
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
