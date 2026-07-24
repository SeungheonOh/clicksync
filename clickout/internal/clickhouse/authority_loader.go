package clickhouse

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/clicksync-project/clickout/internal/model"
)

const (
	manifestPhysicalReplayLimit = 8
	manifestDuplicateLimit      = manifestPhysicalReplayLimit
	manifestRawReadLimit        = manifestPhysicalReplayLimit + 1

	manifestDiscoverySQL = `
SELECT revision, row_digest
FROM dataset_manifest
PREWHERE manifest_key = 1
ORDER BY revision DESC, row_digest DESC
LIMIT 9`

	manifestRevisionSQL = `
SELECT *
FROM dataset_manifest
PREWHERE manifest_key = 1
  AND revision = ?
ORDER BY manifest_key, revision, row_digest, transition_id
LIMIT 9`
)

type authorityManifestDiscoveryRow struct {
	Revision  uint64 `ch:"revision"`
	RowDigest string `ch:"row_digest"`
}

// authorityHeadObservation is deliberately comparable even when the selected
// rows are corrupt. The fingerprints cover the bounded discovery read and the
// independently exact-keyed latest/predecessor physical groups before any row
// is decoded or trusted.
type authorityHeadObservation struct {
	Present                bool
	Revision               uint64
	HasPredecessor         bool
	DiscoveryFingerprint   authorityHash
	LatestFingerprint      authorityHash
	PredecessorFingerprint authorityHash
}

type authorityHeadAttempt struct {
	Observation   authorityHeadObservation
	Latest        authorityRecord
	Predecessor   *authorityRecord
	Found         bool
	ValidationErr error
}

type authorityHeadAttemptReader func(
	context.Context,
) (authorityHeadAttempt, error)

var ErrSnapshotUnavailable = errors.New("snapshot authority is unavailable")

type SnapshotUnavailableError struct {
	Reason      string
	TrustStatus string
	TrustBasis  string
	TrustReason string
	Physical    model.Head
	Effective   model.Head
}

func (err *SnapshotUnavailableError) Error() string {
	if err == nil || err.Reason == "" {
		return ErrSnapshotUnavailable.Error()
	}
	return fmt.Sprintf("%s: %s", ErrSnapshotUnavailable, err.Reason)
}

func (err *SnapshotUnavailableError) Is(target error) bool {
	return target == ErrSnapshotUnavailable
}

func newSnapshotUnavailableError(
	reason string,
	record *authorityRecord,
) *SnapshotUnavailableError {
	result := &SnapshotUnavailableError{Reason: reason}
	if record == nil {
		return result
	}
	result.TrustStatus = record.TrustStatus
	result.TrustBasis = record.TrustBasis
	result.TrustReason = record.TrustReason
	result.Physical = modelAuthorityHead(record.Physical)
	result.Effective = modelAuthorityHead(record.Effective)
	return result
}

func invalidAuthorityError(err error) error {
	if err == nil || errors.Is(err, ErrInvalidDataset) {
		return err
	}
	return fmt.Errorf("%w: %v", ErrInvalidDataset, err)
}

func encodeAuthorityRawObservationRow[T any](
	domain string,
	index int,
	row T,
) ([]byte, error) {
	var value bytes.Buffer
	if err := gob.NewEncoder(&value).Encode(row); err != nil {
		return nil, fmt.Errorf(
			"encode %s row %d for stability observation: %w",
			domain,
			index,
			err,
		)
	}
	return value.Bytes(), nil
}

func sameAuthorityIdentity(left, right authorityRecord) bool {
	return left.DatasetID == right.DatasetID &&
		left.SchemaContractHash == right.SchemaContractHash &&
		left.NetworkMagic == right.NetworkMagic &&
		left.NetworkName == right.NetworkName &&
		left.ByronGenesisID == right.ByronGenesisID &&
		left.ByronGenesisJSONHash == right.ByronGenesisJSONHash &&
		left.ShelleyGenesisID == right.ShelleyGenesisID &&
		left.ShelleyGenesisJSONHash == right.ShelleyGenesisJSONHash &&
		left.Start == right.Start &&
		left.TrustMode == right.TrustMode &&
		left.CreatedAt.Equal(right.CreatedAt)
}

func fingerprintAuthorityRawGroup[T any](
	domain string,
	rows []T,
) (authorityHash, error) {
	encoded := make([][]byte, 0, len(rows))
	for index := range rows {
		value, err := encodeAuthorityRawObservationRow(
			domain,
			index,
			rows[index],
		)
		if err != nil {
			return authorityHash{}, err
		}
		encoded = append(encoded, value)
	}
	sort.Slice(encoded, func(left, right int) bool {
		return bytes.Compare(encoded[left], encoded[right]) < 0
	})

	digest := sha256.New()
	_, _ = digest.Write([]byte("clickout-authority-head-observation\x00"))
	_, _ = digest.Write([]byte(domain))
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], uint64(len(encoded)))
	_, _ = digest.Write(number[:])
	for _, row := range encoded {
		binary.BigEndian.PutUint64(number[:], uint64(len(row)))
		_, _ = digest.Write(number[:])
		_, _ = digest.Write(row)
	}
	var result authorityHash
	copy(result[:], digest.Sum(nil))
	return result, nil
}

func makeAuthorityHeadObservation(
	discovery []authorityManifestDiscoveryRow,
	latest []authorityDBRow,
	predecessor []authorityDBRow,
) (authorityHeadObservation, error) {
	discoveryFingerprint, err := fingerprintAuthorityRawGroup(
		"discovery",
		discovery,
	)
	if err != nil {
		return authorityHeadObservation{}, err
	}
	observation := authorityHeadObservation{
		DiscoveryFingerprint: discoveryFingerprint,
	}
	if len(discovery) == 0 {
		return observation, nil
	}

	observation.Present = true
	observation.Revision = discovery[0].Revision
	for _, row := range discovery {
		if row.Revision > observation.Revision {
			observation.Revision = row.Revision
		}
	}
	observation.HasPredecessor = observation.Revision > 1
	observation.LatestFingerprint, err = fingerprintAuthorityRawGroup(
		"latest",
		latest,
	)
	if err != nil {
		return authorityHeadObservation{}, err
	}
	if observation.HasPredecessor {
		observation.PredecessorFingerprint, err =
			fingerprintAuthorityRawGroup("predecessor", predecessor)
		if err != nil {
			return authorityHeadObservation{}, err
		}
	}
	return observation, nil
}

func validateAuthorityRevisionRecords(
	records []authorityRecord,
	revision uint64,
	name string,
) (authorityRecord, error) {
	if len(records) == 0 {
		return authorityRecord{}, fmt.Errorf("%s manifest revision is missing", name)
	}
	if len(records) >= manifestRawReadLimit {
		return authorityRecord{}, fmt.Errorf(
			"%s manifest revision has at least nine physical rows",
			name,
		)
	}
	var (
		first        authorityRecord
		firstEncoded []byte
	)
	for index, record := range records {
		if record.Revision != revision {
			return authorityRecord{}, fmt.Errorf(
				"%s manifest row differs from exact revision %d",
				name,
				revision,
			)
		}
		if err := verifyAuthorityRecord(record); err != nil {
			return authorityRecord{}, fmt.Errorf(
				"invalid %s manifest revision: %w",
				name,
				err,
			)
		}
		encoded, err := json.Marshal(record)
		if err != nil {
			return authorityRecord{}, err
		}
		if index == 0 {
			first = record
			firstEncoded = encoded
			continue
		}
		if !bytes.Equal(encoded, firstEncoded) {
			return authorityRecord{}, fmt.Errorf(
				"%s manifest physical rows conflict",
				name,
			)
		}
	}
	return first, nil
}

func validateAuthorityRawRevisionReplays(
	rows []authorityDBRow,
	name string,
) error {
	if len(rows) < 2 {
		return nil
	}
	first, err := encodeAuthorityRawObservationRow(name, 0, rows[0])
	if err != nil {
		return err
	}
	for index := 1; index < len(rows); index++ {
		encoded, err := encodeAuthorityRawObservationRow(
			name,
			index,
			rows[index],
		)
		if err != nil {
			return err
		}
		if !bytes.Equal(first, encoded) {
			return fmt.Errorf("%s manifest physical rows conflict", name)
		}
	}
	return nil
}

func decodeAuthorityRevisionGroup(
	rows []authorityDBRow,
	revision uint64,
	name string,
) (authorityRecord, error) {
	if len(rows) >= manifestRawReadLimit {
		return authorityRecord{}, fmt.Errorf(
			"%s manifest revision has at least nine physical rows",
			name,
		)
	}
	if err := validateAuthorityRawRevisionReplays(rows, name); err != nil {
		return authorityRecord{}, err
	}
	records := make([]authorityRecord, 0, len(rows))
	for index := range rows {
		record, err := rows[index].record()
		if err != nil {
			return authorityRecord{}, fmt.Errorf(
				"decode %s manifest row %d: %w",
				name,
				index,
				err,
			)
		}
		records = append(records, record)
	}
	return validateAuthorityRevisionRecords(records, revision, name)
}

func validateAuthorityPredecessor(
	latest authorityRecord,
	predecessor *authorityRecord,
) error {
	if latest.Revision == 1 {
		if predecessor != nil {
			return errors.New("revision one has an unexpected predecessor")
		}
		return nil
	}
	if predecessor == nil {
		return errors.New("manifest predecessor is missing")
	}
	if predecessor.Revision != latest.Revision-1 ||
		latest.PreviousRowDigest == nil ||
		predecessor.RowDigest != *latest.PreviousRowDigest ||
		!sameAuthorityIdentity(latest, *predecessor) {
		return errors.New(
			"manifest predecessor revision/digest/immutable identity mismatch",
		)
	}
	return nil
}

func validateAuthorityDiscoveryRows(
	discovery []authorityManifestDiscoveryRow,
	observation authorityHeadObservation,
) error {
	if !observation.Present {
		if len(discovery) != 0 {
			return errors.New(
				"absent manifest observation carries discovery rows",
			)
		}
		return nil
	}
	if len(discovery) == 0 {
		return errors.New(
			"present manifest observation has no discovery rows",
		)
	}
	for index, row := range discovery {
		if row.Revision == 0 {
			return errors.New("manifest discovery contains reserved revision zero")
		}
		if observation.Revision == 1 && row.Revision != 1 {
			return errors.New("revision one has lower manifest history")
		}
		if row.Revision > observation.Revision {
			return errors.New(
				"manifest discovery exceeds observed revision",
			)
		}
		if index > 0 {
			previous := discovery[index-1]
			if row.Revision > previous.Revision ||
				(row.Revision == previous.Revision &&
					row.RowDigest > previous.RowDigest) {
				return errors.New(
					"manifest discovery rows are not in canonical order",
				)
			}
		}
	}
	return nil
}

func validateAuthorityHeadGroups(
	discovery []authorityManifestDiscoveryRow,
	latestRows []authorityDBRow,
	predecessorRows []authorityDBRow,
	observation authorityHeadObservation,
) (authorityRecord, *authorityRecord, error) {
	if !observation.Present {
		if len(latestRows) != 0 ||
			len(predecessorRows) != 0 {
			return authorityRecord{}, nil, errors.New(
				"absent manifest discovery carries exact revision rows",
			)
		}
	}
	if err := validateAuthorityDiscoveryRows(
		discovery,
		observation,
	); err != nil {
		return authorityRecord{}, nil, err
	}
	if !observation.Present {
		return authorityRecord{}, nil, nil
	}
	if !observation.HasPredecessor && len(predecessorRows) != 0 {
		return authorityRecord{}, nil, errors.New(
			"revision one observation carries predecessor rows",
		)
	}
	latest, err := decodeAuthorityRevisionGroup(
		latestRows,
		observation.Revision,
		"latest",
	)
	if err != nil {
		return authorityRecord{}, nil, err
	}
	latestDigest := string(latest.RowDigest[:])
	for _, row := range discovery {
		if row.Revision == observation.Revision &&
			row.RowDigest != latestDigest {
			return authorityRecord{}, nil, errors.New(
				"manifest discovery differs from exact latest digest",
			)
		}
	}
	if !observation.HasPredecessor {
		return latest, nil, nil
	}

	predecessor, err := decodeAuthorityRevisionGroup(
		predecessorRows,
		observation.Revision-1,
		"predecessor",
	)
	if err != nil {
		return authorityRecord{}, nil, err
	}
	if err := validateAuthorityPredecessor(latest, &predecessor); err != nil {
		return authorityRecord{}, nil, err
	}
	predecessorDigest := string(predecessor.RowDigest[:])
	for _, row := range discovery {
		if row.Revision == observation.Revision-1 &&
			row.RowDigest != predecessorDigest {
			return authorityRecord{}, nil, errors.New(
				"manifest discovery differs from exact predecessor digest",
			)
		}
	}
	return latest, &predecessor, nil
}

func authorityManifestPhaseLimits() phaseLimits {
	return hydrationPhaseLimits(manifestRawReadLimit)
}

func (store *Store) loadAuthorityManifestDiscovery(
	ctx context.Context,
) ([]authorityManifestDiscoveryRow, error) {
	queryCtx, finish := store.instrumentPhase(
		ctx,
		"authority_manifest_discovery",
		authorityManifestPhaseLimits(),
	)
	defer finish()
	rows, err := store.conn.Query(queryCtx, manifestDiscoverySQL)
	if err != nil {
		return nil, mapQueryError("authority_manifest_discovery", err)
	}
	defer rows.Close()
	result := make([]authorityManifestDiscoveryRow, 0, manifestRawReadLimit)
	for rows.Next() {
		if len(result) >= manifestRawReadLimit {
			return nil, errors.New(
				"authority manifest discovery exceeded LIMIT 9",
			)
		}
		var row authorityManifestDiscoveryRow
		if err := rows.ScanStruct(&row); err != nil {
			return nil, fmt.Errorf("scan authority manifest discovery: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate authority manifest discovery: %w", err)
	}
	return result, nil
}

func (store *Store) loadAuthorityManifestRevision(
	ctx context.Context,
	revision uint64,
	phase string,
) ([]authorityDBRow, error) {
	queryCtx, finish := store.instrumentPhase(
		ctx,
		phase,
		authorityManifestPhaseLimits(),
	)
	defer finish()
	rows, err := store.conn.Query(queryCtx, manifestRevisionSQL, revision)
	if err != nil {
		return nil, mapQueryError(phase, err)
	}
	defer rows.Close()
	result := make([]authorityDBRow, 0, manifestRawReadLimit)
	for rows.Next() {
		if len(result) >= manifestRawReadLimit {
			return nil, fmt.Errorf("%s exceeded LIMIT 9", phase)
		}
		var row authorityDBRow
		if err := rows.ScanStruct(&row); err != nil {
			return nil, fmt.Errorf("scan %s: %w", phase, err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %s: %w", phase, err)
	}
	return result, nil
}

func (store *Store) readAuthorityHeadAttempt(
	ctx context.Context,
) (authorityHeadAttempt, error) {
	if err := ctx.Err(); err != nil {
		return authorityHeadAttempt{}, err
	}
	discovery, err := store.loadAuthorityManifestDiscovery(ctx)
	if err != nil {
		return authorityHeadAttempt{}, err
	}
	if len(discovery) == 0 {
		observation, err := makeAuthorityHeadObservation(nil, nil, nil)
		if err != nil {
			return authorityHeadAttempt{}, err
		}
		return authorityHeadAttempt{Observation: observation}, nil
	}

	revision := discovery[0].Revision
	for _, row := range discovery[1:] {
		if row.Revision > revision {
			revision = row.Revision
		}
	}
	latest, err := store.loadAuthorityManifestRevision(
		ctx,
		revision,
		"authority_manifest_latest",
	)
	if err != nil {
		return authorityHeadAttempt{}, err
	}
	var predecessor []authorityDBRow
	if revision > 1 {
		predecessor, err = store.loadAuthorityManifestRevision(
			ctx,
			revision-1,
			"authority_manifest_predecessor",
		)
		if err != nil {
			return authorityHeadAttempt{}, err
		}
	}
	observation, err := makeAuthorityHeadObservation(
		discovery,
		latest,
		predecessor,
	)
	if err != nil {
		return authorityHeadAttempt{}, err
	}
	attempt := authorityHeadAttempt{
		Observation: observation,
		Found:       true,
	}
	attempt.Latest, attempt.Predecessor, attempt.ValidationErr =
		validateAuthorityHeadGroups(
			discovery,
			latest,
			predecessor,
			observation,
		)
	attempt.ValidationErr = invalidAuthorityError(attempt.ValidationErr)
	return attempt, nil
}

func stabilizeAuthorityHead[T any](
	ctx context.Context,
	read authorityHeadAttemptReader,
	resolve func(context.Context, authorityHeadAttempt) (T, error),
) (T, error) {
	var zero T
	if read == nil || resolve == nil {
		return zero, errors.New("authority stable loader has a nil callback")
	}
	for {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		first, err := read(ctx)
		if err != nil {
			return zero, err
		}
		var value T
		retained := first.ValidationErr
		if retained == nil {
			value, retained = resolve(ctx, first)
		}

		second, err := read(ctx)
		if err != nil {
			return zero, err
		}
		if first.Observation != second.Observation {
			continue
		}
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		if retained != nil {
			return zero, retained
		}
		return value, nil
	}
}

func (store *Store) loadAuthorityHead(
	ctx context.Context,
) (authorityRecord, bool, error) {
	type result struct {
		Record authorityRecord
		Found  bool
	}
	value, err := stabilizeAuthorityHead(
		ctx,
		store.readAuthorityHeadAttempt,
		func(
			_ context.Context,
			attempt authorityHeadAttempt,
		) (result, error) {
			if !attempt.Found {
				return result{}, newSnapshotUnavailableError(
					"dataset manifest is absent",
					nil,
				)
			}
			return result{Record: attempt.Latest, Found: attempt.Found}, nil
		},
	)
	if err != nil {
		return authorityRecord{}, false, err
	}
	return value.Record, value.Found, nil
}
