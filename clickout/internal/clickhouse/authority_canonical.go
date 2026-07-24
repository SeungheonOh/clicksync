package clickhouse

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

func normalizeAuthorityTime(value time.Time) time.Time {
	return value.UTC().Truncate(time.Microsecond)
}

func normalizeOptionalAuthorityTime(value **time.Time) {
	if *value == nil {
		return
	}
	normalized := normalizeAuthorityTime(**value)
	*value = &normalized
}

func normalizeAuthorityTimes(row *authorityRecord) {
	row.CreatedAt = normalizeAuthorityTime(row.CreatedAt)
	row.UpdatedAt = normalizeAuthorityTime(row.UpdatedAt)
	normalizeOptionalAuthorityTime(&row.CheckStartedAt)
	normalizeOptionalAuthorityTime(&row.CheckCompletedAt)
	normalizeOptionalAuthorityTime(&row.LastAgreedAt)
	if row.PendingEvidenceWrite != nil {
		row.PendingEvidenceWrite.ReservedAt = normalizeAuthorityTime(
			row.PendingEvidenceWrite.ReservedAt,
		)
		row.PendingEvidenceWrite.Observation.ObservedAt = normalizeAuthorityTime(
			row.PendingEvidenceWrite.Observation.ObservedAt,
		)
	}
	if row.PendingRollback != nil {
		row.PendingRollback.StartedAt = normalizeAuthorityTime(
			row.PendingRollback.StartedAt,
		)
	}
}

func canonicalAuthorityObservationPayload(
	observation authorityObservation,
) (string, error) {
	observation.ObservedAt = normalizeAuthorityTime(observation.ObservedAt)
	encoded, err := json.Marshal(observation)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func finalizeAuthorityObservationIdentity(row *authorityObservation) error {
	if row == nil {
		return errors.New("nil peer observation")
	}
	row.ObservedAt = normalizeAuthorityTime(row.ObservedAt)
	seed := *row
	seed.ID = [16]byte{}
	seed.EvidenceIdentity = authorityHash{}
	seed.EvidenceOrdinal = 0
	encoded, err := json.Marshal(seed)
	if err != nil {
		return err
	}
	row.EvidenceIdentity = sha256.Sum256(append(
		[]byte("clicksync-peer-evidence\x00"),
		encoded...,
	))
	idHash := sha256.Sum256(append(
		[]byte("clicksync-peer-observation\x00"),
		row.EvidenceIdentity[:]...,
	))
	copy(row.ID[:], idHash[:16])
	row.ID[6] = (row.ID[6] & 0x0f) | 0x50
	row.ID[8] = (row.ID[8] & 0x3f) | 0x80
	return nil
}

func verifyAuthorityObservation(
	row authorityObservation,
	digest authorityHash,
) error {
	expectedID := row.ID
	expectedEvidence := row.EvidenceIdentity
	recomputed := row
	if err := finalizeAuthorityObservationIdentity(&recomputed); err != nil {
		return err
	}
	if recomputed.ID != expectedID || recomputed.EvidenceIdentity != expectedEvidence {
		return errors.New("peer observation canonical identity mismatch")
	}
	row.ObservedAt = normalizeAuthorityTime(row.ObservedAt)
	encoded, err := json.Marshal(row)
	if err != nil {
		return err
	}
	if sha256.Sum256(encoded) != digest {
		return errors.New("peer observation canonical digest mismatch")
	}
	return nil
}

func decodeAuthorityObservation(payload string) (authorityObservation, error) {
	decoder := json.NewDecoder(strings.NewReader(payload))
	decoder.DisallowUnknownFields()
	var observation authorityObservation
	if err := decoder.Decode(&observation); err != nil {
		return authorityObservation{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return authorityObservation{}, errors.New("pending evidence payload has trailing JSON")
	}
	canonical, err := canonicalAuthorityObservationPayload(observation)
	if err != nil {
		return authorityObservation{}, err
	}
	if canonical != payload {
		return authorityObservation{}, errors.New("pending evidence payload is not canonical JSON")
	}
	return observation, nil
}

func canonicalAuthorityPayload(row authorityRecord) ([]byte, error) {
	encoded, err := json.Marshal(row)
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		return nil, err
	}
	delete(fields, "RowDigest")
	return json.Marshal(fields)
}

func finalizeAuthorityRecord(row *authorityRecord) error {
	row.ManifestKey = 1
	normalizeAuthorityTimes(row)
	transitionSeed := *row
	transitionSeed.TransitionID = [16]byte{}
	payload, err := canonicalAuthorityPayload(transitionSeed)
	if err != nil {
		return err
	}
	transitionHash := sha256.Sum256(append(
		[]byte("clicksync-manifest-transition\x00"),
		payload...,
	))
	copy(row.TransitionID[:], transitionHash[:16])
	row.TransitionID[6] = (row.TransitionID[6] & 0x0f) | 0x50
	row.TransitionID[8] = (row.TransitionID[8] & 0x3f) | 0x80
	payload, err = canonicalAuthorityPayload(*row)
	if err != nil {
		return err
	}
	row.RowDigest = sha256.Sum256(payload)
	return nil
}
