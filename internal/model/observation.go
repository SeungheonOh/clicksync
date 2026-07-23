package model

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

func FinalizePeerObservationIdentity(row *PeerObservation) error {
	if row == nil {
		return errors.New("nil peer observation")
	}
	row.ObservedAt = row.ObservedAt.UTC().Truncate(time.Microsecond)
	identityInput := *row
	identityInput.ID = [16]byte{}
	identityInput.EvidenceIdentity = Hash32{}
	// The durable admission ordinal orders the authoritative evidence set but
	// is assigned by the sole writer after observation construction. It must
	// not alter the logical observation/evidence identity used for retries.
	identityInput.EvidenceOrdinal = 0
	encoded, err := json.Marshal(identityInput)
	if err != nil {
		return fmt.Errorf("encode peer evidence identity: %w", err)
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

func PeerObservationDigest(row PeerObservation) (Hash32, error) {
	row.ObservedAt = row.ObservedAt.UTC().Truncate(time.Microsecond)
	encoded, err := json.Marshal(row)
	if err != nil {
		return Hash32{}, fmt.Errorf("encode peer observation digest: %w", err)
	}
	return sha256.Sum256(encoded), nil
}

func VerifyPeerObservationIdentity(row PeerObservation, digest []byte) error {
	if len(digest) != len(Hash32{}) {
		return fmt.Errorf("peer observation digest has %d bytes", len(digest))
	}
	expectedID := row.ID
	expectedEvidence := row.EvidenceIdentity
	recomputed := row
	if err := FinalizePeerObservationIdentity(&recomputed); err != nil {
		return err
	}
	if recomputed.ID != expectedID {
		return errors.New("peer observation ID differs from canonical persisted fields")
	}
	if recomputed.EvidenceIdentity != expectedEvidence {
		return errors.New("peer evidence identity differs from canonical persisted fields")
	}
	recomputedDigest, err := PeerObservationDigest(row)
	if err != nil {
		return err
	}
	if string(recomputedDigest[:]) != string(digest) {
		return errors.New("peer observation digest differs from canonical persisted fields")
	}
	return nil
}
