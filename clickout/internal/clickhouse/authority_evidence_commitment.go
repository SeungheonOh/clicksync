package clickhouse

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
)

const authorityEvidenceMaximumLogicalRows = uint32(65535)

// PrefixDigests is indexed by logical row count: element zero commits to the
// empty set and the final element is Digest.
type authorityEvidenceCommitment struct {
	Rows          []authorityObservationRow
	Count         uint32
	Digest        authorityHash
	PrefixDigests []authorityHash
}

func canonicalAuthorityEvidenceCommitment(
	rows []authorityObservationRow,
	group [16]byte,
	attempt uint32,
) (authorityEvidenceCommitment, error) {
	if group == ([16]byte{}) || attempt == 0 {
		return authorityEvidenceCommitment{}, errors.New(
			"authority evidence expectation is incomplete",
		)
	}
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("clicksync-trust-evidence-set\x00"))
	empty := authorityHash(sha256.Sum256(
		[]byte("clicksync-trust-evidence-set\x00"),
	))
	result := authorityEvidenceCommitment{
		Rows:          make([]authorityObservationRow, 0),
		PrefixDigests: []authorityHash{empty},
	}
	ids := make(map[[16]byte]uint32)
	identities := make(map[authorityHash]uint32)
	var (
		lastOrdinal uint32
		replays     uint8
	)
	for _, row := range rows {
		observation := row.Observation
		if err := verifyAuthorityObservation(observation, row.Digest); err != nil {
			return authorityEvidenceCommitment{}, fmt.Errorf(
				"invalid canonical authority evidence row: %w",
				err,
			)
		}
		if observation.AgreementGroup != group ||
			observation.CheckAttempt != attempt {
			return authorityEvidenceCommitment{}, errors.New(
				"authority evidence row differs from exact group/attempt",
			)
		}
		ordinal := observation.EvidenceOrdinal
		if ordinal == lastOrdinal {
			if len(result.Rows) == 0 {
				return authorityEvidenceCommitment{}, errors.New(
					"authority evidence starts at ordinal zero",
				)
			}
			previous := result.Rows[len(result.Rows)-1]
			if observation.ID != previous.Observation.ID ||
				observation.EvidenceIdentity !=
					previous.Observation.EvidenceIdentity ||
				row.Digest != previous.Digest {
				return authorityEvidenceCommitment{}, errors.New(
					"one authority evidence ordinal has conflicting physical rows",
				)
			}
			if replays == manifestDuplicateLimit {
				return authorityEvidenceCommitment{}, errors.New(
					"authority evidence ordinal has at least nine physical rows",
				)
			}
			replays++
			continue
		}
		if ordinal != lastOrdinal+1 {
			return authorityEvidenceCommitment{}, fmt.Errorf(
				"authority evidence ordinal gap: got %d after %d",
				ordinal,
				lastOrdinal,
			)
		}
		if ordinal > authorityEvidenceMaximumLogicalRows {
			return authorityEvidenceCommitment{}, errors.New(
				"authority evidence exceeds UInt16 logical cardinality",
			)
		}
		if previous, exists := ids[observation.ID]; exists {
			return authorityEvidenceCommitment{}, fmt.Errorf(
				"authority observation ID is assigned to ordinals %d and %d",
				previous,
				ordinal,
			)
		}
		if previous, exists := identities[observation.EvidenceIdentity]; exists {
			return authorityEvidenceCommitment{}, fmt.Errorf(
				"authority evidence identity is assigned to ordinals %d and %d",
				previous,
				ordinal,
			)
		}
		ids[observation.ID] = ordinal
		identities[observation.EvidenceIdentity] = ordinal
		lastOrdinal = ordinal
		replays = 1
		result.Rows = append(result.Rows, row)

		var encodedOrdinal [4]byte
		binary.BigEndian.PutUint32(encodedOrdinal[:], ordinal)
		_, _ = hasher.Write(encodedOrdinal[:])
		_, _ = hasher.Write(observation.ID[:])
		_, _ = hasher.Write(row.Digest[:])
		var prefix authorityHash
		copy(prefix[:], hasher.Sum(nil))
		result.PrefixDigests = append(result.PrefixDigests, prefix)
	}
	result.Count = lastOrdinal
	result.Digest = result.PrefixDigests[len(result.PrefixDigests)-1]
	return result, nil
}
