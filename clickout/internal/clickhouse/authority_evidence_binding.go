package clickhouse

import (
	"errors"
	"fmt"
	"strings"
)

type authorityEvidenceOutcome struct {
	Confirmed    uint16
	Disagreement bool
}

func bindAuthorityEvidenceSet(
	rows []authorityObservationRow,
	checkID [16]byte,
	group [16]byte,
	attempt uint32,
	required uint16,
	checked authorityHead,
) (authorityEvidenceCommitment, authorityEvidenceOutcome, error) {
	commitment, err := canonicalAuthorityEvidenceCommitment(rows, group, attempt)
	if err != nil {
		return authorityEvidenceCommitment{}, authorityEvidenceOutcome{}, err
	}
	operators := make(map[string]string, len(commitment.Rows))
	var outcome authorityEvidenceOutcome
	for _, physical := range commitment.Rows {
		observation := physical.Observation
		if observation.CheckID != checkID ||
			observation.AgreementGroup != group ||
			observation.CheckAttempt != attempt ||
			observation.CorroborationRequired != required ||
			observation.CheckedEventSeq != checked.EventSeq ||
			authorityObservationPoint(observation) != checked.Point {
			return authorityEvidenceCommitment{}, authorityEvidenceOutcome{}, errors.New(
				"authority evidence row differs from exact check identity",
			)
		}
		if err := validateAuthorityObservationProvenance(
			observation,
			checked.Point,
		); err != nil {
			return authorityEvidenceCommitment{}, authorityEvidenceOutcome{}, err
		}
		operatorKey := physical.OperatorKey
		if operatorKey == "" ||
			operatorKey != strings.ToLower(strings.TrimSpace(observation.Operator)) {
			return authorityEvidenceCommitment{}, authorityEvidenceOutcome{}, errors.New(
				"authority evidence operator key differs from canonical label",
			)
		}
		if previous, exists := operators[operatorKey]; exists {
			if previous != observation.Result {
				return authorityEvidenceCommitment{}, authorityEvidenceOutcome{}, errors.New(
					"one normalized operator emitted multiple outcomes",
				)
			}
			return authorityEvidenceCommitment{}, authorityEvidenceOutcome{}, errors.New(
				"one normalized operator emitted multiple distinct evidence rows",
			)
		}
		operators[operatorKey] = observation.Result
		switch observation.Result {
		case "agreed":
			outcome.Confirmed++
		case "disagreed", "quarantined":
			outcome.Disagreement = true
		}
	}
	return commitment, outcome, nil
}

func bindAuthorityEvidence(
	record authorityRecord,
	currentRows []authorityObservationRow,
	lastAgreedRows []authorityObservationRow,
) (err error) {
	defer func() {
		err = invalidAuthorityError(err)
	}()
	var current authorityEvidenceCommitment
	if record.CheckID == nil {
		if len(currentRows) != 0 {
			return errors.New("manifest without a current check carries evidence rows")
		}
	} else {
		if record.AgreementGroup == nil ||
			record.Checked == nil ||
			record.EvidenceDigest == nil {
			return errors.New("current evidence binding expectation is incomplete")
		}
		var outcome authorityEvidenceOutcome
		var err error
		current, outcome, err = bindAuthorityEvidenceSet(
			currentRows,
			*record.CheckID,
			*record.AgreementGroup,
			record.CheckAttempt,
			record.CorroborationRequired,
			*record.Checked,
		)
		if err != nil {
			return fmt.Errorf("bind current authority evidence: %w", err)
		}
		if record.PendingEvidenceWrite == nil {
			if current.Count != record.EvidenceCount ||
				current.Digest != *record.EvidenceDigest {
				return errors.New(
					"current authority evidence differs from manifest commitment",
				)
			}
		} else {
			count := record.EvidenceCount
			if current.Count != count && current.Count != count+1 {
				return errors.New(
					"current authority evidence differs from pending committed prefix",
				)
			}
			if uint32(len(current.PrefixDigests)) <= count ||
				current.PrefixDigests[count] != *record.EvidenceDigest {
				return errors.New(
					"current authority evidence prefix differs from manifest commitment",
				)
			}
			pending := record.PendingEvidenceWrite
			pendingPayload, err := canonicalAuthorityObservationPayload(
				pending.Observation,
			)
			if err != nil {
				return err
			}
			if pendingPayload != pending.Payload {
				return errors.New("pending evidence reservation payload is not exact")
			}
			if current.Count == count+1 {
				physical := current.Rows[count]
				payload, err := canonicalAuthorityObservationPayload(
					physical.Observation,
				)
				if err != nil {
					return err
				}
				if physical.Digest != pending.Digest ||
					payload != pending.Payload {
					return errors.New(
						"physical pending evidence differs from exact reservation",
					)
				}
			} else {
				prospectiveRows := append(
					append([]authorityObservationRow(nil), current.Rows...),
					authorityObservationRow{
						Observation: pending.Observation,
						OperatorKey: strings.ToLower(strings.TrimSpace(
							pending.Observation.Operator,
						)),
						Digest: pending.Digest,
					},
				)
				prospective, _, err := bindAuthorityEvidenceSet(
					prospectiveRows,
					*record.CheckID,
					*record.AgreementGroup,
					record.CheckAttempt,
					record.CorroborationRequired,
					*record.Checked,
				)
				if err != nil {
					return fmt.Errorf(
						"bind prospective pending authority evidence: %w",
						err,
					)
				}
				if prospective.Count != count+1 ||
					prospective.PrefixDigests[count] != *record.EvidenceDigest {
					return errors.New(
						"pending authority evidence does not extend the exact committed prefix",
					)
				}
			}
		}
		if record.CheckCompletedAt == nil {
			if record.CorroborationConfirmed != 0 || record.Disagreement {
				return errors.New(
					"checking authority carries completed evidence outcome",
				)
			}
		} else if outcome.Confirmed != record.CorroborationConfirmed ||
			outcome.Disagreement != record.Disagreement {
			return errors.New(
				"completed current evidence outcome differs from manifest outcome",
			)
		}
	}

	if record.LastAgreedEvidence == nil {
		if len(lastAgreedRows) != 0 {
			return errors.New("manifest without last-agreed authority carries evidence rows")
		}
		return nil
	}
	reference := *record.LastAgreedEvidence
	if record.CheckID != nil && *record.CheckID == reference.CheckID {
		if record.CheckCompletedAt == nil ||
			record.EvidenceState != "frozen" ||
			record.PendingEvidenceWrite != nil {
			return errors.New(
				"immutable last-agreed evidence check is not frozen and complete",
			)
		}
	}
	rows := lastAgreedRows
	if record.CheckID != nil && *record.CheckID == reference.CheckID {
		rows = currentRows
	}
	last, outcome, err := bindAuthorityEvidenceSet(
		rows,
		reference.CheckID,
		reference.Group,
		reference.Attempt,
		reference.Required,
		reference.Checked,
	)
	if err != nil {
		return fmt.Errorf("bind last-agreed authority evidence: %w", err)
	}
	if last.Count != reference.Count ||
		last.Digest != reference.Digest ||
		outcome.Confirmed != reference.Confirmed ||
		outcome.Disagreement {
		return errors.New(
			"last-agreed authority differs from immutable evidence outcome",
		)
	}
	return nil
}
