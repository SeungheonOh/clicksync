package agreement

import (
	"fmt"

	"cardano-clicksync/internal/model"
	"cardano-clicksync/internal/relay"
)

const (
	MaxDiagnostics  = 16
	MaxIdentityText = 128
)

type DifferenceField string

const (
	DifferenceEventKind DifferenceField = "event_kind"
	DifferencePoint     DifferenceField = "point"
	DifferenceBlockType DifferenceField = "block_type"
	DifferenceRawLength DifferenceField = "raw_length"
	DifferenceDigest    DifferenceField = "digest"
	DifferenceRawPolicy DifferenceField = "raw_retention"
)

// Diagnostic is deliberately body-free and bounded. It contains only the
// exact fields used by strict agreement.
type Diagnostic struct {
	RelayIndex int
	Relay      model.RelayIdentity
	Kind       relay.EventKind
	Point      model.Point
	BlockType  uint
	RawLength  uint64
	Digest     model.Hash32
}

type Difference struct {
	Field       DifferenceField
	Diagnostics []Diagnostic
	TotalRelays int
	Truncated   bool
}

func (e *Difference) Error() string {
	if e == nil {
		return "relay agreement difference"
	}
	return fmt.Sprintf(
		"relay agreement differs on %s (%d relays)",
		e.Field,
		e.TotalRelays,
	)
}

type SourceError struct {
	RelayIndex int
	Err        error
}

func (e *SourceError) Error() string {
	if e == nil {
		return "relay source failed"
	}
	return fmt.Sprintf("relay source %d failed: %v", e.RelayIndex, e.Err)
}

func (e *SourceError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func newDifference(field DifferenceField, events []relay.Event) *Difference {
	count := min(len(events), MaxDiagnostics)
	diagnostics := make([]Diagnostic, count)
	for index := range count {
		event := events[index]
		diagnostics[index] = Diagnostic{
			RelayIndex: index,
			Relay:      boundedIdentity(event.Relay),
			Kind:       event.Kind,
			Point:      event.Point,
			BlockType:  event.BlockType,
			RawLength:  event.RawLength,
			Digest:     event.Digest,
		}
	}
	return &Difference{
		Field:       field,
		Diagnostics: diagnostics,
		TotalRelays: len(events),
		Truncated:   len(events) > count,
	}
}

func boundedIdentity(identity model.RelayIdentity) model.RelayIdentity {
	identity.Host = boundedText(identity.Host)
	identity.Address = boundedText(identity.Address)
	identity.Operator = boundedText(identity.Operator)
	return identity
}

func boundedText(value string) string {
	if len(value) <= MaxIdentityText {
		return value
	}
	return value[:MaxIdentityText]
}
