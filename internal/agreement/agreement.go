package agreement

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cardano-clicksync/internal/model"
	"cardano-clicksync/internal/relay"
)

type Source interface {
	Next(context.Context) (relay.Event, error)
}

type Engine struct {
	sources []Source
	events  []relay.Event
}

func New(sources ...Source) (*Engine, error) {
	if len(sources) < 2 {
		return nil, errors.New("strict agreement requires at least two relay sources")
	}
	for index, source := range sources {
		if source == nil {
			return nil, fmt.Errorf("relay source %d is nil", index)
		}
	}
	return &Engine{
		sources: append([]Source(nil), sources...),
		events:  make([]relay.Event, len(sources)),
	}, nil
}

// Next consumes exactly one event from every source. It is an ordered,
// single-consumer API. Any error or difference makes the ordinal unusable;
// callers restart the whole relay set.
func (e *Engine) Next(ctx context.Context) (model.AgreedEvent, error) {
	if ctx == nil {
		return model.AgreedEvent{}, errors.New("agreement context is required")
	}
	for index, source := range e.sources {
		event, err := source.Next(ctx)
		if err != nil {
			clear(e.events)
			return model.AgreedEvent{}, &SourceError{
				RelayIndex: index,
				Err:        err,
			}
		}
		e.events[index] = event
	}
	agreed, err := compare(e.events)
	clear(e.events)
	return agreed, err
}

func compare(events []relay.Event) (model.AgreedEvent, error) {
	first := events[0]
	if first.Kind != relay.Forward && first.Kind != relay.Rollback {
		return model.AgreedEvent{}, newDifference(DifferenceEventKind, events)
	}
	for _, event := range events[1:] {
		if event.Kind != first.Kind {
			return model.AgreedEvent{}, newDifference(DifferenceEventKind, events)
		}
	}
	for _, event := range events[1:] {
		if event.Point != first.Point {
			return model.AgreedEvent{}, newDifference(DifferencePoint, events)
		}
	}

	if first.Kind == relay.Rollback {
		for _, event := range events {
			if len(event.RawCBOR) != 0 {
				return model.AgreedEvent{}, newDifference(DifferenceRawPolicy, events)
			}
		}
		return agreedRollback(events), nil
	}

	for _, event := range events[1:] {
		if event.BlockType != first.BlockType {
			return model.AgreedEvent{}, newDifference(DifferenceBlockType, events)
		}
	}
	for _, event := range events[1:] {
		if event.RawLength != first.RawLength {
			return model.AgreedEvent{}, newDifference(DifferenceRawLength, events)
		}
	}
	for _, event := range events[1:] {
		if event.Digest != first.Digest {
			return model.AgreedEvent{}, newDifference(DifferenceDigest, events)
		}
	}
	if uint64(len(first.RawCBOR)) != first.RawLength {
		return model.AgreedEvent{}, newDifference(DifferenceRawPolicy, events)
	}
	for _, event := range events[1:] {
		if len(event.RawCBOR) != 0 {
			return model.AgreedEvent{}, newDifference(DifferenceRawPolicy, events)
		}
	}
	return agreedForward(events), nil
}

func agreedForward(events []relay.Event) model.AgreedEvent {
	first := events[0]
	return model.AgreedEvent{
		Kind:        model.EventForward,
		Point:       first.Point,
		BlockType:   first.BlockType,
		ContentHash: first.Digest,
		RawLength:   first.RawLength,
		RawCBOR:     first.RawCBOR,
		Relays:      relayIdentities(events),
		ObservedAt:  latestObservation(events),
	}
}

func agreedRollback(events []relay.Event) model.AgreedEvent {
	return model.AgreedEvent{
		Kind:       model.EventRollback,
		Point:      events[0].Point,
		Relays:     relayIdentities(events),
		ObservedAt: latestObservation(events),
	}
}

func relayIdentities(events []relay.Event) []model.RelayIdentity {
	ret := make([]model.RelayIdentity, len(events))
	for index, event := range events {
		ret[index] = event.Relay
	}
	return ret
}

func latestObservation(events []relay.Event) time.Time {
	var latest time.Time
	for _, event := range events {
		if event.ObservedAt.After(latest) {
			latest = event.ObservedAt
		}
	}
	return latest
}
