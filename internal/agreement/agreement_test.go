package agreement

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"cardano-clicksync/internal/model"
	"cardano-clicksync/internal/relay"
)

type sourceResult struct {
	event relay.Event
	err   error
}

type channelSource struct {
	results <-chan sourceResult
}

func (s channelSource) Next(ctx context.Context) (relay.Event, error) {
	select {
	case <-ctx.Done():
		return relay.Event{}, context.Cause(ctx)
	case result, ok := <-s.results:
		if !ok {
			return relay.Event{}, io.EOF
		}
		return result.event, result.err
	}
}

func TestIdenticalForwardAgreement(t *testing.T) {
	for _, count := range []int{2, 3} {
		t.Run(fmt.Sprintf("%d-of-%d", count, count), func(t *testing.T) {
			raw := []byte{0x82, 0x01, 0xa0}
			events := make([]relay.Event, count)
			for index := range events {
				events[index] = forwardEvent(index, raw)
				events[index].ObservedAt = time.Unix(int64(index+1), 0)
			}
			engine := engineForEvents(t, events)
			got, err := engine.Next(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if got.Kind != model.EventForward ||
				got.Point != events[0].Point ||
				got.BlockType != events[0].BlockType ||
				got.ContentHash != events[0].Digest ||
				got.RawLength != uint64(len(raw)) ||
				string(got.RawCBOR) != string(raw) {
				t.Fatalf("agreed event = %+v", got)
			}
			if len(got.Relays) != count {
				t.Fatalf("relay count = %d, want %d", len(got.Relays), count)
			}
			for index, identity := range got.Relays {
				if identity.Operator != fmt.Sprintf("operator-%d", index) {
					t.Fatalf("relay %d attribution = %+v", index, identity)
				}
			}
			if !got.ObservedAt.Equal(events[count-1].ObservedAt) {
				t.Fatalf("observation = %v, want latest %v", got.ObservedAt, events[count-1].ObservedAt)
			}
		})
	}
}

func TestForwardDifferences(t *testing.T) {
	tests := map[string]struct {
		field  DifferenceField
		mutate func(*relay.Event)
	}{
		"event-kind": {
			field: DifferenceEventKind,
			mutate: func(event *relay.Event) {
				event.Kind = relay.Rollback
				event.RawCBOR = nil
			},
		},
		"point": {
			field: DifferencePoint,
			mutate: func(event *relay.Event) {
				event.Point.Slot++
			},
		},
		"block-type": {
			field: DifferenceBlockType,
			mutate: func(event *relay.Event) {
				event.BlockType++
			},
		},
		"raw-length": {
			field: DifferenceRawLength,
			mutate: func(event *relay.Event) {
				event.RawLength++
			},
		},
		"digest": {
			field: DifferenceDigest,
			mutate: func(event *relay.Event) {
				event.Digest[0] ^= 0xff
			},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			raw := []byte{1, 2, 3}
			events := []relay.Event{
				forwardEvent(0, raw),
				forwardEvent(1, raw),
			}
			test.mutate(&events[1])
			engine := engineForEvents(t, events)
			got, err := engine.Next(context.Background())
			if err == nil {
				t.Fatalf("mismatch produced output: %+v", got)
			}
			var difference *Difference
			if !errors.As(err, &difference) || difference.Field != test.field {
				t.Fatalf("error = %#v, want difference %s", err, test.field)
			}
			if !reflect.DeepEqual(got, model.AgreedEvent{}) {
				t.Fatalf("mismatch returned partial output: %+v", got)
			}
		})
	}
}

func TestIdenticalRollbackAgreement(t *testing.T) {
	target := point(90, 9)
	events := []relay.Event{
		rollbackEvent(0, target),
		rollbackEvent(1, target),
		rollbackEvent(2, target),
	}
	got, err := engineForEvents(t, events).Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != model.EventRollback ||
		got.Point != target ||
		got.RawCBOR != nil ||
		got.RawLength != 0 ||
		len(got.Relays) != 3 {
		t.Fatalf("rollback agreement = %+v", got)
	}
}

func TestRollbackTargetDifference(t *testing.T) {
	events := []relay.Event{
		rollbackEvent(0, point(90, 9)),
		rollbackEvent(1, point(89, 8)),
	}
	_, err := engineForEvents(t, events).Next(context.Background())
	var difference *Difference
	if !errors.As(err, &difference) || difference.Field != DifferencePoint {
		t.Fatalf("error = %#v, want point difference", err)
	}
}

func TestClosedAndErrorSource(t *testing.T) {
	want := errors.New("relay disconnected")
	tests := []struct {
		name   string
		source Source
		index  int
		want   error
	}{
		{
			name:   "closed",
			source: channelSource{results: closedResults()},
			index:  1,
			want:   io.EOF,
		},
		{
			name:   "error",
			source: sourceWith(sourceResult{err: want}),
			index:  1,
			want:   want,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			engine, err := New(
				sourceWith(sourceResult{event: rollbackEvent(0, point(1, 1))}),
				test.source,
			)
			if err != nil {
				t.Fatal(err)
			}
			_, err = engine.Next(context.Background())
			var sourceErr *SourceError
			if !errors.As(err, &sourceErr) ||
				sourceErr.RelayIndex != test.index ||
				!errors.Is(err, test.want) {
				t.Fatalf("source error = %#v", err)
			}
		})
	}
}

func TestNoOutputBeforeLastRelayArrives(t *testing.T) {
	raw := []byte{1}
	first := make(chan sourceResult, 1)
	last := make(chan sourceResult)
	first <- sourceResult{event: forwardEvent(0, raw)}
	engine, err := New(
		channelSource{results: first},
		channelSource{results: last},
	)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := engine.Next(context.Background())
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("agreement returned before the last relay: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	last <- sourceResult{event: forwardEvent(1, raw)}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("agreement did not return after the last relay")
	}
}

func TestSlowRelayBackpressuresFastRelay(t *testing.T) {
	slow := make(chan sourceResult)
	fast := make(chan sourceResult, 1)
	engine, err := New(
		channelSource{results: slow},
		channelSource{results: fast},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	engineDone := make(chan error, 1)
	go func() {
		_, err := engine.Next(ctx)
		engineDone <- err
	}()
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		for index := 0; index < 2; index++ {
			select {
			case fast <- sourceResult{
				event: rollbackEvent(1, point(uint64(index+1), byte(index+1))),
			}:
			case <-ctx.Done():
				return
			}
		}
	}()
	waitUntil(t, func() bool { return len(fast) == cap(fast) })
	select {
	case <-producerDone:
		t.Fatal("fast relay advanced beyond its bounded queue")
	default:
	}
	cancel()
	select {
	case <-producerDone:
	case <-time.After(time.Second):
		t.Fatal("fast producer remained blocked after cancellation")
	}
	select {
	case err := <-engineDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("agreement cancellation = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("agreement remained blocked after cancellation")
	}
}

func TestRawRetentionPolicy(t *testing.T) {
	raw := []byte{1, 2, 3}
	tests := []struct {
		name   string
		mutate func([]relay.Event)
	}{
		{
			name: "source-length",
			mutate: func(events []relay.Event) {
				events[0].RawCBOR = events[0].RawCBOR[:2]
			},
		},
		{
			name: "follower-retained-body",
			mutate: func(events []relay.Event) {
				events[1].RawCBOR = []byte("must-not-survive")
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			events := []relay.Event{
				forwardEvent(0, raw),
				forwardEvent(1, raw),
			}
			test.mutate(events)
			_, err := engineForEvents(t, events).Next(context.Background())
			var difference *Difference
			if !errors.As(err, &difference) ||
				difference.Field != DifferenceRawPolicy {
				t.Fatalf("error = %#v, want raw policy difference", err)
			}
		})
	}
}

func TestDiagnosticsAreBodyFreeAndBounded(t *testing.T) {
	const secret = "raw-secret-that-must-not-appear"
	events := make([]relay.Event, MaxDiagnostics+5)
	sources := make([]Source, len(events))
	for index := range events {
		events[index] = forwardEvent(index, []byte(secret))
		events[index].Relay.Host = strings.Repeat("h", MaxIdentityText+100)
		sources[index] = sourceWith(sourceResult{event: events[index]})
	}
	events[len(events)-1].Digest[0] ^= 1
	sources[len(sources)-1] = sourceWith(sourceResult{
		event: events[len(events)-1],
	})
	engine, err := New(sources...)
	if err != nil {
		t.Fatal(err)
	}
	_, err = engine.Next(context.Background())
	var difference *Difference
	if !errors.As(err, &difference) {
		t.Fatalf("error = %#v, want difference", err)
	}
	if len(difference.Diagnostics) != MaxDiagnostics ||
		!difference.Truncated ||
		difference.TotalRelays != len(events) {
		t.Fatalf("unbounded diagnostics: %+v", difference)
	}
	for _, diagnostic := range difference.Diagnostics {
		if len(diagnostic.Relay.Host) > MaxIdentityText {
			t.Fatalf("identity was not bounded: %d", len(diagnostic.Relay.Host))
		}
	}
	if strings.Contains(fmt.Sprintf("%+v", difference), secret) {
		t.Fatal("raw body leaked into agreement diagnostics")
	}
}

func TestNewRequiresEveryConfiguredRelay(t *testing.T) {
	if _, err := New(sourceWith()); err == nil {
		t.Fatal("one source was accepted")
	}
}

func FuzzCompareRejectsForwardFieldDifference(f *testing.F) {
	f.Add(byte(0))
	f.Add(byte(1))
	f.Add(byte(2))
	f.Add(byte(3))
	f.Add(byte(4))
	f.Fuzz(func(t *testing.T, selector byte) {
		raw := []byte{1, 2, 3}
		events := []relay.Event{
			forwardEvent(0, raw),
			forwardEvent(1, raw),
		}
		switch selector % 5 {
		case 0:
			events[1].Kind = relay.Rollback
			events[1].RawCBOR = nil
		case 1:
			events[1].Point.Hash[0] ^= 1
		case 2:
			events[1].BlockType++
		case 3:
			events[1].RawLength++
		case 4:
			events[1].Digest[0] ^= 1
		}
		got, err := compare(events)
		if err == nil || !reflect.DeepEqual(got, model.AgreedEvent{}) {
			t.Fatalf(
				"non-identical compared fields agreed: selector=%d output=%+v",
				selector,
				got,
			)
		}
	})
}

func engineForEvents(t *testing.T, events []relay.Event) *Engine {
	t.Helper()
	sources := make([]Source, len(events))
	for index, event := range events {
		sources[index] = sourceWith(sourceResult{event: event})
	}
	engine, err := New(sources...)
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

func sourceWith(results ...sourceResult) Source {
	channel := make(chan sourceResult, len(results))
	for _, result := range results {
		channel <- result
	}
	close(channel)
	return channelSource{results: channel}
}

func closedResults() <-chan sourceResult {
	channel := make(chan sourceResult)
	close(channel)
	return channel
}

func forwardEvent(index int, raw []byte) relay.Event {
	event := relay.Event{
		Kind:       relay.Forward,
		Point:      point(100, 1),
		BlockType:  7,
		RawLength:  uint64(len(raw)),
		Digest:     relay.RawBlockDigest(7, raw),
		Relay:      identity(index),
		ObservedAt: time.Unix(1, 0),
	}
	if index == 0 {
		event.RawCBOR = append([]byte(nil), raw...)
	}
	return event
}

func rollbackEvent(index int, target model.Point) relay.Event {
	return relay.Event{
		Kind:       relay.Rollback,
		Point:      target,
		Relay:      identity(index),
		ObservedAt: time.Unix(1, 0),
	}
}

func identity(index int) model.RelayIdentity {
	return model.RelayIdentity{
		Host:         fmt.Sprintf("relay-%d.example:3001", index),
		Address:      fmt.Sprintf("192.0.2.%d:3001", index+1),
		Operator:     fmt.Sprintf("operator-%d", index),
		N2NVersion:   13,
		NetworkMagic: 764824073,
	}
}

func point(slot uint64, marker byte) model.Point {
	var hash model.Hash32
	hash[0] = marker
	return model.Point{Slot: slot, Hash: hash, BlockNumber: slot * 10}
}

func waitUntil(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition was not reached")
		}
		time.Sleep(time.Millisecond)
	}
}
