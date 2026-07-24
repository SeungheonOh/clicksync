package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
)

const publicationHighWaterSQL = `
SELECT greatest(
    (SELECT max(publication_id) FROM clicksync.blocks),
    (SELECT max(publication_id) FROM clicksync.transactions),
    (SELECT max(publication_id) FROM clicksync.inputs),
    (SELECT max(publication_id) FROM clicksync.outputs),
    (SELECT max(publication_id) FROM clicksync.datum_bodies),
    (SELECT max(publication_id) FROM clicksync.datum_observations),
    (SELECT max(publication_id) FROM clicksync.withdrawals),
    (SELECT max(publication_id) FROM clicksync.redeemers),
    (SELECT max(publication_id) FROM clicksync.transaction_metadata),
    (SELECT max(publication_id) FROM clicksync.chain_events)
)`

const eventHighWaterSQL = `
SELECT greatest(
    (SELECT max(event_seq) FROM clicksync.chain_events),
    (SELECT max(event_seq) FROM clicksync.rollbacks)
)`

type Allocator struct {
	mu              sync.Mutex
	nextPublication uint64
	nextEvent       uint64
}

func (d *DB) loadAllocator(ctx context.Context) (*Allocator, error) {
	var publicationHighWater uint64
	if err := d.conn.QueryRow(ctx, publicationHighWaterSQL).Scan(&publicationHighWater); err != nil {
		return nil, fmt.Errorf("read raw publication high-water: %w", err)
	}
	var eventHighWater uint64
	if err := d.conn.QueryRow(ctx, eventHighWaterSQL).Scan(&eventHighWater); err != nil {
		return nil, fmt.Errorf("read raw event high-water: %w", err)
	}
	return newAllocator(publicationHighWater, eventHighWater)
}

func newAllocator(publicationHighWater, eventHighWater uint64) (*Allocator, error) {
	if publicationHighWater == math.MaxUint64 {
		return nil, errors.New("publication identity space exhausted")
	}
	if eventHighWater == math.MaxUint64 {
		return nil, errors.New("event identity space exhausted")
	}
	return &Allocator{
		nextPublication: publicationHighWater + 1,
		nextEvent:       eventHighWater + 1,
	}, nil
}

func (a *Allocator) reservePublications(count uint64) (uint64, error) {
	if a == nil {
		return 0, errors.New("nil allocator")
	}
	if count == 0 {
		return 0, errors.New("cannot reserve zero publication identities")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	first, next, err := reserveRange(a.nextPublication, count, "publication")
	if err != nil {
		return 0, err
	}
	a.nextPublication = next
	return first, nil
}

func (a *Allocator) reserveEvents(count uint64) (uint64, error) {
	if a == nil {
		return 0, errors.New("nil allocator")
	}
	if count == 0 {
		return 0, errors.New("cannot reserve zero event identities")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	first, next, err := reserveRange(a.nextEvent, count, "event")
	if err != nil {
		return 0, err
	}
	a.nextEvent = next
	return first, nil
}

func reserveRange(next, count uint64, kind string) (uint64, uint64, error) {
	if next == 0 || count-1 > math.MaxUint64-next {
		return 0, next, fmt.Errorf("%s identity space exhausted", kind)
	}
	first := next
	if count-1 == math.MaxUint64-next {
		next = 0
	} else {
		next += count
	}
	return first, next, nil
}
