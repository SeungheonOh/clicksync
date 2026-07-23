package store

import (
	"math"
	"strings"
	"sync"
	"testing"
)

func TestPublicationHighWaterCoversEveryRawIdentityTable(t *testing.T) {
	tables := []string{
		"blocks",
		"transactions",
		"inputs",
		"outputs",
		"datum_bodies",
		"datum_observations",
		"withdrawals",
		"redeemers",
		"transaction_metadata",
		"chain_events",
	}
	for _, table := range tables {
		needle := "clicksync." + table
		if count := strings.Count(publicationHighWaterSQL, needle); count != 1 {
			t.Fatalf("publication high-water must reference %s exactly once, got %d", needle, count)
		}
	}
	if !strings.Contains(publicationHighWaterSQL, "max(first_publication_id)") {
		t.Fatal("datum body first-publication identities are not included")
	}
	for _, table := range []string{"chain_events", "rollbacks"} {
		if !strings.Contains(eventHighWaterSQL, "clicksync."+table) {
			t.Fatalf("event high-water omits clicksync.%s", table)
		}
	}
}

func TestAllocatorStartsStrictlyAboveRawHighWater(t *testing.T) {
	allocator, err := newAllocator(91, 117)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := allocator.ReservePublication()
	if err != nil {
		t.Fatal(err)
	}
	if publication != 92 {
		t.Fatalf("publication = %d, want 92", publication)
	}
	event, err := allocator.ReserveEvents(3)
	if err != nil {
		t.Fatal(err)
	}
	if event != 118 {
		t.Fatalf("first event = %d, want 118", event)
	}
	next, err := allocator.ReserveEvents(1)
	if err != nil {
		t.Fatal(err)
	}
	if next != 121 {
		t.Fatalf("next event = %d, want 121", next)
	}
}

func TestAllocatorDoesNotIssueDuplicatesConcurrently(t *testing.T) {
	allocator, err := newAllocator(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	const workers = 64
	results := make(chan uint64, workers)
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			value, reserveErr := allocator.ReservePublication()
			if reserveErr != nil {
				t.Error(reserveErr)
				return
			}
			results <- value
		}()
	}
	wait.Wait()
	close(results)
	seen := make(map[uint64]struct{}, workers)
	for value := range results {
		if _, exists := seen[value]; exists {
			t.Fatalf("duplicate publication identity %d", value)
		}
		seen[value] = struct{}{}
	}
	if len(seen) != workers {
		t.Fatalf("reserved %d identities, want %d", len(seen), workers)
	}
}

func TestAllocatorRejectsIdentityOverflow(t *testing.T) {
	if _, err := newAllocator(math.MaxUint64, 0); err == nil {
		t.Fatal("expected publication overflow failure")
	}
	if _, err := newAllocator(0, math.MaxUint64); err == nil {
		t.Fatal("expected event overflow failure")
	}
	allocator, err := newAllocator(0, math.MaxUint64-1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := allocator.ReserveEvents(2); err == nil {
		t.Fatal("expected multi-event overflow failure")
	}
	publicationAllocator, err := newAllocator(math.MaxUint64-1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := publicationAllocator.ReservePublication(); err != nil || got != math.MaxUint64 {
		t.Fatalf("reserve final publication = %d, %v", got, err)
	}
	if _, err := publicationAllocator.ReservePublication(); err == nil {
		t.Fatal("expected publication exhaustion after final identity")
	}
	eventAllocator, err := newAllocator(0, math.MaxUint64-2)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := eventAllocator.ReserveEvents(2); err != nil || got != math.MaxUint64-1 {
		t.Fatalf("reserve final event range = %d, %v", got, err)
	}
	if _, err := eventAllocator.ReserveEvents(1); err == nil {
		t.Fatal("expected event exhaustion after final identity")
	}
}
