package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const insertAdoptionsSQL = `INSERT INTO clicksync.chain_events
(
    event_seq, publication_id, event_kind, active, rollback_id, block_hash,
    slot, block_number, is_byron_ebb, writer_id, recorded_at
)`

const adoptionReadbackSQL = `
SELECT
    event_seq, publication_id, event_kind, active, rollback_id, block_hash,
    slot, block_number, is_byron_ebb, writer_id, recorded_at
FROM clicksync.chain_events
WHERE event_seq BETWEEN ? AND ?
ORDER BY event_seq, event_kind, publication_id`

const uncertaintyReadbackTimeout = 15 * time.Second

func (d *DB) Publish(
	ctx context.Context,
	lock Lock,
	candidates []Candidate,
) (Commit, error) {
	if len(candidates) == 0 {
		return Commit{}, errors.New("cannot publish an empty candidate batch")
	}
	if lock == nil {
		return Commit{}, errors.New("publication requires the writer lock")
	}
	if err := validateCandidates(candidates); err != nil {
		return Commit{}, err
	}
	d.operationMu.Lock()
	defer d.operationMu.Unlock()
	if err := lock.AssertHeld(); err != nil {
		return Commit{}, fmt.Errorf("publication writer lock is not held: %w", err)
	}
	identity, allocator, err := d.initializedStore()
	if err != nil {
		return Commit{}, err
	}

	count := uint64(len(candidates))
	firstPublication, err := allocator.reservePublications(count)
	if err != nil {
		return Commit{}, err
	}
	firstEvent, err := allocator.reserveEvents(count)
	if err != nil {
		// The publication range remains burned.
		return Commit{
			FirstPublicationID: firstPublication,
			LastPublicationID:  firstPublication + count - 1,
		}, err
	}
	insertedAt := d.now()
	publications := make([]publication, len(candidates))
	commit := Commit{
		FirstPublicationID: firstPublication,
		LastPublicationID:  firstPublication + count - 1,
		FirstEventSeq:      firstEvent,
		LastEventSeq:       firstEvent + count - 1,
		Blocks:             make([]CanonicalBlock, len(candidates)),
	}
	for index, candidate := range candidates {
		publicationID := firstPublication + uint64(index)
		eventSeq := firstEvent + uint64(index)
		publications[index] = publication{
			PublicationID: publicationID,
			EventSeq:      eventSeq,
			Block:         candidate.Block,
			Counts:        countFacts(candidate.Block),
			ContentHash:   candidate.ContentHash,
			Relays:        append([]Relay(nil), candidate.Relays...),
			WriterID:      d.writerID,
			InsertedAt:    insertedAt,
		}
		commit.Blocks[index] = CanonicalBlock{
			PublicationID: publicationID,
			EventSeq:      eventSeq,
			Point:         pointForBlock(candidate.Block),
		}
	}

	if err := d.insertFactsConcurrently(ctx, identity, publications); err != nil {
		return commit, err
	}
	if err := lock.AssertHeld(); err != nil {
		return commit, fmt.Errorf(
			"publication writer lock was lost before adoption: %w",
			err,
		)
	}
	insertErr := d.insertAdoptions(ctx, publications)
	if insertErr == nil {
		commit.Committed = true
		return commit, nil
	}

	readbackCtx, cancel := uncertaintyContext(ctx)
	defer cancel()
	committed, readbackErr := d.adoptionsCommitted(readbackCtx, publications)
	switch {
	case readbackErr != nil:
		if errors.Is(readbackErr, ErrCommitConflict) {
			return commit, errors.Join(insertErr, readbackErr)
		}
		return commit, errors.Join(
			ErrCommitIndeterminate,
			fmt.Errorf("adoption insert: %w", insertErr),
			fmt.Errorf("adoption exact readback: %w", readbackErr),
		)
	case committed:
		commit.Committed = true
		commit.ResolvedUncertain = true
		return commit, nil
	default:
		return commit, errors.Join(
			ErrNotCommitted,
			fmt.Errorf("adoption insert: %w", insertErr),
		)
	}
}

func (d *DB) initializedStore() (DatasetIdentity, *Allocator, error) {
	if d == nil {
		return DatasetIdentity{}, nil, errorsNewNilDB()
	}
	d.initializeMu.Lock()
	defer d.initializeMu.Unlock()
	if d.identity == nil || d.allocator == nil {
		return DatasetIdentity{}, nil, errors.New("store is not initialized")
	}
	return *d.identity, d.allocator, nil
}

func (d *DB) insertFactsConcurrently(
	ctx context.Context,
	identity DatasetIdentity,
	publications []publication,
) error {
	type factJob struct {
		table  string
		insert func(context.Context, []publication) error
	}
	jobs := []factJob{
		{table: "blocks", insert: func(ctx context.Context, publications []publication) error {
			return d.insertBlocks(ctx, identity.NetworkMagic, publications)
		}},
		{table: "transactions", insert: d.insertTransactions},
		{table: "inputs", insert: d.insertInputs},
		{table: "outputs", insert: d.insertOutputs},
		{table: "datum_bodies", insert: d.insertDatumBodies},
		{table: "datum_observations", insert: d.insertDatumObservations},
		{table: "withdrawals", insert: d.insertWithdrawals},
		{table: "redeemers", insert: d.insertRedeemers},
		{table: "transaction_metadata", insert: d.insertMetadata},
	}
	jobCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	errs := make(chan error, len(jobs))
	var wait sync.WaitGroup
	wait.Add(len(jobs))
	for _, job := range jobs {
		job := job
		go func() {
			defer wait.Done()
			if err := job.insert(jobCtx, publications); err != nil {
				wrapped := fmt.Errorf("insert %s facts: %w", job.table, err)
				errs <- wrapped
				cancel(wrapped)
			}
		}()
	}
	wait.Wait()
	close(errs)
	var joined error
	for err := range errs {
		joined = errors.Join(joined, err)
	}
	return joined
}

func (d *DB) insertAdoptions(
	ctx context.Context,
	publications []publication,
) error {
	batch, err := d.conn.PrepareBatch(ctx, insertAdoptionsSQL)
	if err != nil {
		return err
	}
	for _, item := range publications {
		point := pointForBlock(item.Block)
		if err := batch.Append(
			item.EventSeq,
			item.PublicationID,
			"adoption",
			true,
			nil,
			bytes32(point.Hash),
			point.Slot,
			point.BlockNumber,
			point.IsByronEBB,
			item.WriterID,
			item.InsertedAt,
		); err != nil {
			_ = batch.Abort()
			return err
		}
	}
	return batch.Send()
}

func (d *DB) adoptionsCommitted(
	ctx context.Context,
	publications []publication,
) (bool, error) {
	if len(publications) == 0 {
		return false, errors.New("cannot read back an empty adoption batch")
	}
	firstEvent := publications[0].EventSeq
	lastEvent := publications[len(publications)-1].EventSeq
	result, err := d.conn.Query(
		ctx,
		adoptionReadbackSQL,
		firstEvent,
		lastEvent,
	)
	if err != nil {
		return false, err
	}
	defer result.Close()
	expected := make(map[uint64]publication, len(publications))
	for _, item := range publications {
		expected[item.EventSeq] = item
	}
	seen := make(map[uint64]struct{}, len(publications))
	for result.Next() {
		var (
			eventSeq      uint64
			publicationID uint64
			eventKind     string
			active        bool
			rollbackID    *uuid.UUID
			hashBytes     []byte
			slot          uint64
			blockNumber   uint64
			isByronEBB    bool
			writerID      uuid.UUID
			recordedAt    time.Time
		)
		if err := result.Scan(
			&eventSeq,
			&publicationID,
			&eventKind,
			&active,
			&rollbackID,
			&hashBytes,
			&slot,
			&blockNumber,
			&isByronEBB,
			&writerID,
			&recordedAt,
		); err != nil {
			return false, err
		}
		item, ok := expected[eventSeq]
		if !ok {
			return false, fmt.Errorf(
				"%w: adoption readback returned out-of-range event %d",
				ErrCommitConflict,
				eventSeq,
			)
		}
		point := pointForBlock(item.Block)
		if _, duplicate := seen[eventSeq]; duplicate ||
			publicationID != item.PublicationID ||
			eventKind != "adoption" ||
			!active ||
			rollbackID != nil ||
			!bytes.Equal(hashBytes, point.Hash[:]) ||
			slot != point.Slot ||
			blockNumber != point.BlockNumber ||
			isByronEBB != point.IsByronEBB ||
			writerID != item.WriterID ||
			!recordedAt.Equal(item.InsertedAt) {
			return false, fmt.Errorf(
				"%w: adoption event %d differs from expected row",
				ErrCommitConflict,
				eventSeq,
			)
		}
		seen[eventSeq] = struct{}{}
	}
	if err := result.Err(); err != nil {
		return false, err
	}
	if len(seen) == 0 {
		return false, nil
	}
	if len(seen) != len(expected) {
		return false, fmt.Errorf(
			"%w: adoption range is partially committed (%d/%d)",
			ErrCommitConflict,
			len(seen),
			len(expected),
		)
	}
	return true, nil
}

func validateCandidates(candidates []Candidate) error {
	for index, candidate := range candidates {
		if candidate.Block.Hash == ([32]byte{}) {
			return fmt.Errorf("candidate %d block hash is zero", index)
		}
		if len(candidate.Relays) < 2 {
			return fmt.Errorf("candidate %d has fewer than two agreeing relays", index)
		}
		for relayIndex, relay := range candidate.Relays {
			if strings.TrimSpace(relay.Host) == "" ||
				strings.TrimSpace(relay.Address) == "" ||
				strings.TrimSpace(relay.Operator) == "" {
				return fmt.Errorf(
					"candidate %d relay %d identity is incomplete",
					index,
					relayIndex,
				)
			}
		}
		counts := countFacts(candidate.Block)
		for name, count := range map[string]uint64{
			"transactions":       counts.transactions,
			"inputs":             counts.inputs,
			"outputs":            counts.outputs,
			"datum observations": counts.datumObservations,
			"withdrawals":        counts.withdrawals,
			"redeemers":          counts.redeemers,
			"metadata":           counts.metadata,
		} {
			if count > math.MaxUint32 {
				return fmt.Errorf(
					"candidate %d %s count exceeds UInt32",
					index,
					name,
				)
			}
		}
		for _, datum := range candidate.Block.Datums {
			if uint64(len(datum.CBOR)) > math.MaxUint32 {
				return fmt.Errorf("candidate %d datum body exceeds UInt32", index)
			}
		}
		for _, transaction := range candidate.Block.Transactions {
			for _, redeemer := range transaction.Redeemers {
				if uint64(len(redeemer.DataCBOR)) > math.MaxUint32 {
					return fmt.Errorf("candidate %d redeemer data exceeds UInt32", index)
				}
			}
			if transaction.Metadata != nil &&
				uint64(len(transaction.Metadata.CBOR)) > math.MaxUint32 {
				return fmt.Errorf("candidate %d metadata exceeds UInt32", index)
			}
		}
	}
	return nil
}

func uncertaintyContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), uncertaintyReadbackTimeout)
}
