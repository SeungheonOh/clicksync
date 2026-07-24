package store

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
)

func rollbackPoints() (Point, Point, Point) {
	return Point{Slot: 30, Hash: testHash(3), BlockNumber: 3},
		Point{Slot: 20, Hash: testHash(2), BlockNumber: 2},
		Point{Slot: 10, Hash: testHash(1), BlockNumber: 1}
}

func configureRollbackState(
	connection *fakeConnection,
	extra func(string, []any) ([][]any, error),
) {
	tip, parent, target := rollbackPoints()
	connection.queryFn = func(query string, args []any) ([][]any, error) {
		switch {
		case sameSQL(query, latestAdoptionSQL):
			return [][]any{{
				uint64(3), uint64(3), bytes32(tip.Hash), tip.Slot,
				tip.BlockNumber, false,
			}}, nil
		case sameSQL(query, latestRollbackSQL):
			return nil, nil
		case sameSQL(query, canonicalWindowSQL):
			return [][]any{
				{uint64(3), uint64(3), bytes32(tip.Hash), tip.Slot, tip.BlockNumber, false},
				{uint64(2), uint64(2), bytes32(parent.Hash), parent.Slot, parent.BlockNumber, false},
				{uint64(1), uint64(1), bytes32(target.Hash), target.Slot, target.BlockNumber, false},
			}, nil
		default:
			if extra != nil {
				return extra(query, args)
			}
			return nil, fmt.Errorf("unexpected query")
		}
	}
}

func rollbackRequest(target Point) RollbackRequest {
	return RollbackRequest{
		To: Point{
			Slot: target.Slot,
			Hash: target.Hash,
			// ChainSync rollback targets do not carry block number/EBB.
		},
		Relays: []Relay{
			{Host: "relay-a:3001", Address: "192.0.2.1:3001", Operator: "a", N2NVersion: 13},
			{Host: "relay-b:3001", Address: "192.0.2.2:3001", Operator: "b", N2NVersion: 14},
		},
		Reason:       "unanimous relay rollback",
		MaximumDepth: 3,
	}
}

func TestRollbackInvalidatesDescendantsThenWritesHeader(t *testing.T) {
	t.Parallel()
	connection := newFakeConnection()
	configureRollbackState(connection, nil)
	db := initializedTestDB(connection, 3, 3)
	_, _, target := rollbackPoints()
	commit, err := db.Rollback(
		context.Background(),
		&fakeLock{},
		rollbackRequest(target),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !commit.Committed || commit.Noop || commit.EventSeq != 4 ||
		commit.To != target || len(commit.Descendants) != 2 {
		t.Fatalf("rollback commit = %#v", commit)
	}
	connection.mu.Lock()
	order := append([]string(nil), connection.sendOrder...)
	connection.mu.Unlock()
	invalidationIndex, headerIndex := -1, -1
	for index, key := range order {
		switch key {
		case "invalidation":
			invalidationIndex = index
		case "rollbacks":
			headerIndex = index
		}
	}
	if invalidationIndex < 0 || headerIndex <= invalidationIndex {
		t.Fatalf("send order = %#v, want invalidations before header", order)
	}
	invalidations := connection.rowsFor("invalidation")
	if len(invalidations) != 2 ||
		invalidations[0][0] != uint64(4) ||
		invalidations[0][2] != "invalidation" ||
		invalidations[0][3] != false {
		t.Fatalf("invalidations = %#v", invalidations)
	}
	header := connection.rowsFor("rollbacks")
	if len(header) != 1 || header[0][12] != uint32(2) ||
		header[0][13].([]string)[0] != "relay-a:3001" {
		t.Fatalf("rollback header = %#v", header)
	}
}

func TestRollbackToDurableTipIsNoop(t *testing.T) {
	t.Parallel()
	connection := newFakeConnection()
	configureRollbackState(connection, nil)
	db := initializedTestDB(connection, 3, 3)
	tip, _, _ := rollbackPoints()
	commit, err := db.Rollback(
		context.Background(),
		&fakeLock{},
		rollbackRequest(tip),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !commit.Noop || !commit.Committed {
		t.Fatalf("tip rollback = %#v", commit)
	}
	if connection.sendCount("invalidation") != 0 ||
		connection.sendCount("rollbacks") != 0 {
		t.Fatal("durable-tip rollback wrote database rows")
	}
	first, err := db.allocator.reserveEvents(1)
	if err != nil || first != 4 {
		t.Fatalf("noop consumed event identity: %d, %v", first, err)
	}
}

func TestResolveRollbackRejectsNoncanonicalBelowStartAndTooDeep(t *testing.T) {
	t.Parallel()
	tip, parent, target := rollbackPoints()
	state := State{
		Dataset:  DatasetIdentity{Start: Point{Origin: true}},
		Snapshot: 3,
		Tip:      tip,
		Canonical: []CanonicalBlock{
			{PublicationID: 3, EventSeq: 3, Point: tip},
			{PublicationID: 2, EventSeq: 2, Point: parent},
			{PublicationID: 1, EventSeq: 1, Point: target},
		},
	}
	request := rollbackRequest(Point{Slot: 99, Hash: testHash(9)})
	if _, _, err := resolveRollback(state, request); !errors.Is(err, ErrInvalidRollback) {
		t.Fatalf("noncanonical target error = %v", err)
	}
	request.To = Point{Origin: true}
	request.MaximumDepth = 2
	if _, _, err := resolveRollback(state, request); !errors.Is(err, ErrInvalidRollback) {
		t.Fatalf("too-deep target error = %v", err)
	}
	state.Dataset.Start = target
	request.To = Point{Origin: true}
	if _, _, err := resolveRollback(state, request); !errors.Is(err, ErrInvalidRollback) {
		t.Fatalf("below-start target error = %v", err)
	}
}

func TestRollbackLockLossLeavesHeaderlessInvalidations(t *testing.T) {
	t.Parallel()
	connection := newFakeConnection()
	configureRollbackState(connection, nil)
	db := initializedTestDB(connection, 3, 3)
	_, _, target := rollbackPoints()
	_, err := db.Rollback(
		context.Background(),
		&fakeLock{failAt: 2},
		rollbackRequest(target),
	)
	if err == nil {
		t.Fatal("lock loss was ignored")
	}
	if connection.sendCount("invalidation") != 1 ||
		connection.sendCount("rollbacks") != 0 {
		t.Fatalf(
			"sends = invalidation %d, header %d",
			connection.sendCount("invalidation"),
			connection.sendCount("rollbacks"),
		)
	}
	// Recovery authority still reports the adoption tip because no header
	// exists; canonicalWindowSQL itself joins invalidations to rollbacks.
	state, stateErr := db.State(context.Background(), 3)
	if stateErr != nil {
		t.Fatal(stateErr)
	}
	tip, _, _ := rollbackPoints()
	if state.Tip != tip {
		t.Fatalf("orphan invalidations changed tip to %#v", state.Tip)
	}
}

func TestUncertainInvalidationsRequireExactCompleteness(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		readback   string
		wantError  error
		wantHeader bool
	}{
		{name: "complete", readback: "complete", wantHeader: true},
		{name: "absent", readback: "absent", wantError: ErrNotCommitted},
		{name: "partial", readback: "partial", wantError: ErrNotCommitted},
		{name: "conflicting", readback: "conflicting", wantError: ErrCommitConflict},
		{name: "query_failure", readback: "failure", wantError: ErrNotCommitted},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			connection := newFakeConnection()
			connection.sendErrors["invalidation"] = errors.New("response lost")
			configureRollbackState(
				connection,
				func(query string, _ []any) ([][]any, error) {
					if !sameSQL(query, invalidationReadbackSQL) {
						return nil, fmt.Errorf("unexpected query")
					}
					if test.readback == "failure" {
						return nil, errors.New("readback unavailable")
					}
					rows := invalidationReadbackRows(connection)
					switch test.readback {
					case "absent":
						return nil, nil
					case "partial":
						return rows[:1], nil
					case "conflicting":
						rows[0][0] = uint64(999)
						return rows, nil
					default:
						return rows, nil
					}
				},
			)
			db := initializedTestDB(connection, 3, 3)
			_, _, target := rollbackPoints()
			commit, err := db.Rollback(
				context.Background(),
				&fakeLock{},
				rollbackRequest(target),
			)
			if test.wantError != nil {
				if !errors.Is(err, test.wantError) {
					t.Fatalf("error = %v, want %v", err, test.wantError)
				}
				if connection.sendCount("rollbacks") != 0 {
					t.Fatal("header was inserted without exact invalidation completeness")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !commit.Committed || !commit.ResolvedUncertain {
				t.Fatalf("resolved invalidation commit = %#v", commit)
			}
			if !test.wantHeader || connection.sendCount("rollbacks") != 1 {
				t.Fatal("complete invalidations did not lead to one header")
			}
		})
	}
}

func TestUncertainRollbackHeaderExactReadback(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		readback  string
		wantError error
		committed bool
	}{
		{name: "complete", readback: "complete", committed: true},
		{name: "absent", readback: "absent", wantError: ErrNotCommitted},
		{name: "conflicting", readback: "conflicting", wantError: ErrCommitConflict},
		{name: "query_failure", readback: "failure", wantError: ErrCommitIndeterminate},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			connection := newFakeConnection()
			connection.sendErrors["rollbacks"] = errors.New("response lost")
			configureRollbackState(
				connection,
				func(query string, _ []any) ([][]any, error) {
					switch {
					case sameSQL(query, rollbackReadbackSQL):
						if test.readback == "failure" {
							return nil, errors.New("header readback unavailable")
						}
						if test.readback == "absent" {
							return nil, nil
						}
						rows := rollbackReadbackRows(connection)
						if test.readback == "conflicting" {
							rows[0][15] = "different reason"
						}
						return rows, nil
					case sameSQL(query, invalidationReadbackSQL):
						return invalidationReadbackRows(connection), nil
					default:
						return nil, fmt.Errorf("unexpected query")
					}
				},
			)
			db := initializedTestDB(connection, 3, 3)
			_, _, target := rollbackPoints()
			commit, err := db.Rollback(
				context.Background(),
				&fakeLock{},
				rollbackRequest(target),
			)
			if test.wantError != nil {
				if !errors.Is(err, test.wantError) {
					t.Fatalf("error = %v, want %v", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !commit.Committed || !commit.ResolvedUncertain {
				t.Fatalf("resolved header commit = %#v", commit)
			}
		})
	}
}

func TestRollbackThenReadoptionUsesFreshPublication(t *testing.T) {
	t.Parallel()
	connection := newFakeConnection()
	configureRollbackState(connection, nil)
	db := initializedTestDB(connection, 3, 3)
	_, _, target := rollbackPoints()
	rollbackCommit, err := db.Rollback(
		context.Background(),
		&fakeLock{},
		rollbackRequest(target),
	)
	if err != nil {
		t.Fatal(err)
	}
	candidate := richCandidate(2)
	publicationCommit, err := db.Publish(
		context.Background(),
		&fakeLock{},
		[]Candidate{candidate},
	)
	if err != nil {
		t.Fatal(err)
	}
	if rollbackCommit.EventSeq != 4 ||
		publicationCommit.FirstPublicationID != 4 ||
		publicationCommit.FirstEventSeq != 5 {
		t.Fatalf(
			"re-adoption reused identity: rollback %#v publication %#v",
			rollbackCommit,
			publicationCommit,
		)
	}
}

func invalidationReadbackRows(connection *fakeConnection) [][]any {
	inserted := connection.rowsFor("invalidation")
	rows := make([][]any, len(inserted))
	for index, row := range inserted {
		rollbackID := row[4].(uuid.UUID)
		rows[index] = []any{
			row[1],
			row[2],
			row[3],
			&rollbackID,
			row[5],
			row[6],
			row[7],
			row[8],
			row[9],
			row[10],
		}
	}
	return rows
}

func rollbackReadbackRows(connection *fakeConnection) [][]any {
	inserted := connection.rowsFor("rollbacks")
	rows := make([][]any, len(inserted))
	for index, row := range inserted {
		toSlot := nullableUint64Pointer(row[3])
		toNumber := nullableUint64Pointer(row[5])
		oldSlot := nullableUint64Pointer(row[7])
		oldNumber := nullableUint64Pointer(row[9])
		rows[index] = []any{
			row[0],
			row[2],
			toSlot,
			row[4],
			toNumber,
			row[6],
			oldSlot,
			row[8],
			oldNumber,
			row[10],
			row[11],
			row[12],
			row[13],
			row[14],
			row[15],
			row[16],
			row[17],
			row[18],
		}
	}
	return rows
}

func nullableUint64Pointer(value any) *uint64 {
	if value == nil {
		return nil
	}
	converted := value.(uint64)
	return &converted
}
