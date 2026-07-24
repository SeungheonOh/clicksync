package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"cardano-clicksync/internal/model"
	"cardano-clicksync/migrations"
)

func testHash(value byte) model.Hash32 {
	var hash model.Hash32
	for index := range hash {
		hash[index] = value
	}
	return hash
}

func initializedTestDB(
	connection *fakeConnection,
	publicationHighWater uint64,
	eventHighWater uint64,
) *DB {
	db := newDB(connection)
	identity := DatasetIdentity{
		DatasetID:    uuid.MustParse("10000000-0000-0000-0000-000000000001"),
		SchemaHash:   model.Hash32(migrations.SchemaHash),
		NetworkMagic: 42,
		NetworkName:  "testnet",
		Start:        Point{Origin: true},
		CreatedAt:    time.Date(2026, 7, 24, 1, 2, 3, 0, time.UTC),
		SourceBuild:  "test",
	}
	allocator, err := newAllocator(publicationHighWater, eventHighWater)
	if err != nil {
		panic(err)
	}
	db.identity = &identity
	db.allocator = allocator
	db.writerID = uuid.MustParse("20000000-0000-0000-0000-000000000002")
	db.now = func() time.Time {
		return time.Date(2026, 7, 24, 4, 5, 6, 123456000, time.UTC)
	}
	return db
}

func TestStateDerivesLatestAdoptionTipAndWindow(t *testing.T) {
	t.Parallel()
	connection := newFakeConnection()
	tip := Point{Slot: 30, Hash: testHash(3), BlockNumber: 3}
	parent := Point{Slot: 20, Hash: testHash(2), BlockNumber: 2}
	connection.queryFn = func(query string, args []any) ([][]any, error) {
		switch {
		case sameSQL(query, latestAdoptionSQL):
			return [][]any{
				{uint64(30), uint64(103), bytes32(tip.Hash), tip.Slot, tip.BlockNumber, false},
				{uint64(20), uint64(102), bytes32(parent.Hash), parent.Slot, parent.BlockNumber, false},
			}, nil
		case sameSQL(query, latestRollbackSQL):
			return nil, nil
		case sameSQL(query, canonicalWindowSQL):
			if got, want := args[2], uint64(5); got != want {
				t.Fatalf("canonical limit = %v, want %v", got, want)
			}
			return [][]any{
				{uint64(103), uint64(30), bytes32(tip.Hash), tip.Slot, tip.BlockNumber, false},
				{uint64(102), uint64(20), bytes32(parent.Hash), parent.Slot, parent.BlockNumber, false},
			}, nil
		default:
			return nil, errors.New("unexpected query")
		}
	}
	db := initializedTestDB(connection, 103, 30)
	state, err := db.State(context.Background(), 4)
	if err != nil {
		t.Fatal(err)
	}
	if state.Snapshot != 30 || state.Tip != tip {
		t.Fatalf("state head = %d %#v, want 30 %#v", state.Snapshot, state.Tip, tip)
	}
	if len(state.Canonical) != 2 ||
		state.Canonical[0].Point != tip ||
		state.Canonical[1].Point != parent {
		t.Fatalf("canonical window = %#v", state.Canonical)
	}
	if len(state.Intersections) != 3 ||
		state.Intersections[0] != tip ||
		state.Intersections[2] != (Point{Origin: true}) {
		t.Fatalf("intersection candidates = %#v", state.Intersections)
	}
}

func TestStateLatestRollbackAndReadoption(t *testing.T) {
	t.Parallel()
	parent := Point{Slot: 20, Hash: testHash(2), BlockNumber: 2}
	readopted := Point{Slot: 30, Hash: testHash(3), BlockNumber: 3}
	for _, test := range []struct {
		name      string
		adoption  uint64
		rollback  uint64
		tip       Point
		canonical [][]any
	}{
		{
			name:     "rollback_is_latest",
			adoption: 30,
			rollback: 40,
			tip:      parent,
			canonical: [][]any{
				{uint64(102), uint64(20), bytes32(parent.Hash), parent.Slot, parent.BlockNumber, false},
			},
		},
		{
			name:     "later_readoption_is_latest",
			adoption: 50,
			rollback: 40,
			tip:      readopted,
			canonical: [][]any{
				{uint64(203), uint64(50), bytes32(readopted.Hash), readopted.Slot, readopted.BlockNumber, false},
				{uint64(102), uint64(20), bytes32(parent.Hash), parent.Slot, parent.BlockNumber, false},
			},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			connection := newFakeConnection()
			connection.queryFn = func(query string, _ []any) ([][]any, error) {
				switch {
				case sameSQL(query, latestAdoptionSQL):
					point := readopted
					if test.adoption == 30 {
						point = Point{Slot: 35, Hash: testHash(9), BlockNumber: 4}
					}
					return [][]any{{
						test.adoption,
						uint64(203),
						bytes32(point.Hash),
						point.Slot,
						point.BlockNumber,
						false,
					}}, nil
				case sameSQL(query, latestRollbackSQL):
					slot := parent.Slot
					number := parent.BlockNumber
					return [][]any{{
						test.rollback,
						false,
						&slot,
						bytes32(parent.Hash),
						&number,
						false,
					}}, nil
				case sameSQL(query, canonicalWindowSQL):
					return test.canonical, nil
				default:
					return nil, errors.New("unexpected query")
				}
			}
			state, err := initializedTestDB(connection, 203, 50).State(
				context.Background(),
				10,
			)
			if err != nil {
				t.Fatal(err)
			}
			wantSnapshot := max(test.adoption, test.rollback)
			if state.Snapshot != wantSnapshot || state.Tip != test.tip {
				t.Fatalf(
					"state = %d %#v, want %d %#v",
					state.Snapshot,
					state.Tip,
					wantSnapshot,
					test.tip,
				)
			}
		})
	}
}

func TestStateIgnoresHeaderlessInvalidationsByContract(t *testing.T) {
	t.Parallel()
	if !strings.Contains(canonicalWindowSQL, "INNER JOIN clicksync.rollbacks") ||
		!strings.Contains(canonicalWindowSQL, "rb.rollback_id = ce.rollback_id") ||
		!strings.Contains(canonicalWindowSQL, "rb.event_seq = ce.event_seq") {
		t.Fatal("canonical membership does not require the exact rollback header")
	}
}

func TestStateAtFreshDatasetStart(t *testing.T) {
	t.Parallel()
	connection := newFakeConnection()
	connection.queryFn = func(query string, _ []any) ([][]any, error) {
		switch {
		case sameSQL(query, latestAdoptionSQL), sameSQL(query, latestRollbackSQL):
			return nil, nil
		default:
			return nil, errors.New("unexpected query")
		}
	}
	db := initializedTestDB(connection, 0, 0)
	state, err := db.State(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if state.Snapshot != 0 || !state.Tip.Origin || len(state.Canonical) != 0 ||
		len(state.Intersections) != 1 || !state.Intersections[0].Origin {
		t.Fatalf("fresh state = %#v", state)
	}
}

func TestInspectIsReadOnlyAndDoesNotRequireInitialization(t *testing.T) {
	t.Parallel()
	emptyConnection := newFakeConnection()
	empty, found, err := newDB(emptyConnection).Inspect(
		context.Background(),
		10,
	)
	if err != nil {
		t.Fatal(err)
	}
	if found ||
		empty.Snapshot != 0 ||
		empty.Tip != (Point{}) ||
		empty.Dataset != (DatasetIdentity{}) ||
		len(empty.Canonical) != 0 ||
		len(empty.Intersections) != 0 {
		t.Fatalf("empty Inspect = %#v, %v", empty, found)
	}
	if len(emptyConnection.execs) != 0 || len(emptyConnection.batches) != 0 {
		t.Fatal("empty Inspect mutated ClickHouse")
	}

	connection := newFakeConnection()
	identity := initializedTestDB(connection, 0, 0).identity
	connection.datasetRows = [][]any{datasetScanRow(*identity)}
	connection.queryFn = func(query string, _ []any) ([][]any, error) {
		switch {
		case sameSQL(query, loadDatasetSQL):
			return cloneRows(connection.datasetRows), nil
		case sameSQL(query, latestAdoptionSQL), sameSQL(query, latestRollbackSQL):
			return nil, nil
		default:
			return nil, errors.New("unexpected query")
		}
	}
	state, found, err := newDB(connection).Inspect(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if !found || state.Dataset.DatasetID != identity.DatasetID ||
		!state.Tip.Origin {
		t.Fatalf("Inspect state = %#v, found %v", state, found)
	}
	if len(connection.execs) != 0 || len(connection.batches) != 0 {
		t.Fatal("Inspect mutated ClickHouse")
	}
}

func TestInspectRejectsPhysicalConflictAndSchemaMismatch(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func([][]any)
	}{
		{
			name: "physical_conflict",
			mutate: func(rows [][]any) {
				rows[1][3] = "different-network"
			},
		},
		{
			name: "schema_mismatch",
			mutate: func(rows [][]any) {
				rows[0][1] = bytes32(testHash(0xee))
				rows[1][1] = bytes32(testHash(0xee))
			},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			connection := newFakeConnection()
			identity := initializedTestDB(connection, 0, 0).identity
			rows := [][]any{datasetScanRow(*identity), datasetScanRow(*identity)}
			test.mutate(rows)
			connection.datasetRows = rows
			_, found, err := newDB(connection).Inspect(context.Background(), 10)
			if err == nil || found {
				t.Fatalf("Inspect conflict = found %v, err %v", found, err)
			}
		})
	}
}

func TestLatestActionRejectsSameEventKindConflict(t *testing.T) {
	t.Parallel()
	connection := newFakeConnection()
	point := Point{Slot: 1, Hash: testHash(1), BlockNumber: 1}
	connection.queryFn = func(query string, _ []any) ([][]any, error) {
		switch {
		case sameSQL(query, latestAdoptionSQL):
			return [][]any{{
				uint64(7), uint64(1), bytes32(point.Hash), point.Slot,
				point.BlockNumber, false,
			}}, nil
		case sameSQL(query, latestRollbackSQL):
			slot := point.Slot
			number := point.BlockNumber
			return [][]any{{
				uint64(7), false, &slot, bytes32(point.Hash), &number, false,
			}}, nil
		default:
			return nil, errors.New("unexpected query")
		}
	}
	_, err := initializedTestDB(connection, 1, 7).State(context.Background(), 2)
	if !errors.Is(err, ErrCommitConflict) {
		t.Fatalf("error = %v, want ErrCommitConflict", err)
	}
}

func datasetScanRow(identity DatasetIdentity) []any {
	var startSlot, startHash, startNumber any
	if !identity.Start.Origin {
		slot := identity.Start.Slot
		number := identity.Start.BlockNumber
		startSlot = &slot
		startHash = bytes32(identity.Start.Hash)
		startNumber = &number
	}
	return []any{
		identity.DatasetID,
		bytes32(identity.SchemaHash),
		identity.NetworkMagic,
		identity.NetworkName,
		identity.Start.Origin,
		startSlot,
		startHash,
		startNumber,
		identity.Start.IsByronEBB,
		identity.CreatedAt,
		identity.SourceBuild,
	}
}
