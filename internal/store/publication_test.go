package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"cardano-clicksync/internal/model"
)

func richCandidate(blockValue byte) Candidate {
	blockHash := testHash(blockValue)
	parentHash := testHash(blockValue - 1)
	txHash := testHash(blockValue + 20)
	sourceHash := testHash(blockValue + 21)
	datumHash := testHash(blockValue + 22)
	dataHash := testHash(blockValue + 23)
	metadataHash := testHash(blockValue + 24)
	var policy model.Hash28
	var credential model.Hash28
	for index := range policy {
		policy[index] = blockValue + 30
		credential[index] = blockValue + 31
	}
	inline := "inline"
	kind := "key"
	fee := uint64(123)
	outputIndex := uint32(0)
	targetOrdinal := uint32(2)
	return Candidate{
		Block: model.Block{
			Hash:       blockHash,
			ParentHash: &parentHash,
			Slot:       uint64(blockValue) * 10,
			Number:     uint64(blockValue),
			Era:        "Conway",
			Type:       7,
			ObservedAt: time.Date(2026, 7, 24, 1, 0, int(blockValue), 0, time.UTC),
			Datums: []model.DatumBody{{
				Hash: datumHash,
				CBOR: []byte{0xd8, blockValue},
			}},
			Transactions: []model.Transaction{{
				Hash:         txHash,
				Order:        0,
				Era:          "Conway",
				Phase2Valid:  true,
				FlowKind:     "regular",
				DeclaredFee:  &fee,
				EffectiveFee: &fee,
				MintApplied:  true,
				Mint: []model.AssetDelta{{
					PolicyID: policy,
					Name:     []byte{0x00, blockValue},
					Quantity: -2,
				}},
				Inputs: []model.Input{{
					TransactionHash:  txHash,
					TransactionOrder: 0,
					SourceHash:       sourceHash,
					SourceIndex:      1,
					BodyOrdinal:      0,
					Role:             "regular",
					Consumed:         true,
				}},
				Outputs: []model.Output{{
					TransactionHash:         txHash,
					TransactionOrder:        0,
					Index:                   0,
					BodyOrdinal:             0,
					Kind:                    "regular",
					Address:                 []byte{0x01, 0x00, blockValue},
					PaymentCredentialKind:   &kind,
					PaymentCredentialHash:   &credential,
					Lovelace:                999,
					Assets:                  []model.Asset{{PolicyID: policy, Name: []byte{0xff}, Quantity: 0}},
					DatumKind:               "inline",
					DatumHash:               &datumHash,
					ReferenceScriptLanguage: &inline,
				}},
				DatumObservations: []model.DatumObservation{{
					Hash:             datumHash,
					TransactionHash:  txHash,
					TransactionOrder: 0,
					SourceKind:       "inline_output",
					SourceOrdinal:    0,
					OutputIndex:      &outputIndex,
				}},
				Withdrawals: []model.Withdrawal{{
					TransactionHash:  txHash,
					TransactionOrder: 0,
					BodyOrdinal:      0,
					RewardAccount:    []byte{0xe1, blockValue},
					Lovelace:         77,
					Applied:          true,
					// Credential enrichment is intentionally absent.
				}},
				Redeemers: []model.Redeemer{{
					TransactionHash:   txHash,
					TransactionOrder:  0,
					RawPurposeTag:     0,
					Purpose:           "spend",
					Index:             0,
					DataCBOR:          []byte{0x18, blockValue},
					DataHash:          dataHash,
					ExUnitsMemory:     10,
					ExUnitsSteps:      20,
					Applied:           true,
					TargetTxHash:      &sourceHash,
					TargetOutputIndex: &outputIndex,
					TargetBodyOrdinal: &targetOrdinal,
				}},
				Metadata: &model.Metadata{
					TransactionHash:  txHash,
					TransactionOrder: 0,
					Labels:           []uint64{1, 9},
					CBOR:             []byte{0xa1, blockValue},
					ContentHash:      metadataHash,
				},
			}},
		},
		ContentHash: testHash(blockValue + 40),
		Relays: []Relay{
			{Host: "relay-a:3001", Address: "192.0.2.1:3001", Operator: "a", N2NVersion: 13},
			{Host: "relay-b:3001", Address: "192.0.2.2:3001", Operator: "b", N2NVersion: 14},
		},
		RawLength: 1234,
	}
}

func TestPublishMapsEveryTableOnceAndPerformsNoSuccessfulRead(t *testing.T) {
	t.Parallel()
	connection := newFakeConnection()
	db := initializedTestDB(connection, 10, 20)
	candidates := []Candidate{richCandidate(2), richCandidate(3)}
	commit, err := db.Publish(context.Background(), &fakeLock{}, candidates)
	if err != nil {
		t.Fatal(err)
	}
	if !commit.Committed || commit.FirstPublicationID != 11 ||
		commit.LastPublicationID != 12 || commit.FirstEventSeq != 21 ||
		commit.LastEventSeq != 22 {
		t.Fatalf("commit = %#v", commit)
	}
	for _, table := range []string{
		"blocks",
		"transactions",
		"inputs",
		"outputs",
		"datum_bodies",
		"datum_observations",
		"withdrawals",
		"redeemers",
		"transaction_metadata",
		"adoption",
	} {
		if got := connection.sendCount(table); got != 1 {
			t.Errorf("%s sends = %d, want 1", table, got)
		}
		if got := len(connection.rowsFor(table)); got != 2 {
			t.Errorf("%s rows = %d, want 2", table, got)
		}
	}
	if got := connection.queryCount(); got != 0 {
		t.Fatalf("ordinary successful publication issued %d SELECT queries", got)
	}

	block := connection.rowsFor("blocks")[0]
	if got := block[15].([]byte); string(got) != string(candidates[0].ContentHash[:]) {
		t.Fatalf("content hash = %x, want %x", got, candidates[0].ContentHash)
	}
	if got := block[16].([]string); len(got) != 2 || got[0] != "relay-a:3001" {
		t.Fatalf("relay hosts = %#v", got)
	}
	if got := block[19].([]uint16); len(got) != 2 || got[1] != 14 {
		t.Fatalf("n2n versions = %#v", got)
	}
	if got := block[20].(uint32); got != 42 {
		t.Fatalf("network magic = %d, want 42", got)
	}
	if got := len(connection.rowsFor("inputs")[0]); got != 9 {
		t.Fatalf("input column count = %d, want 9 (no source resolution)", got)
	}
	output := connection.rowsFor("outputs")[0]
	if output[9] == nil {
		t.Fatal("payment credential hash was not mapped")
	}
	if output[16] != nil || output[17] == nil {
		t.Fatal("independently nullable reference-script enrichment was not preserved")
	}
	withdrawal := connection.rowsFor("withdrawals")[0]
	if withdrawal[8] != nil || withdrawal[9] != nil {
		t.Fatal("missing withdrawal credential enrichment was not mapped as NULL")
	}
}

func TestPublishSchedulesIndependentFactTablesConcurrently(t *testing.T) {
	t.Parallel()
	connection := newFakeConnection()
	connection.blockFactSends = make(chan struct{})
	connection.factSendEntered = make(chan string, 16)
	db := initializedTestDB(connection, 0, 0)
	result := make(chan error, 1)
	go func() {
		_, err := db.Publish(
			context.Background(),
			&fakeLock{},
			[]Candidate{richCandidate(2)},
		)
		result <- err
	}()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for entered := 0; entered < 2; entered++ {
		select {
		case <-connection.factSendEntered:
		case <-timer.C:
			t.Fatal("fact inserts did not overlap")
		}
	}
	close(connection.blockFactSends)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	connection.mu.Lock()
	maximum := connection.maxFactSends
	connection.mu.Unlock()
	if maximum < 2 {
		t.Fatalf("maximum concurrent fact sends = %d, want at least 2", maximum)
	}
}

func TestEveryFactFailurePreventsAdoption(t *testing.T) {
	t.Parallel()
	for _, table := range []string{
		"blocks",
		"transactions",
		"inputs",
		"outputs",
		"datum_bodies",
		"datum_observations",
		"withdrawals",
		"redeemers",
		"transaction_metadata",
	} {
		table := table
		t.Run(table, func(t *testing.T) {
			connection := newFakeConnection()
			connection.sendErrors[table] = errors.New("injected failure")
			db := initializedTestDB(connection, 0, 0)
			commit, err := db.Publish(
				context.Background(),
				&fakeLock{},
				[]Candidate{richCandidate(2)},
			)
			if err == nil {
				t.Fatal("fact failure was ignored")
			}
			if commit.Committed {
				t.Fatal("failed fact batch was reported committed")
			}
			if got := connection.sendCount("adoption"); got != 0 {
				t.Fatalf("adoption sends = %d, want 0", got)
			}
		})
	}
}

func TestLockLossPreventsAdoption(t *testing.T) {
	t.Parallel()
	connection := newFakeConnection()
	db := initializedTestDB(connection, 0, 0)
	_, err := db.Publish(
		context.Background(),
		&fakeLock{failAt: 2},
		[]Candidate{richCandidate(2)},
	)
	if err == nil {
		t.Fatal("lock loss was ignored")
	}
	if got := connection.sendCount("adoption"); got != 0 {
		t.Fatalf("adoption sends = %d, want 0", got)
	}
}

func TestFailedAttemptBurnsIdentifiers(t *testing.T) {
	t.Parallel()
	connection := newFakeConnection()
	connection.sendErrors["blocks"] = errors.New("disk full")
	db := initializedTestDB(connection, 0, 0)
	first, err := db.Publish(
		context.Background(),
		&fakeLock{},
		[]Candidate{richCandidate(2)},
	)
	if err == nil {
		t.Fatal("first publication unexpectedly succeeded")
	}
	delete(connection.sendErrors, "blocks")
	second, err := db.Publish(
		context.Background(),
		&fakeLock{},
		[]Candidate{richCandidate(3)},
	)
	if err != nil {
		t.Fatal(err)
	}
	if first.FirstPublicationID != 1 || first.FirstEventSeq != 1 ||
		second.FirstPublicationID != 2 || second.FirstEventSeq != 2 {
		t.Fatalf("identifiers were reused: first %#v, second %#v", first, second)
	}
}

func TestAdoptionUncertaintyExactReadback(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		rows      func(*DB, []Candidate) [][]any
		queryErr  error
		wantError error
		committed bool
	}{
		{
			name: "complete",
			rows: func(db *DB, candidates []Candidate) [][]any {
				return adoptionRows(db, candidates)
			},
			committed: true,
		},
		{
			name:      "absent",
			rows:      func(*DB, []Candidate) [][]any { return nil },
			wantError: ErrNotCommitted,
		},
		{
			name: "partial",
			rows: func(db *DB, candidates []Candidate) [][]any {
				return adoptionRows(db, candidates)[:1]
			},
			wantError: ErrCommitConflict,
		},
		{
			name: "conflicting",
			rows: func(db *DB, candidates []Candidate) [][]any {
				rows := adoptionRows(db, candidates)
				rows[0][1] = uint64(999)
				return rows
			},
			wantError: ErrCommitConflict,
		},
		{
			name:      "readback_failure",
			queryErr:  errors.New("query unavailable"),
			wantError: ErrCommitIndeterminate,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			connection := newFakeConnection()
			connection.sendErrors["adoption"] = errors.New("response lost")
			db := initializedTestDB(connection, 0, 0)
			candidates := []Candidate{richCandidate(2), richCandidate(3)}
			connection.queryFn = func(query string, _ []any) ([][]any, error) {
				if !sameSQL(query, adoptionReadbackSQL) {
					return nil, fmt.Errorf("unexpected query")
				}
				if test.queryErr != nil {
					return nil, test.queryErr
				}
				return test.rows(db, candidates), nil
			}
			commit, err := db.Publish(
				context.Background(),
				&fakeLock{},
				candidates,
			)
			if test.wantError != nil {
				if !errors.Is(err, test.wantError) {
					t.Fatalf("error = %v, want %v", err, test.wantError)
				}
				if commit.Committed {
					t.Fatal("error case reported committed")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !commit.Committed || !commit.ResolvedUncertain {
				t.Fatalf("uncertain complete commit = %#v", commit)
			}
		})
	}
}

func adoptionRows(db *DB, candidates []Candidate) [][]any {
	rows := make([][]any, len(candidates))
	recordedAt := db.now()
	for index, candidate := range candidates {
		point := pointForBlock(candidate.Block)
		rows[index] = []any{
			uint64(index + 1),
			uint64(index + 1),
			"adoption",
			true,
			nil,
			bytes32(point.Hash),
			point.Slot,
			point.BlockNumber,
			point.IsByronEBB,
			db.writerID,
			recordedAt,
		}
	}
	return rows
}
