package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/google/uuid"

	"clicksync/internal/config"
	"clicksync/internal/model"
	"clicksync/internal/n2n"
	"clicksync/internal/publication"
	"clicksync/internal/syncer"
	"clicksync/internal/writerlock"
)

func TestNativePendingRollbackRecoveryCuts(t *testing.T) {
	if os.Getenv("CLICKSYNC_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set CLICKSYNC_CLICKHOUSE_INTEGRATION=1 for isolated ClickHouse")
	}
	for _, test := range []struct {
		name      string
		prepare   func(*testing.T, context.Context, *DB, publication.Lock, publication.RollbackCommit, []publication.Descendant)
		depthZero bool
	}{
		{
			name: "reserved_before_invalidations",
			prepare: func(
				_ *testing.T,
				_ context.Context,
				_ *DB,
				_ publication.Lock,
				_ publication.RollbackCommit,
				_ []publication.Descendant,
			) {
			},
		},
		{
			name: "invalidations_inserted_before_marker",
			prepare: func(
				t *testing.T,
				ctx context.Context,
				db *DB,
				_ publication.Lock,
				commit publication.RollbackCommit,
				descendants []publication.Descendant,
			) {
				t.Helper()
				if err := db.InsertInvalidations(ctx, commit, descendants); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "invalidations_written_before_header",
			prepare: func(
				t *testing.T,
				ctx context.Context,
				db *DB,
				lock publication.Lock,
				commit publication.RollbackCommit,
				descendants []publication.Descendant,
			) {
				t.Helper()
				if err := db.InsertInvalidations(ctx, commit, descendants); err != nil {
					t.Fatal(err)
				}
				if err := db.MarkRollbackInvalidations(ctx, lock, commit, "recovery-test"); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "header_committed_before_manifest",
			prepare: func(
				t *testing.T,
				ctx context.Context,
				db *DB,
				lock publication.Lock,
				commit publication.RollbackCommit,
				descendants []publication.Descendant,
			) {
				t.Helper()
				if err := db.InsertInvalidations(ctx, commit, descendants); err != nil {
					t.Fatal(err)
				}
				if err := db.MarkRollbackInvalidations(ctx, lock, commit, "recovery-test"); err != nil {
					t.Fatal(err)
				}
				if err := db.InsertRollbackHeader(ctx, commit); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "byte_identical_physical_duplicates",
			prepare: func(
				t *testing.T,
				ctx context.Context,
				db *DB,
				lock publication.Lock,
				commit publication.RollbackCommit,
				descendants []publication.Descendant,
			) {
				t.Helper()
				if err := db.InsertInvalidations(ctx, commit, descendants); err != nil {
					t.Fatal(err)
				}
				if err := db.MarkRollbackInvalidations(ctx, lock, commit, "recovery-test"); err != nil {
					t.Fatal(err)
				}
				if err := db.InsertRollbackHeader(ctx, commit); err != nil {
					t.Fatal(err)
				}
				if err := db.conn.Exec(
					ctx,
					`INSERT INTO clicksync.chain_events
SELECT *
FROM clicksync.chain_events
WHERE event_kind = 'invalidation'
  AND event_seq = ?
  AND rollback_id = ?`,
					commit.EventSeq,
					uuid.UUID(commit.RollbackID),
				); err != nil {
					t.Fatal(err)
				}
				if err := db.conn.Exec(
					ctx,
					`INSERT INTO clicksync.rollbacks
SELECT *
FROM clicksync.rollbacks
WHERE event_seq = ?
  AND rollback_id = ?`,
					commit.EventSeq,
					uuid.UUID(commit.RollbackID),
				); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:      "depth_zero_reserved",
			depthZero: true,
			prepare: func(
				_ *testing.T,
				_ context.Context,
				_ *DB,
				_ publication.Lock,
				_ publication.RollbackCommit,
				_ []publication.Descendant,
			) {
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			db, lock, seed, first, second, now := nativeRollbackFixture(t, ctx)
			defer db.Close()
			defer lock.Release()

			target := first
			depth := uint32(1)
			oldEvent := uint64(2)
			oldTip := second
			if test.depthZero {
				target = second
				depth = 0
			}
			checkPoint := chainPointForTest(target)
			check, err := db.BeginTrustCheck(
				ctx,
				lock,
				&checkPoint,
				2,
				now.Add(3*time.Second),
				seed.WriterID,
				"recovery-test",
			)
			if err != nil {
				t.Fatal(err)
			}
			observations := []model.PeerObservation{
				trustObservationForTest(check, "relay-a", "operator-a", "agreed", now.Add(4*time.Second)),
				trustObservationForTest(check, "relay-b", "operator-b", "agreed", now.Add(4*time.Second)),
			}
			if err := db.InsertPeerObservations(ctx, lock, observations); err != nil {
				t.Fatal(err)
			}
			eventSeq := uint64(3)
			commit := publication.RollbackCommit{
				RollbackID:            id16(0xc1),
				EventSeq:              eventSeq,
				To:                    target,
				OldTip:                oldTip,
				OldEventSeq:           oldEvent,
				Depth:                 depth,
				Reason:                "native crash-cut recovery",
				ObservedPeers:         []string{"relay-a", "relay-b"},
				ObservedOperators:     []string{"operator-a", "operator-b"},
				CorroborationRequired: 2,
				CheckID:               copyIDPointer(check.ID),
				AgreementGroup:        copyIDPointer(check.AgreementGroup),
				CheckAttempt:          check.Attempt,
				CheckedEventSeq:       check.CheckedEventSeq,
				WriterID:              seed.WriterID,
				RecordedAt:            now.Add(5 * time.Second),
			}
			commit, err = db.ReserveRollbackManifest(ctx, lock, commit, "recovery-test")
			if err != nil {
				t.Fatal(err)
			}
			descendants := []publication.Descendant(nil)
			if depth != 0 {
				descendants = []publication.Descendant{{
					PublicationID: 2,
					Point:         second,
				}}
			}
			test.prepare(t, ctx, db, lock, commit, descendants)

			restartSeed := seed
			restartSeed.CreatedAt = now.Add(10 * time.Second)
			if _, err := db.LoadOrCreateManifest(ctx, lock, restartSeed); err != nil {
				t.Fatal(err)
			}
			latest, found, err := db.loadLatestManifestRecord(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !found || latest.PendingRollback != nil {
				t.Fatalf("recovered manifest found=%t pending=%+v", found, latest.PendingRollback)
			}
			if latest.TrustStatus != "agreed" ||
				latest.Checked == nil ||
				latest.Checked.EventSeq != check.CheckedEventSeq ||
				latest.Checked.Point != target ||
				latest.Physical != (manifestHead{EventSeq: eventSeq, Point: target}) ||
				latest.Effective != latest.Physical ||
				latest.LastAgreed == nil ||
				*latest.LastAgreed != latest.Physical {
				t.Fatalf("recovered manifest = %+v", latest)
			}
			if committed, err := db.RollbackCommitted(ctx, commit); err != nil || !committed {
				t.Fatalf("rollback committed=%t err=%v", committed, err)
			}
		})
	}
}

func TestNativeTrustEvidenceCanonicalIntegrity(t *testing.T) {
	if os.Getenv("CLICKSYNC_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set CLICKSYNC_CLICKHOUSE_INTEGRATION=1 for isolated ClickHouse")
	}
	t.Run("identical_duplicate_and_exact_observer_set", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		db, lock, seed, first, second, now := nativeRollbackFixture(t, ctx)
		defer db.Close()
		defer lock.Release()
		check := beginNativeRollbackCheck(t, ctx, db, lock, seed, first, now)
		rows := []model.PeerObservation{
			trustObservationForTest(check, "relay-a", "operator-a", "agreed", now.Add(4*time.Second)),
			trustObservationForTest(check, "relay-b", "operator-b", "agreed", now.Add(4*time.Second)),
		}
		if err := db.InsertPeerObservations(ctx, lock, rows); err != nil {
			t.Fatal(err)
		}
		if err := db.conn.Exec(
			ctx,
			`INSERT INTO clicksync.peer_observations
SELECT * EXCEPT(operator_key)
FROM clicksync.peer_observations
WHERE observation_id = ?`,
			uuid.UUID(rows[0].ID),
		); err != nil {
			t.Fatal(err)
		}
		evidence, err := db.readTrustEvidence(ctx, check)
		if err != nil || evidence.Confirmed != 2 {
			t.Fatalf("duplicate evidence=%+v err=%v", evidence, err)
		}
		commit := rollbackCommitForTest(check, seed.WriterID, first, second, now)
		commit, err = db.ReserveRollbackManifest(
			ctx,
			lock,
			commit,
			"trust-integrity-test",
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.validateRollbackCommitEvidence(ctx, commit); err != nil {
			t.Fatal(err)
		}
		swapped := commit
		swapped.ObservedPeers = []string{"relay-b", "relay-a"}
		if _, err := db.validateRollbackCommitEvidence(ctx, swapped); err == nil {
			t.Fatal("swapped peer/operator evidence was accepted")
		}
		omitted := commit
		omitted.ObservedPeers = omitted.ObservedPeers[:1]
		omitted.ObservedOperators = omitted.ObservedOperators[:1]
		if _, err := db.validateRollbackCommitEvidence(ctx, omitted); err == nil {
			t.Fatal("omitted agreed observer was accepted")
		}
	})

	for _, test := range []struct {
		name   string
		mutate func(syncer.CheckIdentity, []model.PeerObservation) []model.PeerObservation
	}{
		{
			name: "valid_digest_wrong_checkpoint",
			mutate: func(_ syncer.CheckIdentity, rows []model.PeerObservation) []model.PeerObservation {
				wrong := rows[0]
				slot := *wrong.CheckpointSlot + 1
				wrong.CheckpointSlot = &slot
				if err := model.FinalizePeerObservationIdentity(&wrong); err != nil {
					panic(err)
				}
				return []model.PeerObservation{wrong, rows[1]}
			},
		},
		{
			name: "cross_attempt_same_check_id",
			mutate: func(_ syncer.CheckIdentity, rows []model.PeerObservation) []model.PeerObservation {
				wrong := rows[0]
				wrong.CheckAttempt++
				if err := model.FinalizePeerObservationIdentity(&wrong); err != nil {
					panic(err)
				}
				return []model.PeerObservation{wrong, rows[1]}
			},
		},
		{
			name: "multiple_outcomes_normalized_operator",
			mutate: func(check syncer.CheckIdentity, rows []model.PeerObservation) []model.PeerObservation {
				conflict := trustObservationForTest(
					check,
					"relay-c",
					" OPERATOR-A ",
					"disagreed",
					rows[0].ObservedAt,
				)
				return append(rows, conflict)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			db, lock, seed, first, _, now := nativeRollbackFixture(t, ctx)
			defer db.Close()
			defer lock.Release()
			check := beginNativeRollbackCheck(t, ctx, db, lock, seed, first, now)
			rows := []model.PeerObservation{
				trustObservationForTest(check, "relay-a", "operator-a", "agreed", now.Add(4*time.Second)),
				trustObservationForTest(check, "relay-b", "operator-b", "agreed", now.Add(4*time.Second)),
			}
			err := db.InsertPeerObservations(ctx, lock, test.mutate(check, rows))
			if test.name == "valid_digest_wrong_checkpoint" ||
				test.name == "cross_attempt_same_check_id" {
				if err == nil {
					t.Fatal("invalid observation was admitted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.readTrustEvidence(ctx, check); err == nil {
				t.Fatal("malformed canonical trust evidence was accepted")
			}
		})
	}

	t.Run("hashed_field_or_digest_corruption", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		db, lock, seed, first, _, now := nativeRollbackFixture(t, ctx)
		defer db.Close()
		defer lock.Release()
		check := beginNativeRollbackCheck(t, ctx, db, lock, seed, first, now)
		rows := []model.PeerObservation{
			trustObservationForTest(check, "relay-a", "operator-a", "agreed", now.Add(4*time.Second)),
			trustObservationForTest(check, "relay-b", "operator-b", "agreed", now.Add(4*time.Second)),
		}
		if err := db.InsertPeerObservations(ctx, lock, rows); err != nil {
			t.Fatal(err)
		}
		if err := db.conn.Exec(
			ctx,
			`INSERT INTO clicksync.peer_observations
SELECT * EXCEPT(operator_key) REPLACE('tampered reason' AS reason)
FROM clicksync.peer_observations
WHERE observation_id = ?`,
			uuid.UUID(rows[0].ID),
		); err != nil {
			t.Fatal(err)
		}
		if _, err := db.readTrustEvidence(ctx, check); err == nil {
			t.Fatal("hashed-field corruption was accepted")
		}
	})
}

func TestNativeTrustAttemptIdentityAdvancesWithinAgreementGroup(t *testing.T) {
	if os.Getenv("CLICKSYNC_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set CLICKSYNC_CLICKHOUSE_INTEGRATION=1 for isolated ClickHouse")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	db, lock, seed, first, second, now := nativeRollbackFixture(t, ctx)
	defer db.Close()
	defer lock.Release()

	newer := beginNativeRollbackCheck(t, ctx, db, lock, seed, second, now)
	if err := db.InsertPeerObservations(ctx, lock, []model.PeerObservation{
		trustObservationForTest(newer, "relay-a", "operator-a", "disagreed", now.Add(4*time.Second)),
		trustObservationForTest(newer, "relay-b", "operator-b", "disagreed", now.Add(4*time.Second)),
	}); err != nil {
		t.Fatal(err)
	}
	olderPoint := chainPointForTest(first)
	older, err := db.BeginTrustCheck(
		ctx,
		lock,
		&olderPoint,
		2,
		now.Add(5*time.Second),
		seed.WriterID,
		"trust-attempt-test",
	)
	if err != nil {
		t.Fatal(err)
	}
	if older.AgreementGroup != newer.AgreementGroup ||
		older.Attempt != newer.Attempt+1 ||
		older.ID == newer.ID {
		t.Fatalf("newer=%+v older=%+v", newer, older)
	}
	if err := db.InsertPeerObservations(ctx, lock, []model.PeerObservation{
		trustObservationForTest(older, "relay-a", "operator-a", "agreed", now.Add(6*time.Second)),
		trustObservationForTest(older, "relay-b", "operator-b", "agreed", now.Add(6*time.Second)),
	}); err != nil {
		t.Fatal(err)
	}
	if evidence, err := db.readTrustEvidence(ctx, older); err != nil ||
		evidence.Confirmed != 2 {
		t.Fatalf("older evidence=%+v err=%v", evidence, err)
	}
	retry, err := db.BeginTrustCheck(
		ctx,
		lock,
		&olderPoint,
		2,
		now.Add(7*time.Second),
		seed.WriterID,
		"trust-attempt-test",
	)
	if err != nil {
		t.Fatal(err)
	}
	if retry.AgreementGroup != older.AgreementGroup ||
		retry.Attempt != older.Attempt+1 ||
		retry.ID == older.ID {
		t.Fatalf("older=%+v retry=%+v", older, retry)
	}
}

func TestNativePeriodicAgreementReleasesExactAdvancedPrimarySuffix(t *testing.T) {
	if os.Getenv("CLICKSYNC_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set CLICKSYNC_CLICKHOUSE_INTEGRATION=1 for isolated ClickHouse")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	db, lock, seed, first, second, now := nativeRollbackFixture(t, ctx)
	defer db.Close()
	defer lock.Release()

	check := beginNativeRollbackCheck(t, ctx, db, lock, seed, second, now)
	if !check.Physical {
		t.Fatalf("periodic check did not begin at the physical head: %+v", check)
	}
	third := publication.Point{
		Slot:        13,
		Hash:        hash32Fill(0xa3),
		BlockNumber: 4,
	}
	insertNativeBlock(t, ctx, db, 3, third, &second.Hash, seed.WriterID, now)
	insertNativeAdoption(t, ctx, db, 3, 3, third, seed.WriterID, now)
	if err := db.PersistManifest(ctx, lock, publication.ManifestUpdate{
		EventSeq:        3,
		Tip:             third,
		Kind:            publication.ManifestAdoption,
		RemoteAdoptions: 1,
		WriterID:        seed.WriterID,
		WriterBuild:     "advanced-periodic-check",
		UpdatedAt:       now.Add(4 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	observations := []model.PeerObservation{
		trustObservationForTest(check, "relay-a", "operator-a", "agreed", now.Add(5*time.Second)),
		trustObservationForTest(check, "relay-b", "operator-b", "agreed", now.Add(5*time.Second)),
	}
	for index := range observations {
		observations[index].Kind = "checkpoint"
		observations[index].ProofMethod = syncer.ObservationProofChainSyncSingleton
		if err := model.FinalizePeerObservationIdentity(&observations[index]); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.InsertPeerObservations(ctx, lock, observations); err != nil {
		t.Fatal(err)
	}
	resolution, err := db.FinalizeTrustCheck(
		ctx,
		lock,
		check,
		false,
		"",
		now.Add(6*time.Second),
		seed.WriterID,
		"advanced-periodic-check",
	)
	if err != nil || resolution.Status != "agreed" || !resolution.Servable {
		t.Fatalf("resolution=%+v err=%v", resolution, err)
	}
	latest, found, err := db.loadLatestManifestRecord(ctx)
	if err != nil || !found {
		t.Fatalf("load manifest found=%t err=%v", found, err)
	}
	if latest.TrustBasis != "primary_only" ||
		latest.PrimarySuffix != 1 ||
		latest.LastAgreed == nil ||
		*latest.LastAgreed != (manifestHead{EventSeq: 2, Point: second}) ||
		latest.Physical != (manifestHead{EventSeq: 3, Point: third}) ||
		latest.Effective != latest.Physical ||
		!latest.Servable {
		t.Fatalf("advanced periodic agreement = %+v", latest)
	}
	if err := verifyManifestRecord(latest); err != nil {
		t.Fatalf("advanced periodic manifest is invalid: %v", err)
	}

	older := beginNativeRollbackCheck(t, ctx, db, lock, seed, first, now.Add(7*time.Second))
	if older.Physical {
		t.Fatalf("older rollback candidate was marked physical: %+v", older)
	}
	if err := db.InsertPeerObservations(ctx, lock, []model.PeerObservation{
		trustObservationForTest(older, "relay-a", "operator-a", "agreed", now.Add(11*time.Second)),
		trustObservationForTest(older, "relay-b", "operator-b", "agreed", now.Add(11*time.Second)),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.FinalizeTrustCheck(
		ctx,
		lock,
		older,
		false,
		"",
		now.Add(12*time.Second),
		seed.WriterID,
		"advanced-periodic-check",
	); err == nil {
		t.Fatal("older rollback candidate released the physical suffix")
	}
}

func TestNativeConcurrentStatusNeverExposesRejectedPhysicalHead(t *testing.T) {
	if os.Getenv("CLICKSYNC_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set CLICKSYNC_CLICKHOUSE_INTEGRATION=1 for isolated ClickHouse")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	db, lock, seed, first, second, now := nativeRollbackFixture(t, ctx)
	defer db.Close()
	defer lock.Release()

	physical := beginNativeRollbackCheck(t, ctx, db, lock, seed, second, now)
	failures := make(chan error, 8)
	stopReaders := make(chan struct{})
	var readers sync.WaitGroup
	for range 8 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				status, found, err := db.DatasetStatus(ctx)
				if err != nil {
					failures <- err
					return
				}
				if !found ||
					status.TrustStatus != "checking" ||
					status.Physical.EventSeq != 2 ||
					status.Effective.EventSeq >= status.Physical.EventSeq {
					failures <- fmt.Errorf("unsafe concurrent status: %+v found=%t", status, found)
					return
				}
			}
		}()
	}
	if err := db.InsertPeerObservations(ctx, lock, []model.PeerObservation{
		trustObservationForTest(physical, "relay-a", "operator-a", "disagreed", now.Add(4*time.Second)),
		trustObservationForTest(physical, "relay-b", "operator-b", "disagreed", now.Add(4*time.Second)),
	}); err != nil {
		t.Fatal(err)
	}
	olderPoint := chainPointForTest(first)
	older, err := db.BeginTrustCheck(
		ctx,
		lock,
		&olderPoint,
		2,
		now.Add(5*time.Second),
		seed.WriterID,
		"concurrent-status-test",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.InsertPeerObservations(ctx, lock, []model.PeerObservation{
		trustObservationForTest(older, "relay-a", "operator-a", "unavailable", now.Add(6*time.Second)),
		trustObservationForTest(older, "relay-b", "operator-b", "unavailable", now.Add(6*time.Second)),
	}); err != nil {
		t.Fatal(err)
	}
	close(stopReaders)
	readers.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	status, found, err := db.DatasetStatus(ctx)
	if err != nil || !found {
		t.Fatalf("final status found=%t err=%v", found, err)
	}
	if status.TrustStatus != "checking" ||
		status.CheckAttempt != older.Attempt ||
		status.AgreementGroup != fmt.Sprintf("%x", older.AgreementGroup) ||
		status.Effective.EventSeq >= status.Physical.EventSeq {
		t.Fatalf("final clamped status = %+v", status)
	}
}

func TestNativeRollbackDuplicateBounds(t *testing.T) {
	if os.Getenv("CLICKSYNC_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set CLICKSYNC_CLICKHOUSE_INTEGRATION=1 for isolated ClickHouse")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	db, lock, seed, first, second, now := nativeRollbackFixture(t, ctx)
	defer db.Close()
	defer lock.Release()
	check := beginNativeRollbackCheck(t, ctx, db, lock, seed, first, now)
	if err := db.InsertPeerObservations(ctx, lock, []model.PeerObservation{
		trustObservationForTest(check, "relay-a", "operator-a", "agreed", now.Add(4*time.Second)),
		trustObservationForTest(check, "relay-b", "operator-b", "agreed", now.Add(4*time.Second)),
	}); err != nil {
		t.Fatal(err)
	}
	commit := rollbackCommitForTest(check, seed.WriterID, first, second, now)
	commit, err := db.ReserveRollbackManifest(
		ctx,
		lock,
		commit,
		"trust-integrity-test",
	)
	if err != nil {
		t.Fatal(err)
	}
	descendants := []publication.Descendant{{PublicationID: 2, Point: second}}
	if err := db.InsertInvalidations(ctx, commit, descendants); err != nil {
		t.Fatal(err)
	}
	for range 8 {
		if err := db.conn.Exec(
			ctx,
			`INSERT INTO clicksync.chain_events
SELECT *
FROM clicksync.chain_events
WHERE event_kind = 'invalidation'
  AND event_seq = ?
  AND rollback_id = ?
LIMIT 1`,
			commit.EventSeq,
			uuid.UUID(commit.RollbackID),
		); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.invalidationsCommitted(ctx, commit, descendants); err == nil {
		t.Fatal("nine invalidation rows were accepted")
	}
	if err := db.InsertRollbackHeader(ctx, commit); err != nil {
		t.Fatal(err)
	}
	for range 8 {
		if err := db.conn.Exec(
			ctx,
			`INSERT INTO clicksync.rollbacks
SELECT *
FROM clicksync.rollbacks
WHERE event_seq = ?
  AND rollback_id = ?
LIMIT 1`,
			commit.EventSeq,
			uuid.UUID(commit.RollbackID),
		); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.RollbackCommitted(ctx, commit); err == nil {
		t.Fatal("nine rollback headers were accepted")
	}
}

func TestNativePendingEvidenceRecoveryCuts(t *testing.T) {
	if os.Getenv("CLICKSYNC_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set CLICKSYNC_CLICKHOUSE_INTEGRATION=1 for isolated ClickHouse")
	}
	for _, physicalBeforeRestart := range []bool{false, true} {
		name := "reservation_before_row"
		if physicalBeforeRestart {
			name = "row_before_prefix_commit"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			db, lock, seed, first, _, now := nativeRollbackFixture(t, ctx)
			defer db.Close()
			defer lock.Release()
			check := beginNativeRollbackCheck(t, ctx, db, lock, seed, first, now)
			row := trustObservationForTest(
				check,
				"relay-c",
				"operator-c",
				"agreed",
				now.Add(4*time.Second),
			)
			if err := db.reserveEvidenceWrite(ctx, lock, row); err != nil {
				t.Fatal(err)
			}
			record, found, err := db.loadLatestManifestRecord(ctx)
			if err != nil || !found || record.PendingEvidenceWrite == nil {
				t.Fatalf("reserved record found=%t pending=%+v err=%v", found, record.PendingEvidenceWrite, err)
			}
			if physicalBeforeRestart {
				if err := db.insertPeerObservationRows(
					ctx,
					[]model.PeerObservation{record.PendingEvidenceWrite.Observation},
				); err != nil {
					t.Fatal(err)
				}
			}
			status, found, err := db.DatasetStatus(ctx)
			if err != nil || !found || status.PendingEvidence == nil ||
				status.EvidenceCount != 0 ||
				status.PendingEvidence.Ordinal != 1 {
				t.Fatalf("legal crash-cut status found=%t status=%+v err=%v", found, status, err)
			}
			lockPath := lock.Path()
			if err := lock.Release(); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(config.Config{
				ClickHouseHost:     "127.0.0.1",
				ClickHousePort:     19100,
				ClickHouseUser:     "default",
				ClickHousePassword: "integration-only",
			})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			restartLock, err := writerlock.Acquire(lockPath, "single-host-flock")
			if err != nil {
				t.Fatal(err)
			}
			defer restartLock.Release()
			restartSeed := seed
			restartSeed.CreatedAt = now.Add(10 * time.Second)
			restartSeed.WriterBuild = "pending-evidence-recovery"
			if _, err := reopened.LoadOrCreateManifest(
				ctx,
				restartLock,
				restartSeed,
			); err != nil {
				t.Fatal(err)
			}
			status, found, err = reopened.DatasetStatus(ctx)
			if err != nil || !found ||
				status.PendingEvidence != nil ||
				status.EvidenceCount != 1 ||
				len(status.EvidenceDigest) != 64 {
				t.Fatalf("recovered status found=%t status=%+v err=%v", found, status, err)
			}
		})
	}
}

func TestNativeEvidenceFreezeSerialization(t *testing.T) {
	if os.Getenv("CLICKSYNC_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set CLICKSYNC_CLICKHOUSE_INTEGRATION=1 for isolated ClickHouse")
	}
	t.Run("insert_vs_finalize", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		db, lock, seed, first, _, now := nativeRollbackFixture(t, ctx)
		defer db.Close()
		defer lock.Release()
		check := beginNativeRollbackCheck(t, ctx, db, lock, seed, first, now)
		firstRow := trustObservationForTest(check, "relay-c", "operator-c", "agreed", now.Add(4*time.Second))
		if err := db.InsertPeerObservations(ctx, lock, []model.PeerObservation{firstRow}); err != nil {
			t.Fatal(err)
		}
		secondRow := trustObservationForTest(check, "relay-d", "operator-d", "agreed", now.Add(5*time.Second))
		start := make(chan struct{})
		var insertErr, finalizeErr error
		var resolution syncer.TrustResolution
		var group sync.WaitGroup
		group.Add(2)
		go func() {
			defer group.Done()
			<-start
			insertErr = db.InsertPeerObservations(ctx, lock, []model.PeerObservation{secondRow})
		}()
		go func() {
			defer group.Done()
			<-start
			resolution, finalizeErr = db.FinalizeTrustCheck(
				ctx, lock, check, false, "", now.Add(6*time.Second),
				seed.WriterID, "freeze-race",
			)
		}()
		close(start)
		group.Wait()
		if finalizeErr != nil {
			t.Fatal(finalizeErr)
		}
		record, found, err := db.loadLatestManifestRecord(ctx)
		if err != nil || !found || record.EvidenceState != "frozen" ||
			record.PendingEvidenceWrite != nil || record.EvidenceDigest == nil {
			t.Fatalf("frozen record found=%t record=%+v err=%v", found, record, err)
		}
		commitment, err := db.readTrustEvidenceCommitment(ctx, check)
		if err != nil ||
			commitment.Count != record.EvidenceCount ||
			commitment.Digest != *record.EvidenceDigest {
			t.Fatalf("frozen commitment=%+v record=%+v err=%v", commitment, record, err)
		}
		if insertErr == nil {
			if record.EvidenceCount != 2 || resolution.Status != "agreed" {
				t.Fatalf("admission won race: status=%+v record=%+v", resolution, record)
			}
		} else if record.EvidenceCount != 1 || resolution.Status != "unavailable" {
			t.Fatalf("freeze won race: insert=%v status=%+v record=%+v", insertErr, resolution, record)
		}
	})

	t.Run("insert_vs_new_check", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		db, lock, seed, first, second, now := nativeRollbackFixture(t, ctx)
		defer db.Close()
		defer lock.Release()
		oldCheck := beginNativeRollbackCheck(t, ctx, db, lock, seed, first, now)
		row := trustObservationForTest(oldCheck, "relay-c", "operator-c", "agreed", now.Add(4*time.Second))
		nextPoint := chainPointForTest(second)
		start := make(chan struct{})
		var insertErr, beginErr error
		var nextCheck syncer.CheckIdentity
		var group sync.WaitGroup
		group.Add(2)
		go func() {
			defer group.Done()
			<-start
			insertErr = db.InsertPeerObservations(ctx, lock, []model.PeerObservation{row})
		}()
		go func() {
			defer group.Done()
			<-start
			nextCheck, beginErr = db.BeginTrustCheck(
				ctx, lock, &nextPoint, 2, now.Add(5*time.Second),
				seed.WriterID, "begin-race",
			)
		}()
		close(start)
		group.Wait()
		if beginErr != nil || nextCheck.ID == oldCheck.ID {
			t.Fatalf("new check=%+v err=%v", nextCheck, beginErr)
		}
		oldCommitment, err := db.readTrustEvidenceCommitment(ctx, oldCheck)
		if err != nil {
			t.Fatal(err)
		}
		if (insertErr == nil && oldCommitment.Count != 1) ||
			(insertErr != nil && oldCommitment.Count != 0) {
			t.Fatalf("insert=%v old commitment=%+v", insertErr, oldCommitment)
		}
		if err := db.InsertPeerObservations(ctx, lock, []model.PeerObservation{row}); err == nil {
			t.Fatal("superseded check accepted late evidence")
		}
	})
}

func TestNativeFrozenEvidenceRejectsLateAuthorityButAllowsDiagnostics(t *testing.T) {
	if os.Getenv("CLICKSYNC_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set CLICKSYNC_CLICKHOUSE_INTEGRATION=1 for isolated ClickHouse")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	db, lock, seed, _, second, now := nativeRollbackFixture(t, ctx)
	defer db.Close()
	defer lock.Release()
	check := beginNativeRollbackCheck(t, ctx, db, lock, seed, second, now)
	rows := []model.PeerObservation{
		trustObservationForTest(check, "relay-c", "operator-c", "agreed", now.Add(4*time.Second)),
		trustObservationForTest(check, "relay-d", "operator-d", "agreed", now.Add(4*time.Second)),
	}
	if err := db.InsertPeerObservations(ctx, lock, rows); err != nil {
		t.Fatal(err)
	}
	if err := db.conn.Exec(
		ctx,
		`INSERT INTO clicksync.peer_observations
SELECT * EXCEPT(operator_key) REPLACE(true AS parent_verified)
FROM clicksync.peer_observations
WHERE observation_id = ?
LIMIT 1`,
		uuid.UUID(rows[0].ID),
	); err == nil {
		t.Fatal("native proof CHECK accepted one-bit parent verification forgery")
	}
	if _, err := db.FinalizeTrustCheck(
		ctx, lock, check, false, "", now.Add(5*time.Second),
		seed.WriterID, "late-evidence",
	); err != nil {
		t.Fatal(err)
	}
	before, err := db.readTrustEvidenceCommitment(ctx, check)
	if err != nil {
		t.Fatal(err)
	}
	for index, result := range []string{"agreed", "disagreed"} {
		late := trustObservationForTest(
			check,
			fmt.Sprintf("relay-late-%d", index),
			fmt.Sprintf("operator-late-%d", index),
			result,
			now.Add(time.Duration(6+index)*time.Second),
		)
		if err := db.InsertPeerObservations(ctx, lock, []model.PeerObservation{late}); err == nil {
			t.Fatalf("frozen check accepted late %s", result)
		}
	}
	diagnostic := trustObservationForTest(
		check, "relay-diagnostic", "operator-diagnostic", "unavailable",
		now.Add(8*time.Second),
	)
	diagnostic.Kind = "source_change"
	diagnostic.ProofMethod = syncer.ObservationProofNone
	diagnostic.PointVerified = false
	if err := model.FinalizePeerObservationIdentity(&diagnostic); err != nil {
		t.Fatal(err)
	}
	if err := db.InsertPeerObservations(ctx, lock, []model.PeerObservation{diagnostic}); err != nil {
		t.Fatal(err)
	}
	after, err := db.readTrustEvidenceCommitment(ctx, check)
	if err != nil || after != before {
		t.Fatalf("diagnostic changed frozen C: before=%+v after=%+v err=%v", before, after, err)
	}
}

func TestNativeEvidenceOrdinalCorruptionFailsClosed(t *testing.T) {
	if os.Getenv("CLICKSYNC_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set CLICKSYNC_CLICKHOUSE_INTEGRATION=1 for isolated ClickHouse")
	}
	for _, test := range []struct {
		name    string
		prepare func(*testing.T, context.Context, *DB, publication.Lock, syncer.CheckIdentity, time.Time)
	}{
		{
			name: "gap",
			prepare: func(
				t *testing.T,
				ctx context.Context,
				db *DB,
				_ publication.Lock,
				check syncer.CheckIdentity,
				_ time.Time,
			) {
				if err := db.conn.Exec(ctx, `
INSERT INTO clicksync.peer_observations
SELECT * EXCEPT(operator_key) REPLACE(toUInt32(3) AS evidence_ordinal)
FROM clicksync.peer_observations
WHERE check_id = ?
ORDER BY evidence_ordinal
LIMIT 1`, uuid.UUID(check.ID)); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "conflicting_same_ordinal",
			prepare: func(
				t *testing.T,
				ctx context.Context,
				db *DB,
				lock publication.Lock,
				check syncer.CheckIdentity,
				now time.Time,
			) {
				second := trustObservationForTest(
					check, "relay-d", "operator-d", "agreed", now.Add(5*time.Second),
				)
				if err := db.InsertPeerObservations(ctx, lock, []model.PeerObservation{second}); err != nil {
					t.Fatal(err)
				}
				if err := db.conn.Exec(ctx, `
INSERT INTO clicksync.peer_observations
SELECT * EXCEPT(operator_key) REPLACE(toUInt32(1) AS evidence_ordinal)
FROM clicksync.peer_observations
WHERE observation_id = ?`, uuid.UUID(second.ID)); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "ninth_identical_replay",
			prepare: func(
				t *testing.T,
				ctx context.Context,
				db *DB,
				_ publication.Lock,
				check syncer.CheckIdentity,
				_ time.Time,
			) {
				for range 8 {
					if err := db.conn.Exec(ctx, `
INSERT INTO clicksync.peer_observations
SELECT * EXCEPT(operator_key)
FROM clicksync.peer_observations
WHERE check_id = ?
ORDER BY evidence_ordinal
LIMIT 1`, uuid.UUID(check.ID)); err != nil {
						t.Fatal(err)
					}
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			db, lock, seed, _, second, now := nativeRollbackFixture(t, ctx)
			defer db.Close()
			defer lock.Release()
			check := beginNativeRollbackCheck(t, ctx, db, lock, seed, second, now)
			first := trustObservationForTest(
				check, "relay-c", "operator-c", "agreed", now.Add(4*time.Second),
			)
			if err := db.InsertPeerObservations(ctx, lock, []model.PeerObservation{first}); err != nil {
				t.Fatal(err)
			}
			test.prepare(t, ctx, db, lock, check, now)
			if _, err := db.readTrustEvidenceCommitment(ctx, check); err == nil {
				t.Fatal("corrupt evidence ordinal/replay set was accepted")
			}
		})
	}
}

func TestNativeTrustThresholdMatrix(t *testing.T) {
	if os.Getenv("CLICKSYNC_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set CLICKSYNC_CLICKHOUSE_INTEGRATION=1 for isolated ClickHouse")
	}
	for _, test := range []struct {
		name         string
		results      []string
		status       string
		confirmed    uint16
		expectedSafe bool
	}{
		{
			name:         "three_agreed",
			results:      []string{"agreed", "agreed", "agreed"},
			status:       "agreed",
			confirmed:    3,
			expectedSafe: true,
		},
		{
			name:         "two_agreed_one_unavailable",
			results:      []string{"agreed", "agreed", "unavailable"},
			status:       "agreed",
			confirmed:    2,
			expectedSafe: true,
		},
		{
			name:         "one_agreed_two_unavailable",
			results:      []string{"agreed", "unavailable", "unavailable"},
			status:       "unavailable",
			confirmed:    1,
			expectedSafe: true,
		},
		{
			name:         "disagreement_overrides_threshold",
			results:      []string{"agreed", "agreed", "disagreed"},
			status:       "disputed",
			confirmed:    2,
			expectedSafe: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			db, lock, seed, _, second, now := nativeRollbackFixture(t, ctx)
			defer db.Close()
			defer lock.Release()

			point := chainPointForTest(second)
			check, err := db.BeginTrustCheck(
				ctx,
				lock,
				&point,
				2,
				now.Add(3*time.Second),
				seed.WriterID,
				"threshold-matrix",
			)
			if err != nil {
				t.Fatal(err)
			}
			rows := make([]model.PeerObservation, 0, len(test.results))
			for index, result := range test.results {
				rows = append(rows, trustObservationForTest(
					check,
					fmt.Sprintf("relay-%d", index+1),
					fmt.Sprintf("operator-%d", index+1),
					result,
					now.Add(time.Duration(4+index)*time.Second),
				))
			}
			if err := db.InsertPeerObservations(ctx, lock, rows); err != nil {
				t.Fatal(err)
			}
			resolution, err := db.FinalizeTrustCheck(
				ctx,
				lock,
				check,
				false,
				"",
				now.Add(8*time.Second),
				seed.WriterID,
				"threshold-matrix",
			)
			if err != nil {
				t.Fatal(err)
			}
			if resolution.Status != test.status ||
				resolution.Confirmed != test.confirmed ||
				resolution.Required != 2 ||
				resolution.Servable != test.expectedSafe {
				t.Fatalf("resolution = %+v", resolution)
			}
		})
	}
}

func TestNativeAuthoritativeArtifactValidationFailsClosed(t *testing.T) {
	if os.Getenv("CLICKSYNC_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set CLICKSYNC_CLICKHOUSE_INTEGRATION=1 for isolated ClickHouse")
	}
	t.Run("invalidation_at_current_adoption", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		db, lock, seed, first, _, now := nativeRollbackFixture(t, ctx)
		defer db.Close()
		defer lock.Release()
		commit := publication.RollbackCommit{
			RollbackID: id16(0xd1),
			EventSeq:   2,
			WriterID:   seed.WriterID,
			RecordedAt: now.Add(4 * time.Second),
		}
		if err := db.InsertInvalidations(ctx, commit, []publication.Descendant{{
			PublicationID: 1,
			Point:         first,
		}}); err != nil {
			t.Fatal(err)
		}
		if _, _, err := db.DatasetStatus(ctx); err == nil {
			t.Fatal("status served an adoption event with same-event invalidation")
		}
	})

	t.Run("adoption_at_pending_rollback_or_later", func(t *testing.T) {
		for _, offset := range []uint64{0, 1} {
			t.Run(fmt.Sprintf("offset_%d", offset), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
				defer cancel()
				db, lock, seed, first, second, now := nativeRollbackFixture(t, ctx)
				defer db.Close()
				defer lock.Release()
				_ = reserveNativeRollbackForTest(t, ctx, db, lock, seed, first, second, now)
				insertNativeAdoption(
					t, ctx, db, 3+offset, 2, second, seed.WriterID, now.Add(6*time.Second),
				)
				if _, _, err := db.DatasetStatus(ctx); err == nil {
					t.Fatal("status served pending rollback with conflicting adoption")
				}
			})
		}
	})

	t.Run("missing_finalized_invalidation", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		db, lock, seed, first, second, now := nativeRollbackFixture(t, ctx)
		defer db.Close()
		defer lock.Release()
		commit := reserveNativeRollbackForTest(t, ctx, db, lock, seed, first, second, now)
		if err := db.MarkRollbackInvalidations(ctx, lock, commit, "artifact-test"); err != nil {
			t.Fatal(err)
		}
		if err := db.InsertRollbackHeader(ctx, commit); err != nil {
			t.Fatal(err)
		}
		if err := db.FinalizeRollbackManifest(ctx, lock, commit, "artifact-test"); err != nil {
			t.Fatal(err)
		}
		if _, _, err := db.DatasetStatus(ctx); err == nil {
			t.Fatal("status served finalized rollback with missing invalidations")
		}
	})

	t.Run("extra_finalized_invalidation", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		db, lock, seed, first, second, now := nativeRollbackFixture(t, ctx)
		defer db.Close()
		defer lock.Release()
		commit := reserveNativeRollbackForTest(t, ctx, db, lock, seed, first, second, now)
		descendants := []publication.Descendant{{PublicationID: 2, Point: second}}
		if err := db.InsertInvalidations(ctx, commit, descendants); err != nil {
			t.Fatal(err)
		}
		if err := db.MarkRollbackInvalidations(ctx, lock, commit, "artifact-test"); err != nil {
			t.Fatal(err)
		}
		if err := db.InsertRollbackHeader(ctx, commit); err != nil {
			t.Fatal(err)
		}
		if err := db.FinalizeRollbackManifest(ctx, lock, commit, "artifact-test"); err != nil {
			t.Fatal(err)
		}
		if err := db.conn.Exec(ctx, `
INSERT INTO clicksync.chain_events
SELECT * REPLACE
(
    toUInt64(1) AS publication_id,
    unhex(?) AS block_hash,
    toUInt64(?) AS slot,
    toUInt64(?) AS block_number,
    ? AS is_byron_ebb
)
FROM clicksync.chain_events
WHERE event_kind = 'invalidation'
  AND event_seq = ?
LIMIT 1`,
			fmt.Sprintf("%x", first.Hash),
			first.Slot,
			first.BlockNumber,
			first.IsByronEBB,
			commit.EventSeq,
		); err != nil {
			t.Fatal(err)
		}
		if _, _, err := db.DatasetStatus(ctx); err == nil {
			t.Fatal("status served finalized rollback with extra same-event invalidation")
		}
	})
}

func reserveNativeRollbackForTest(
	t *testing.T,
	ctx context.Context,
	db *DB,
	lock publication.Lock,
	seed ManifestSeed,
	first, second publication.Point,
	now time.Time,
) publication.RollbackCommit {
	t.Helper()
	check := beginNativeRollbackCheck(t, ctx, db, lock, seed, first, now)
	if err := db.InsertPeerObservations(ctx, lock, []model.PeerObservation{
		trustObservationForTest(check, "relay-a", "operator-a", "agreed", now.Add(4*time.Second)),
		trustObservationForTest(check, "relay-b", "operator-b", "agreed", now.Add(4*time.Second)),
	}); err != nil {
		t.Fatal(err)
	}
	commit := rollbackCommitForTest(check, seed.WriterID, first, second, now)
	commit, err := db.ReserveRollbackManifest(ctx, lock, commit, "artifact-test")
	if err != nil {
		t.Fatal(err)
	}
	return commit
}

func beginNativeRollbackCheck(
	t *testing.T,
	ctx context.Context,
	db *DB,
	lock publication.Lock,
	seed ManifestSeed,
	target publication.Point,
	now time.Time,
) syncer.CheckIdentity {
	t.Helper()
	point := chainPointForTest(target)
	check, err := db.BeginTrustCheck(
		ctx,
		lock,
		&point,
		2,
		now.Add(3*time.Second),
		seed.WriterID,
		"trust-integrity-test",
	)
	if err != nil {
		t.Fatal(err)
	}
	return check
}

func rollbackCommitForTest(
	check syncer.CheckIdentity,
	writer [16]byte,
	target, oldTip publication.Point,
	now time.Time,
) publication.RollbackCommit {
	return publication.RollbackCommit{
		RollbackID:            id16(0xc8),
		EventSeq:              3,
		To:                    target,
		OldTip:                oldTip,
		OldEventSeq:           2,
		Depth:                 1,
		Reason:                "canonical observer set",
		ObservedPeers:         []string{"relay-a", "relay-b"},
		ObservedOperators:     []string{"operator-a", "operator-b"},
		CorroborationRequired: 2,
		CheckID:               copyIDPointer(check.ID),
		AgreementGroup:        copyIDPointer(check.AgreementGroup),
		CheckAttempt:          check.Attempt,
		CheckedEventSeq:       check.CheckedEventSeq,
		WriterID:              writer,
		RecordedAt:            now.Add(5 * time.Second),
	}
}

func nativeRollbackFixture(
	t *testing.T,
	ctx context.Context,
) (*DB, *writerlock.Lock, ManifestSeed, publication.Point, publication.Point, time.Time) {
	t.Helper()
	db, err := Open(config.Config{
		ClickHouseHost:     "127.0.0.1",
		ClickHousePort:     19100,
		ClickHouseUser:     "default",
		ClickHousePassword: "integration-only",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.conn.Exec(ctx, `DROP DATABASE IF EXISTS clicksync`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Migrate(ctx); err != nil {
		db.Close()
		t.Fatal(err)
	}
	lock, err := writerlock.Acquire(
		filepath.Join(t.TempDir(), "writer.lock"),
		"single-host-flock",
	)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 23, 20, 0, 0, 123456000, time.UTC)
	byronID, byronJSON, shelleyID, shelleyJSON := MainnetGenesisIdentity()
	boundary := publication.Point{
		Slot:        10,
		Hash:        hash32Fill(0xa0),
		BlockNumber: 1,
	}
	seed := ManifestSeed{
		NetworkMagic:           mainnetMagic,
		NetworkName:            "mainnet",
		ByronGenesisID:         byronID,
		ByronGenesisJSONHash:   byronJSON,
		ShelleyGenesisID:       shelleyID,
		ShelleyGenesisJSONHash: shelleyJSON,
		Start:                  boundary,
		WriterID:               id16(0xb0),
		WriterBuild:            "recovery-test",
		SourceBuild:            "recovery-test",
		CreatedAt:              now,
	}
	if _, err := db.LoadOrCreateManifest(ctx, lock, seed); err != nil {
		lock.Release()
		db.Close()
		t.Fatal(err)
	}
	boundaryCheckPoint := chainPointForTest(boundary)
	check, err := db.BeginTrustCheck(
		ctx,
		lock,
		&boundaryCheckPoint,
		2,
		now.Add(time.Second),
		seed.WriterID,
		"recovery-test",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.InsertPeerObservations(ctx, lock, []model.PeerObservation{
		trustObservationForTest(check, "relay-a", "operator-a", "agreed", now.Add(time.Second)),
		trustObservationForTest(check, "relay-b", "operator-b", "agreed", now.Add(time.Second)),
	}); err != nil {
		t.Fatal(err)
	}
	if resolution, err := db.FinalizeTrustCheck(
		ctx,
		lock,
		check,
		false,
		"",
		now.Add(time.Second),
		seed.WriterID,
		"recovery-test",
	); err != nil || resolution.Status != "agreed" {
		t.Fatalf("boundary resolution=%+v err=%v", resolution, err)
	}
	first := publication.Point{
		Slot:        11,
		Hash:        hash32Fill(0xa1),
		BlockNumber: 2,
	}
	second := publication.Point{
		Slot:        12,
		Hash:        hash32Fill(0xa2),
		BlockNumber: 3,
	}
	insertNativeBlock(t, ctx, db, 1, first, &boundary.Hash, seed.WriterID, now)
	insertNativeBlock(t, ctx, db, 2, second, &first.Hash, seed.WriterID, now)
	insertNativeAdoption(t, ctx, db, 1, 1, first, seed.WriterID, now)
	if err := db.PersistManifest(ctx, lock, publication.ManifestUpdate{
		EventSeq:        1,
		Tip:             first,
		Kind:            publication.ManifestAdoption,
		RemoteAdoptions: 1,
		WriterID:        seed.WriterID,
		WriterBuild:     "recovery-test",
		UpdatedAt:       now.Add(2 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	insertNativeAdoption(t, ctx, db, 2, 2, second, seed.WriterID, now)
	if err := db.PersistManifest(ctx, lock, publication.ManifestUpdate{
		EventSeq:        2,
		Tip:             second,
		Kind:            publication.ManifestAdoption,
		RemoteAdoptions: 1,
		WriterID:        seed.WriterID,
		WriterBuild:     "recovery-test",
		UpdatedAt:       now.Add(2 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	return db, lock, seed, first, second, now
}

func insertNativeBlock(
	t *testing.T,
	ctx context.Context,
	db *DB,
	publicationID uint64,
	point publication.Point,
	parent *model.Hash32,
	writer [16]byte,
	at time.Time,
) {
	t.Helper()
	var parentValue any
	if parent != nil {
		parentValue = bytesOf32(*parent)
	}
	batch, err := db.conn.PrepareBatch(
		ctx,
		`INSERT INTO clicksync.blocks
(
    publication_id, block_hash, parent_hash, slot, block_number, era, block_type,
    body_hash_verified, transaction_hashes_verified, facts_digest, writer_id,
    observed_at, inserted_at
)
`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Append(
		publicationID,
		bytesOf32(point.Hash),
		parentValue,
		point.Slot,
		point.BlockNumber,
		"Shelley",
		int16(2),
		true,
		true,
		bytesOf32(hash32Fill(byte(0xd0+publicationID))),
		uuid.UUID(writer),
		at,
		at,
	); err != nil {
		_ = batch.Abort()
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

func insertNativeAdoption(
	t *testing.T,
	ctx context.Context,
	db *DB,
	eventSeq, publicationID uint64,
	point publication.Point,
	writer [16]byte,
	at time.Time,
) {
	t.Helper()
	batch, err := db.conn.PrepareBatch(
		ctx,
		`INSERT INTO clicksync.chain_events
(
    event_seq, publication_id, event_kind, active, rollback_id, block_hash,
    slot, block_number, is_byron_ebb, writer_id, recorded_at
)
`,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Append(
		eventSeq,
		publicationID,
		"adoption",
		true,
		nil,
		bytesOf32(point.Hash),
		point.Slot,
		point.BlockNumber,
		point.IsByronEBB,
		uuid.UUID(writer),
		at,
	); err != nil {
		_ = batch.Abort()
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

func trustObservationForTest(
	check syncer.CheckIdentity,
	peer, operator, result string,
	at time.Time,
) model.PeerObservation {
	point := check.CheckedPoint
	slot := point.Point.Slot
	hash := model.Hash32{}
	copy(hash[:], point.Point.Hash)
	number := point.BlockNumber
	isByronEBB := point.IsByronEBB
	row := model.PeerObservation{
		Kind:                   "rollback",
		ProofMethod:            syncer.ObservationProofPairedChainSyncSingleton,
		PeerHost:               peer,
		PeerAddress:            fmt.Sprintf("192.0.2.%d:3001", len(operator)),
		Operator:               operator,
		N2NVersion:             15,
		NetworkMagic:           mainnetMagic,
		TipSlot:                slot,
		TipHash:                hash,
		TipBlockNumber:         number,
		CheckpointSlot:         &slot,
		CheckpointHash:         &hash,
		CheckpointBlockNumber:  &number,
		CheckpointIsByronEBB:   &isByronEBB,
		CheckID:                check.ID,
		AgreementGroup:         check.AgreementGroup,
		CheckAttempt:           check.Attempt,
		CorroborationRequired:  check.Required,
		CheckedEventSeq:        check.CheckedEventSeq,
		CheckedPointSlot:       &slot,
		CheckedPointHash:       &hash,
		CheckedBlockNumber:     &number,
		CheckedPointIsByronEBB: isByronEBB,
		PointVerified:          result == "agreed",
		Result:                 result,
		ObservedAt:             at,
	}
	if err := model.FinalizePeerObservationIdentity(&row); err != nil {
		panic(err)
	}
	return row
}

func chainPointForTest(point publication.Point) n2n.ChainPoint {
	if point.Origin {
		return n2n.NewChainPointOrigin()
	}
	wire := pcommon.NewPoint(point.Slot, point.Hash[:])
	if point.IsByronEBB {
		return n2n.NewByronEBBChainPoint(wire, point.BlockNumber)
	}
	return n2n.NewChainPoint(wire, point.BlockNumber)
}

func copyIDPointer(value [16]byte) *[16]byte {
	ret := value
	return &ret
}
