package clickhouse

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/clicksync-project/clickout/internal/cursor"
	"github.com/clicksync-project/clickout/internal/metrics"
	"github.com/clicksync-project/clickout/internal/model"
	"github.com/clicksync-project/clickout/internal/repository"
)

func TestContractReadsRollbackAndTrace(t *testing.T) {
	address := os.Getenv("CLICKOUT_TEST_CLICKHOUSE_ADDR")
	database := os.Getenv("CLICKOUT_TEST_CLICKHOUSE_DATABASE")
	if address == "" || database == "" {
		t.Skip("set CLICKOUT_TEST_CLICKHOUSE_ADDR and CLICKOUT_TEST_CLICKHOUSE_DATABASE")
	}
	username := os.Getenv("CLICKOUT_TEST_CLICKHOUSE_USERNAME")
	if username == "" {
		username = "default"
	}
	store, err := Open(Config{
		Addresses:    []string{address},
		Database:     database,
		Username:     username,
		Password:     os.Getenv("CLICKOUT_TEST_CLICKHOUSE_PASSWORD"),
		QueryTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	fixture := newFixture()
	initialAuthority := fixture.insert(t, store)
	fixture.insertUnpublishedAddressCandidate(t, store)

	snapshot, err := store.Snapshot(ctx, model.AtPoint{Tip: true})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.QueryHead.EventSeq != 2 ||
		snapshot.Cutoff.PublicationID != fixture.pub2 ||
		snapshot.VisibilityGeneration != 2 ||
		snapshot.Identity.CompleteHistory ||
		snapshot.Identity.TrustMode != model.TrustPeerObserved {
		t.Fatalf("unexpected snapshot: %#v", snapshot)
	}
	validated, err := store.ValidateSnapshotBeforeRead(ctx, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.SamePin(validated) {
		t.Fatalf("initial pre-read validation changed snapshot pin: %#v", validated)
	}
	snapshot = validated
	sourceState, boundaries, err := store.UTxO(ctx, snapshot, fixture.source)
	if err != nil || len(boundaries) != 0 || sourceState.IsCurrent ||
		sourceState.SpentBy == nil || *sourceState.SpentBy != fixture.txB ||
		len(sourceState.Uses) != 2 ||
		sourceState.Output.PaymentCredentialKind != "script" ||
		string(sourceState.Output.PaymentCredentialHash) != string(fixture.policy[:]) {
		t.Fatalf("unexpected source state: %#v, %#v, %v", sourceState, boundaries, err)
	}
	transactionCollector := &metrics.Collector{}
	transactionCtx := metrics.WithCollector(ctx, transactionCollector)
	transaction, boundaries, err := store.Transaction(transactionCtx, snapshot, fixture.txB)
	if err != nil || len(boundaries) != 0 || len(transaction.Inputs) != 1 ||
		len(transaction.Outputs) != 2 || transaction.Mint[0].Quantity != 7 ||
		transaction.Inputs[0].SourceOutput == nil ||
		string(transaction.Outputs[0].InlineDatumCBOR) != string(fixture.datumCBOR) ||
		string(transaction.Outputs[1].InlineDatumCBOR) != string(fixture.datumCBOR) {
		t.Fatalf("unexpected transaction: %#v, %#v, %v", transaction, boundaries, err)
	}
	inlineQueries := 0
	for _, query := range transactionCollector.Snapshot() {
		if query.Name == "inline_datums_batch" {
			inlineQueries++
		}
	}
	if inlineQueries != 1 {
		t.Fatalf("two inline outputs used %d datum body queries, want one", inlineQueries)
	}
	pageCollector := &metrics.Collector{}
	pageCtx := metrics.WithCollector(ctx, pageCollector)
	page, _, err := store.Address(pageCtx, snapshot, repository.AddressQuery{
		Address: fixture.addressA,
		State:   "history",
		Limit:   1,
	})
	if err != nil || len(page.Items) != 0 || page.Cursor == "" {
		t.Fatalf("unpublished physical window was not an empty resumable page: %#v, %v", page, err)
	}
	for _, query := range pageCollector.Snapshot() {
		if query.Name == "address_outputs" || query.Name == "inline_datums_batch" {
			t.Fatalf("inactive candidate was hydrated by %s", query.Name)
		}
	}
	resume, err := cursor.Decode(
		page.Cursor,
		addressScope(fixture.addressA, "history"),
	)
	if err != nil {
		t.Fatal(err)
	}
	page, _, err = store.Address(ctx, snapshot, repository.AddressQuery{
		Address: fixture.addressA,
		State:   "history",
		Limit:   1,
		LastKey: resume.LastKey,
	})
	if err != nil || len(page.Items) != 1 || page.Items[0].Output.Ref != fixture.source ||
		page.Cursor == "" {
		t.Fatalf("live output after inactive window was not resumable: %#v, %v", page, err)
	}
	datum, err := store.Datum(ctx, snapshot, fixture.datumHash)
	if err != nil || !datum.BodyVerified || len(datum.ActiveObservations) != 2 ||
		string(datum.BodyCBOR) != string(fixture.datumCBOR) {
		t.Fatalf("unexpected datum: %#v, %v", datum, err)
	}
	redeemers, boundaries, err := store.Redeemers(ctx, snapshot, fixture.txB)
	if err != nil || len(boundaries) != 0 || len(redeemers) != 1 ||
		redeemers[0].Target.SourceOutput == nil ||
		redeemers[0].Target.SourceOutput.PaymentCredentialKind != "script" {
		t.Fatalf("unexpected redeemers: %#v, %#v, %v", redeemers, boundaries, err)
	}
	metadata, err := store.Metadata(ctx, snapshot, fixture.txB)
	if err != nil || string(metadata.MapCBOR) != string(fixture.metadataCBOR) {
		t.Fatalf("unexpected metadata: %#v, %v", metadata, err)
	}
	withdrawals, err := store.Withdrawals(ctx, snapshot, fixture.txB)
	if err != nil || len(withdrawals) != 1 || !withdrawals[0].Applied {
		t.Fatalf("unexpected withdrawals: %#v, %v", withdrawals, err)
	}
	forwardResult, boundaries, err := store.ExpandForward(
		ctx,
		snapshot,
		[]model.UTxORef{fixture.source},
		model.AssetSelector{ADA: true},
		repository.ExpansionBudget{MaxEdges: 100, MaxNodes: 1000},
	)
	forward := forwardResult.Hyperedges
	if err != nil || len(boundaries) != 0 || len(forward) != 1 ||
		forward[0].Transaction != fixture.txB || len(forward[0].ProducedOutputs) != 2 ||
		len(forward[0].AppliedWithdrawals) != 1 || forward[0].FeeSink == nil {
		t.Fatalf("unexpected forward edge: %#v, %#v, %v", forward, boundaries, err)
	}
	reverseResult, boundaries, err := store.ExpandReverse(
		ctx,
		snapshot,
		[]model.UTxORef{fixture.destination},
		model.AssetSelector{ADA: true},
		repository.ExpansionBudget{MaxEdges: 100, MaxNodes: 1000},
	)
	reverse := reverseResult.Hyperedges
	if err != nil || len(boundaries) != 0 || len(reverse) != 1 ||
		len(reverse[0].ConsumedInputValues) != 1 ||
		reverse[0].ConsumedInputValues[0].Ref != fixture.source {
		t.Fatalf("unexpected reverse edge: %#v, %#v, %v", reverse, boundaries, err)
	}
	fanInResult, boundaries, err := store.ExpandReverse(
		ctx,
		snapshot,
		[]model.UTxORef{
			fixture.destination,
			{TxHash: fixture.txB, Index: 1},
		},
		model.AssetSelector{ADA: true},
		repository.ExpansionBudget{MaxEdges: 1, MaxNodes: 1000},
	)
	if err != nil || len(boundaries) != 0 || fanInResult.Truncated ||
		len(fanInResult.Hyperedges) != 1 ||
		fanInResult.Hyperedges[0].Transaction != fixture.txB {
		t.Fatalf("reverse fan-in did not deduplicate candidate transaction: %#v, %#v, %v", fanInResult, boundaries, err)
	}
	invalidTx, _, err := store.Transaction(ctx, snapshot, fixture.txC)
	if err != nil || invalidTx.Phase2Valid || invalidTx.MintApplied ||
		len(invalidTx.Inputs) != 2 || invalidTx.Inputs[0].IsConsumed ||
		!invalidTx.Inputs[1].IsConsumed || !invalidTx.Inputs[1].SourceResolved {
		t.Fatalf("invalid transaction semantics wrong: %#v, %v", invalidTx, err)
	}
	invalidResult, _, err := store.ExpandForward(
		ctx,
		snapshot,
		[]model.UTxORef{fixture.collateral},
		model.AssetSelector{ADA: true},
		repository.ExpansionBudget{MaxEdges: 100, MaxNodes: 1000},
	)
	invalidEdges := invalidResult.Hyperedges
	if err != nil || len(invalidEdges) != 1 || len(invalidEdges[0].MintDeltas) != 0 ||
		len(invalidEdges[0].AppliedWithdrawals) != 0 ||
		invalidEdges[0].FeeSink == nil || invalidEdges[0].FeeSink.Lovelace != 300000 ||
		len(invalidEdges[0].ConsumedInputs) != 1 ||
		invalidEdges[0].ConsumedInputs[0] != fixture.collateral ||
		len(invalidEdges[0].ProducedOutputs) != 1 ||
		invalidEdges[0].ProducedOutputs[0].Kind != model.OutputCollateralReturn {
		t.Fatalf("invalid collateral hyperedge wrong: %#v, %v", invalidEdges, err)
	}

	initialLease, err := store.FinishSnapshot(ctx, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.SamePin(initialLease) {
		t.Fatalf("initial Finish changed snapshot pin: %#v", initialLease)
	}
	reservedAuthority := fixture.insertPendingRollbackReservation(
		t,
		store,
		initialAuthority,
	)
	if _, err := store.FinishSnapshot(ctx, initialLease); !errors.Is(
		err,
		ErrSnapshotUnavailable,
	) {
		t.Fatalf("old lease survived pending generation change: %v", err)
	}
	snapshot, err = store.Snapshot(ctx, model.AtPoint{Tip: true})
	if err != nil ||
		snapshot.QueryHead.EventSeq != 2 ||
		snapshot.AuthorityEffective.EventSeq != 2 ||
		snapshot.VisibilityGeneration != 3 ||
		snapshot.Diagnostics.TrustStatus != "checking" {
		t.Fatalf("reserved rollback changed visible head: %#v, %v", snapshot, err)
	}
	validated, err = store.ValidateSnapshotBeforeRead(ctx, snapshot)
	if err != nil || !snapshot.SamePin(validated) {
		t.Fatalf("reserved pre-read validation failed: %#v, %v", validated, err)
	}
	snapshot = validated
	finishedReserved, err := store.FinishSnapshot(ctx, snapshot)
	if err != nil || !snapshot.SamePin(finishedReserved) {
		t.Fatalf("reserved Finish failed: %#v, %v", finishedReserved, err)
	}
	writtenAuthority := fixture.insertPendingRollbackInvalidations(
		t,
		store,
		reservedAuthority,
	)
	snapshot, err = store.Snapshot(ctx, model.AtPoint{Tip: true})
	if err != nil ||
		snapshot.QueryHead.EventSeq != 2 ||
		snapshot.AuthorityEffective.EventSeq != 2 ||
		snapshot.VisibilityGeneration != 3 ||
		snapshot.Diagnostics.TrustStatus != "checking" {
		t.Fatalf(
			"invalidations-written rollback changed visible head: %#v, %v",
			snapshot,
			err,
		)
	}
	validated, err = store.ValidateSnapshotBeforeRead(ctx, snapshot)
	if err != nil || !snapshot.SamePin(validated) {
		t.Fatalf(
			"invalidations-written pre-read validation failed: %#v, %v",
			validated,
			err,
		)
	}
	snapshot = validated
	finishedWritten, err := store.FinishSnapshot(ctx, snapshot)
	if err != nil || !snapshot.SamePin(finishedWritten) {
		t.Fatalf(
			"invalidations-written Finish failed: %#v, %v",
			finishedWritten,
			err,
		)
	}
	fixture.insertRollbackHeader(t, store, writtenAuthority)
	headerWindow, err := store.Snapshot(ctx, model.AtPoint{Tip: true})
	if err != nil ||
		headerWindow.QueryHead.EventSeq != 2 ||
		headerWindow.AuthorityEffective.EventSeq != 2 ||
		headerWindow.VisibilityGeneration != 3 ||
		headerWindow.Diagnostics.TrustStatus != "checking" {
		t.Fatalf("header window changed visible head: %#v, %v", headerWindow, err)
	}
	validatedHeader, err := store.ValidateSnapshotBeforeRead(ctx, headerWindow)
	if err != nil || !headerWindow.SamePin(validatedHeader) {
		t.Fatalf(
			"header-window pre-read validation failed: %#v, %v",
			validatedHeader,
			err,
		)
	}
	headerWindow = validatedHeader
	headerLease, err := store.FinishSnapshot(ctx, headerWindow)
	if err != nil || !headerWindow.SamePin(headerLease) {
		t.Fatalf("header-window Finish failed: %#v, %v", headerLease, err)
	}
	finalizedAuthority := canonicalIntegrationFinalizedRollback(
		t,
		writtenAuthority,
	)
	insertCanonicalAuthorityRevision(t, store, finalizedAuthority, nil)
	if _, err := store.FinishSnapshot(ctx, headerLease); !errors.Is(
		err,
		ErrSnapshotUnavailable,
	) {
		t.Fatalf("header-window lease survived finalization: %v", err)
	}
	snapshot, err = store.Snapshot(ctx, model.AtPoint{Tip: true})
	if err != nil ||
		snapshot.QueryHead.EventSeq != 3 ||
		snapshot.AuthorityEffective.EventSeq != 3 ||
		snapshot.VisibilityGeneration != 3 ||
		snapshot.Diagnostics.TrustStatus != "agreed" {
		t.Fatalf("finalized rollback did not advance snapshot: %#v, %v", snapshot, err)
	}
	validated, err = store.ValidateSnapshotBeforeRead(ctx, snapshot)
	if err != nil || !snapshot.SamePin(validated) {
		t.Fatalf("finalized pre-read validation failed: %#v, %v", validated, err)
	}
	snapshot = validated
	sourceState, _, err = store.UTxO(ctx, snapshot, fixture.source)
	if err != nil || !sourceState.IsCurrent {
		t.Fatalf("rollback did not resurrect source: %#v, %v", sourceState, err)
	}
	if _, _, err := store.Transaction(ctx, snapshot, fixture.txB); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rolled-back transaction remained visible: %v", err)
	}
	datum, err = store.Datum(ctx, snapshot, fixture.datumHash)
	if err != nil || !datum.BodyVerified || len(datum.ActiveObservations) != 0 {
		t.Fatalf("datum provenance did not separate body from active observation: %#v, %v", datum, err)
	}
	finishedFinalized, err := store.FinishSnapshot(ctx, snapshot)
	if err != nil || !snapshot.SamePin(finishedFinalized) {
		t.Fatalf("finalized Finish failed: %#v, %v", finishedFinalized, err)
	}

	readoptedAuthority := fixture.freshReadopt(t, store, finalizedAuthority)
	if readoptedAuthority.Revision != 8 {
		t.Fatalf("unexpected re-adoption revision: %#v", readoptedAuthority)
	}
	refreshedFinalized, err := store.FinishSnapshot(ctx, finishedFinalized)
	if err != nil || !finishedFinalized.SamePin(refreshedFinalized) ||
		refreshedFinalized.Diagnostics.Physical.EventSeq != 6 {
		t.Fatalf(
			"append-only re-adoption invalidated finalized lease: %#v, %v",
			refreshedFinalized,
			err,
		)
	}
	snapshot, err = store.Snapshot(ctx, model.AtPoint{Tip: true})
	if err != nil || snapshot.QueryHead.EventSeq != 6 ||
		snapshot.Cutoff.PublicationID != fixture.pub3 ||
		snapshot.VisibilityGeneration != 3 {
		t.Fatalf("re-adoption failed: %#v, %v", snapshot, err)
	}
	validated, err = store.ValidateSnapshotBeforeRead(ctx, snapshot)
	if err != nil || !snapshot.SamePin(validated) {
		t.Fatalf("re-adoption pre-read validation failed: %#v, %v", validated, err)
	}
	snapshot = validated
	sourceState, _, err = store.UTxO(ctx, snapshot, fixture.source)
	if err != nil || sourceState.IsCurrent {
		t.Fatalf("re-adoption did not restore spend: %#v, %v", sourceState, err)
	}
	transaction, boundaries, err = store.Transaction(ctx, snapshot, fixture.txB)
	if err != nil || len(boundaries) != 0 ||
		len(transaction.Inputs) != 1 ||
		len(transaction.Outputs) != 2 {
		t.Fatalf("re-adopted transaction context failed: %#v, %#v, %v", transaction, boundaries, err)
	}
	datum, err = store.Datum(ctx, snapshot, fixture.datumHash)
	if err != nil || !datum.BodyVerified || len(datum.ActiveObservations) != 2 {
		t.Fatalf("re-adopted datum context failed: %#v, %v", datum, err)
	}
	redeemers, boundaries, err = store.Redeemers(ctx, snapshot, fixture.txB)
	if err != nil || len(boundaries) != 0 || len(redeemers) != 1 ||
		redeemers[0].Target.SourceOutput == nil {
		t.Fatalf("re-adopted redeemer context failed: %#v, %#v, %v", redeemers, boundaries, err)
	}
	metadata, err = store.Metadata(ctx, snapshot, fixture.txB)
	if err != nil || string(metadata.MapCBOR) != string(fixture.metadataCBOR) {
		t.Fatalf("re-adopted metadata context failed: %#v, %v", metadata, err)
	}
	withdrawals, err = store.Withdrawals(ctx, snapshot, fixture.txB)
	if err != nil || len(withdrawals) != 1 || !withdrawals[0].Applied {
		t.Fatalf("re-adopted withdrawal context failed: %#v, %v", withdrawals, err)
	}
	finishedReadopted, err := store.FinishSnapshot(ctx, snapshot)
	if err != nil || !snapshot.SamePin(finishedReadopted) {
		t.Fatalf("re-adoption Finish failed: %#v, %v", finishedReadopted, err)
	}
	atBlock, err := store.Snapshot(ctx, model.AtPoint{BlockHash: &fixture.block2})
	if err != nil || atBlock.QueryHead.EventSeq != 6 ||
		atBlock.Cutoff.PublicationID != fixture.pub3 {
		t.Fatalf("complete AtBlock SQL did not select fresh re-adoption: %#v, %v", atBlock, err)
	}
	validatedAtBlock, err := store.ValidateSnapshotBeforeRead(ctx, atBlock)
	if err != nil || !atBlock.SamePin(validatedAtBlock) {
		t.Fatalf("AtBlock pre-read validation failed: %#v, %v", validatedAtBlock, err)
	}
	atBlock = validatedAtBlock
	reAdoptPage, _, err := store.Address(ctx, atBlock, repository.AddressQuery{
		Address: fixture.addressB,
		State:   "history",
		Limit:   1,
	})
	if err != nil || len(reAdoptPage.Items) != 0 || reAdoptPage.Cursor == "" {
		t.Fatalf("inactive old publication did not form an empty physical page: %#v, %v", reAdoptPage, err)
	}
	reAdoptResume, err := cursor.Decode(
		reAdoptPage.Cursor,
		addressScope(fixture.addressB, "history"),
	)
	if err != nil {
		t.Fatal(err)
	}
	reAdoptPage, _, err = store.Address(ctx, atBlock, repository.AddressQuery{
		Address: fixture.addressB,
		State:   "history",
		Limit:   1,
		LastKey: reAdoptResume.LastKey,
	})
	if err != nil || len(reAdoptPage.Items) != 1 ||
		reAdoptPage.Items[0].Output.Ref != fixture.destination {
		t.Fatalf("fresh publication was not selected across page boundary: %#v, %v", reAdoptPage, err)
	}
	finishedAtBlock, err := store.FinishSnapshot(ctx, atBlock)
	if err != nil || !atBlock.SamePin(finishedAtBlock) {
		t.Fatalf("AtBlock Finish failed: %#v, %v", finishedAtBlock, err)
	}
	noOpReservedAuthority := fixture.insertNoOpRollbackReservation(
		t,
		store,
		readoptedAuthority,
	)
	if noOpReservedAuthority.Revision != 9 {
		t.Fatalf("unexpected no-op reservation revision: %#v", noOpReservedAuthority)
	}
	for name, stale := range map[string]model.Snapshot{
		"tip":      finishedReadopted,
		"at_block": finishedAtBlock,
	} {
		if _, err := store.FinishSnapshot(ctx, stale); !errors.Is(
			err,
			ErrSnapshotUnavailable,
		) {
			t.Fatalf("%s rev8 lease survived no-op generation change: %v", name, err)
		}
	}
	snapshot, err = store.Snapshot(ctx, model.AtPoint{Tip: true})
	if err != nil ||
		snapshot.Diagnostics.Physical.EventSeq != 6 ||
		snapshot.QueryHead.EventSeq != 3 ||
		snapshot.AuthorityEffective.EventSeq != 3 ||
		snapshot.VisibilityGeneration != 4 ||
		snapshot.Diagnostics.TrustStatus != "checking" {
		t.Fatalf("no-op reservation snapshot is wrong: %#v, %v", snapshot, err)
	}
	validated, err = store.ValidateSnapshotBeforeRead(ctx, snapshot)
	if err != nil || !snapshot.SamePin(validated) {
		t.Fatalf("no-op reservation pre-read failed: %#v, %v", validated, err)
	}
	snapshot = validated
	finishedNoOpReserved, err := store.FinishSnapshot(ctx, snapshot)
	if err != nil || !snapshot.SamePin(finishedNoOpReserved) {
		t.Fatalf("no-op reservation Finish failed: %#v, %v", finishedNoOpReserved, err)
	}
	noOpWrittenAuthority := fixture.insertNoOpRollbackInvalidationsWritten(
		t,
		store,
		noOpReservedAuthority,
	)
	refreshedNoOpReserved, err := store.FinishSnapshot(
		ctx,
		finishedNoOpReserved,
	)
	if err != nil || !finishedNoOpReserved.SamePin(refreshedNoOpReserved) {
		t.Fatalf(
			"rev9 lease failed across same-gen rev10 progress: %#v, %v",
			refreshedNoOpReserved,
			err,
		)
	}
	snapshot, err = store.Snapshot(ctx, model.AtPoint{Tip: true})
	if err != nil ||
		snapshot.Diagnostics.Physical.EventSeq != 6 ||
		snapshot.QueryHead.EventSeq != 3 ||
		snapshot.AuthorityEffective.EventSeq != 3 ||
		snapshot.VisibilityGeneration != 4 ||
		snapshot.Diagnostics.TrustStatus != "checking" ||
		noOpWrittenAuthority.Revision != 10 {
		t.Fatalf("no-op invalidations-written snapshot is wrong: %#v, %v", snapshot, err)
	}
	validated, err = store.ValidateSnapshotBeforeRead(ctx, snapshot)
	if err != nil || !snapshot.SamePin(validated) {
		t.Fatalf(
			"no-op invalidations-written pre-read failed: %#v, %v",
			validated,
			err,
		)
	}
	snapshot = validated
	finishedNoOpWritten, err := store.FinishSnapshot(ctx, snapshot)
	if err != nil || !snapshot.SamePin(finishedNoOpWritten) {
		t.Fatalf(
			"no-op invalidations-written Finish failed: %#v, %v",
			finishedNoOpWritten,
			err,
		)
	}
	fixture.insertRollbackHeader(t, store, noOpWrittenAuthority)
	refreshedNoOpWritten, err := store.FinishSnapshot(ctx, finishedNoOpWritten)
	if err != nil || !finishedNoOpWritten.SamePin(refreshedNoOpWritten) {
		t.Fatalf(
			"rev10 lease failed across no-op header materialization: %#v, %v",
			refreshedNoOpWritten,
			err,
		)
	}
	snapshot, err = store.Snapshot(ctx, model.AtPoint{Tip: true})
	if err != nil ||
		snapshot.Diagnostics.Physical.EventSeq != 6 ||
		snapshot.QueryHead.EventSeq != 3 ||
		snapshot.AuthorityEffective.EventSeq != 3 ||
		snapshot.VisibilityGeneration != 4 ||
		snapshot.Diagnostics.TrustStatus != "checking" {
		t.Fatalf("no-op header-window snapshot is wrong: %#v, %v", snapshot, err)
	}
	validated, err = store.ValidateSnapshotBeforeRead(ctx, snapshot)
	if err != nil || !snapshot.SamePin(validated) {
		t.Fatalf("no-op header-window pre-read failed: %#v, %v", validated, err)
	}
	snapshot = validated
	finishedNoOpHeader, err := store.FinishSnapshot(ctx, snapshot)
	if err != nil || !snapshot.SamePin(finishedNoOpHeader) {
		t.Fatalf(
			"no-op header-window Finish failed: %#v, %v",
			finishedNoOpHeader,
			err,
		)
	}
	noOpFinalizedAuthority := canonicalIntegrationFinalizedRollback(
		t,
		noOpWrittenAuthority,
	)
	insertCanonicalAuthorityRevision(t, store, noOpFinalizedAuthority, nil)
	refreshedNoOpHeader, err := store.FinishSnapshot(ctx, finishedNoOpHeader)
	if err != nil ||
		!finishedNoOpHeader.SamePin(refreshedNoOpHeader) ||
		refreshedNoOpHeader.Diagnostics.Physical.EventSeq != 7 {
		t.Fatalf(
			"no-op finalization invalidated stable header-window lease: %#v, %v",
			refreshedNoOpHeader,
			err,
		)
	}
	snapshot, err = store.Snapshot(ctx, model.AtPoint{Tip: true})
	if err != nil ||
		snapshot.QueryHead.EventSeq != 7 ||
		snapshot.AuthorityEffective.EventSeq != 7 ||
		snapshot.Diagnostics.Physical.EventSeq != 7 ||
		snapshot.Cutoff.PublicationID != fixture.pub3 ||
		snapshot.VisibilityGeneration != 4 ||
		snapshot.Diagnostics.TrustStatus != "agreed" {
		t.Fatalf("no-op finalized snapshot is wrong: %#v, %v", snapshot, err)
	}
	validated, err = store.ValidateSnapshotBeforeRead(ctx, snapshot)
	if err != nil || !snapshot.SamePin(validated) {
		t.Fatalf("no-op finalized pre-read failed: %#v, %v", validated, err)
	}
	snapshot = validated
	sourceState, _, err = store.UTxO(ctx, snapshot, fixture.source)
	if err != nil || sourceState.IsCurrent {
		t.Fatalf("no-op rollback changed active membership: %#v, %v", sourceState, err)
	}
	transaction, boundaries, err = store.Transaction(ctx, snapshot, fixture.txB)
	if err != nil || len(boundaries) != 0 ||
		len(transaction.Inputs) != 1 ||
		len(transaction.Outputs) != 2 {
		t.Fatalf("no-op rollback hid active transaction: %#v, %#v, %v", transaction, boundaries, err)
	}
	finishedNoOpFinal, err := store.FinishSnapshot(ctx, snapshot)
	if err != nil || !snapshot.SamePin(finishedNoOpFinal) {
		t.Fatalf("no-op finalized Finish failed: %#v, %v", finishedNoOpFinal, err)
	}
	snapshot = finishedNoOpFinal
	pinned := finishedNoOpFinal
	if pinned.QueryHead.EventSeq != 7 ||
		pinned.Cutoff.PublicationID != fixture.pub3 {
		t.Fatalf("depth-zero rollback lease was not retained: %#v", pinned)
	}
	fixture.insertPostWatermarkShadow(t, store)
	sourceState, _, err = store.UTxO(ctx, pinned, fixture.source)
	if err != nil || sourceState.Output.BlockHash != fixture.block1 {
		t.Fatalf("post-watermark row leaked into captured snapshot: %#v, %v", sourceState, err)
	}
	pinnedPage, _, err := store.Address(ctx, pinned, repository.AddressQuery{
		Address: fixture.addressA,
		State:   "history",
		Limit:   100,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range pinnedPage.Items {
		if item.Output.BlockHeight == 4 {
			t.Fatalf("post-watermark address candidate leaked: %#v", item)
		}
	}
	refreshedPinned, err := store.FinishSnapshot(ctx, pinned)
	if err != nil ||
		!pinned.SamePin(refreshedPinned) ||
		refreshedPinned.Diagnostics.Physical.EventSeq != 7 {
		t.Fatalf(
			"post-watermark writes invalidated the event-7 lease: %#v, %v",
			refreshedPinned,
			err,
		)
	}
	pinned = refreshedPinned

	assertPrimaryKeyPlan(t, store, snapshot, fixture.source)
	fixture.assertDatumDuplicateSemantics(t, store, snapshot)
	fixture.inflateIrrelevantHistory(t, store)
	assertInflatedHistoryReadsStayCandidateBounded(
		t,
		store,
		fixture.collateral,
		fixture.source,
		fixture.block1,
	)
	refreshedPinned, err = store.FinishSnapshot(ctx, pinned)
	if err != nil ||
		!pinned.SamePin(refreshedPinned) ||
		refreshedPinned.Diagnostics.Physical.EventSeq != 7 {
		t.Fatalf(
			"inflated history invalidated the event-7 lease: %#v, %v",
			refreshedPinned,
			err,
		)
	}
	fixture.insertDuplicateEvent(t, store, noOpWrittenAuthority)
	if _, err := store.Snapshot(
		ctx,
		model.AtPoint{Tip: true},
	); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("duplicate committed event was not rejected: %v", err)
	}
}

func assertPrimaryKeyPlan(t *testing.T, store *Store, snapshot model.Snapshot, ref model.UTxORef) {
	t.Helper()
	plan := explainPlan(
		t,
		store,
		"EXPLAIN indexes = 1 "+spendByRefSQL,
		activeArguments(snapshot, hashArgument(ref.TxHash), ref.Index)...,
	)
	if !strings.Contains(plan, "PrimaryKey") ||
		!strings.Contains(plan, "publication_id in") {
		t.Fatalf("source-spend plan is not candidate-first:\n%s", plan)
	}
	t.Logf("source-spend EXPLAIN indexes=1:\n%s", plan)
}

func assertProjectionPlans(
	t *testing.T,
	store *Store,
	snapshot model.Snapshot,
	ref model.UTxORef,
) {
	t.Helper()
	blockPlan := explainPlan(
		t,
		store,
		"EXPLAIN projections = 1 "+outputByRefSQL,
		activeArguments(snapshot, hashArgument(ref.TxHash), ref.Index)...,
	)
	if !strings.Contains(blockPlan, "blocks_by_publication") {
		t.Fatalf("output lookup did not expose blocks publication projection:\n%s", blockPlan)
	}
	membershipPlan := explainPlan(
		t,
		store,
		`EXPLAIN projections = 1
SELECT publication_id, event_seq, active
FROM chain_events
WHERE publication_id = ?
  AND event_seq <= ?
  AND event_kind = 'adoption'`,
		uint64(101),
		snapshot.QueryHead.EventSeq,
	)
	if !strings.Contains(membershipPlan, "chain_events_by_publication") {
		t.Fatalf("membership lookup did not expose publication projection:\n%s", membershipPlan)
	}
	rollbackPlan := explainPlan(
		t,
		store,
		`EXPLAIN projections = 1
SELECT rollback_id, event_seq
FROM rollbacks
WHERE (rollback_id, event_seq) IN ((toUUID(?), ?))`,
		"11111111-1111-4111-8111-111111111111",
		uint64(3),
	)
	if !strings.Contains(rollbackPlan, "rollbacks_by_id") {
		t.Fatalf("rollback lookup did not expose rollback-id projection:\n%s", rollbackPlan)
	}
	latestRollbackPlan := explainPlan(
		t,
		store,
		`EXPLAIN indexes = 1
SELECT event_seq
FROM rollbacks
ORDER BY event_seq DESC
LIMIT 1`,
	)
	if !strings.Contains(latestRollbackPlan, "PrimaryKey") ||
		!strings.Contains(latestRollbackPlan, ".rollbacks)") {
		t.Fatalf("latest rollback did not use event-first ordering:\n%s", latestRollbackPlan)
	}
	var rollbackPrimaryKey string
	if err := store.conn.QueryRow(
		context.Background(),
		`SELECT primary_key
         FROM system.tables
         WHERE database = currentDatabase()
           AND name = 'rollbacks'`,
	).Scan(&rollbackPrimaryKey); err != nil {
		t.Fatal(err)
	}
	if rollbackPrimaryKey != "event_seq, rollback_id" {
		t.Fatalf("unexpected rollback primary order %q", rollbackPrimaryKey)
	}
	tipPlan := explainPlan(
		t,
		store,
		`EXPLAIN indexes = 1
SELECT event_seq, publication_id
FROM chain_events
WHERE event_kind = 'adoption'
ORDER BY event_seq DESC
LIMIT 1`,
	)
	if !strings.Contains(tipPlan, "PrimaryKey") ||
		!strings.Contains(tipPlan, "event_kind") {
		t.Fatalf("tip snapshot did not use event-first ordering:\n%s", tipPlan)
	}
	var chainEventPrimaryKey string
	if err := store.conn.QueryRow(
		context.Background(),
		`SELECT primary_key
         FROM system.tables
         WHERE database = currentDatabase()
           AND name = 'chain_events'`,
	).Scan(&chainEventPrimaryKey); err != nil {
		t.Fatal(err)
	}
	if chainEventPrimaryKey != "event_kind, event_seq, publication_id" {
		t.Fatalf("unexpected chain event primary order %q", chainEventPrimaryKey)
	}
	pinnedPlan := explainPlan(
		t,
		store,
		`EXPLAIN indexes = 1
SELECT publication_id
FROM chain_events
WHERE event_kind = 'adoption'
  AND event_seq <= ?
ORDER BY event_seq DESC
LIMIT 1`,
		snapshot.QueryHead.EventSeq,
	)
	if !strings.Contains(pinnedPlan, "PrimaryKey") ||
		!strings.Contains(pinnedPlan, "event_seq") {
		t.Fatalf("pinned snapshot did not use event indexes:\n%s", pinnedPlan)
	}
	atBlockPlan := explainPlan(
		t,
		store,
		`EXPLAIN indexes = 1
SELECT publication_id
FROM blocks
WHERE block_hash = ?`,
		hashArgument(ref.TxHash),
	)
	if !strings.Contains(atBlockPlan, "PrimaryKey") ||
		!strings.Contains(atBlockPlan, "block_hash") {
		t.Fatalf("block snapshot did not use block-hash ordering:\n%s", atBlockPlan)
	}
	t.Logf("blocks candidate EXPLAIN projections=1:\n%s", blockPlan)
	t.Logf("membership candidate EXPLAIN projections=1:\n%s", membershipPlan)
	t.Logf("rollback candidate EXPLAIN projections=1:\n%s", rollbackPlan)
	t.Logf("latest rollback EXPLAIN indexes=1:\n%s", latestRollbackPlan)
	t.Logf("tip snapshot EXPLAIN indexes=1:\n%s", tipPlan)
	t.Logf("pinned snapshot EXPLAIN indexes=1:\n%s", pinnedPlan)
	t.Logf("at-block snapshot EXPLAIN indexes=1:\n%s", atBlockPlan)
}

func explainPlan(t *testing.T, store *Store, sql string, arguments ...any) string {
	t.Helper()
	queryCtx, finish := store.instrument(context.Background(), "explain_source_spend")
	defer finish()
	rows, err := store.conn.Query(queryCtx, sql, arguments...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(line)
		plan.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return plan.String()
}

func assertInflatedHistoryReadsStayCandidateBounded(
	t *testing.T,
	store *Store,
	ref model.UTxORef,
	resolvedSource model.UTxORef,
	expectedSourceBlock model.Hash32,
) {
	t.Helper()
	collector := &metrics.Collector{}
	ctx := metrics.WithCollector(context.Background(), collector)
	snapshot, err := store.Snapshot(ctx, model.AtPoint{Tip: true})
	if err != nil {
		t.Fatal(err)
	}
	state, boundaries, err := store.UTxO(ctx, snapshot, ref)
	if err != nil || len(boundaries) != 0 || state.Output.Ref != ref {
		t.Fatalf("inflated-history UTxO read failed: %#v %#v %v", state, boundaries, err)
	}
	if state.Consumption == nil {
		t.Fatal("inflated-history UTxO omitted its consuming hyperedge")
	}
	sourceIsCanonical := false
	for _, input := range state.Consumption.Inputs {
		if input.Source == resolvedSource &&
			input.SourceOutput != nil &&
			input.SourceOutput.BlockHash == expectedSourceBlock {
			sourceIsCanonical = true
			break
		}
	}
	if !sourceIsCanonical {
		t.Fatalf(
			"rolled-back post-watermark shadow remained active at inflated tip: %#v",
			state.Consumption.Inputs,
		)
	}
	const (
		inflatedRows = uint64(100_000)
		maxReadRows  = uint64(16_384)
	)
	for _, query := range collector.Snapshot() {
		t.Logf(
			"inflated-history metric name=%s read_rows=%d read_bytes=%d server=%s wall=%s",
			query.Name,
			query.ReadRows,
			query.ReadBytes,
			query.ServerElapsed,
			query.WallElapsed,
		)
		if query.ReadRows > maxReadRows {
			t.Fatalf(
				"query %s read %d rows from %d irrelevant publications; constant bound is %d",
				query.Name,
				query.ReadRows,
				inflatedRows,
				maxReadRows,
			)
		}
	}
	assertPrimaryKeyPlan(t, store, snapshot, ref)
	assertProjectionPlans(t, store, snapshot, ref)
}

type fixture struct {
	pub1, pub2, pub3      uint64
	start, block1, block2 model.Hash32
	txA, txB, txC         model.Hash32
	source                model.UTxORef
	collateral            model.UTxORef
	destination           model.UTxORef
	datumHash             model.Hash32
	dataHash              model.Hash32
	metadataHash          model.Hash32
	policy                model.PolicyID
	addressA              []byte
	addressB              []byte
	datumCBOR             []byte
	redeemerCBOR          []byte
	metadataCBOR          []byte
	rollbackID            string
}

func newFixture() fixture {
	hash := func(value byte) model.Hash32 {
		var result model.Hash32
		for index := range result {
			result[index] = value
		}
		return result
	}
	var policy model.PolicyID
	for index := range policy {
		policy[index] = 0x77
	}
	txA := hash(0x31)
	txB := hash(0x32)
	txC := hash(0x33)
	datumCBOR := []byte{0xd8, 0x79, 0x9f, 0xff}
	redeemerCBOR := []byte{0x81, 0x01}
	metadataCBOR := []byte{0xa1, 0x01, 0x42, 0xff, 0x00}
	return fixture{
		pub1:         101,
		pub2:         102,
		pub3:         103,
		start:        hash(0x10),
		block1:       hash(0x11),
		block2:       hash(0x12),
		txA:          txA,
		txB:          txB,
		txC:          txC,
		source:       model.UTxORef{TxHash: txA, Index: 0},
		collateral:   model.UTxORef{TxHash: txA, Index: 1},
		destination:  model.UTxORef{TxHash: txB, Index: 0},
		datumHash:    calculateContentHash(datumCBOR),
		dataHash:     calculateContentHash(redeemerCBOR),
		metadataHash: calculateContentHash(metadataCBOR),
		policy:       policy,
		addressA:     append([]byte{0x71}, policy[:]...),
		addressB:     append([]byte{0x61}, policy[:]...),
		datumCBOR:    datumCBOR,
		redeemerCBOR: redeemerCBOR,
		metadataCBOR: metadataCBOR,
		rollbackID:   "11111111-1111-4111-8111-111111111111",
	}
}

func canonicalIntegrationAuthority(
	t *testing.T,
	fixture fixture,
) ([]authorityRecord, []authorityObservationRow) {
	t.Helper()
	at := integrationAuthorityTime()
	start := authorityPoint{
		Hash: authorityHash(fixture.start),
	}
	first := authorityPoint{
		Slot:        1,
		Hash:        authorityHash(fixture.block1),
		BlockNumber: 1,
	}
	head := authorityHead{
		EventSeq: 2,
		Point: authorityPoint{
			Slot:        2,
			Hash:        authorityHash(fixture.block2),
			BlockNumber: 2,
		},
	}
	checkID := authorityFill16(0x41)
	group := authorityFill16(0x42)
	writer := integrationAuthorityWriterID()
	evidence := make([]authorityObservationRow, 0, 2)
	for index := uint32(1); index <= 2; index++ {
		slot := head.Point.Slot
		hash := head.Point.Hash
		number := head.Point.BlockNumber
		isByron := head.Point.IsByronEBB
		observation := authorityObservation{
			Kind:                   "checkpoint",
			PeerHost:               "integration-peer-" + string(rune('a'+index-1)),
			PeerAddress:            "192.0.2." + string(rune('0'+index)) + ":3001",
			Operator:               "integration-operator-" + string(rune('a'+index-1)),
			N2NVersion:             15,
			NetworkMagic:           764824073,
			TipSlot:                slot,
			TipHash:                hash,
			TipBlockNumber:         number,
			CheckpointSlot:         &slot,
			CheckpointHash:         &hash,
			CheckpointBlockNumber:  &number,
			CheckpointIsByronEBB:   &isByron,
			CheckID:                checkID,
			AgreementGroup:         group,
			CheckAttempt:           1,
			EvidenceOrdinal:        index,
			ProofMethod:            "chain_sync_singleton",
			CorroborationRequired:  2,
			CheckedEventSeq:        head.EventSeq,
			CheckedPointSlot:       &slot,
			CheckedPointHash:       &hash,
			CheckedBlockNumber:     &number,
			CheckedPointIsByronEBB: isByron,
			PointVerified:          true,
			Result:                 "agreed",
			Reason:                 "integration authority",
			ObservedAt:             at.Add(time.Duration(index) * time.Second),
		}
		if err := finalizeAuthorityObservationIdentity(&observation); err != nil {
			t.Fatal(err)
		}
		payload, err := canonicalAuthorityObservationPayload(observation)
		if err != nil {
			t.Fatal(err)
		}
		evidence = append(evidence, authorityObservationRow{
			Observation: observation,
			OperatorKey: strings.ToLower(strings.TrimSpace(observation.Operator)),
			Digest: authorityHash(
				sha256.Sum256([]byte(payload)),
			),
		})
	}
	commitment, err := canonicalAuthorityEvidenceCommitment(
		evidence,
		group,
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	initial := authorityRecord{
		Revision:               1,
		TransitionKind:         "initialize",
		DatasetID:              authorityFill16(0x33),
		SchemaContractHash:     expectedSchemaContract(),
		NetworkMagic:           764824073,
		NetworkName:            "mainnet",
		ByronGenesisID:         authorityFill32(0x21),
		ByronGenesisJSONHash:   authorityFill32(0x22),
		ShelleyGenesisID:       authorityFill32(0x23),
		ShelleyGenesisJSONHash: authorityFill32(0x24),
		Start:                  start,
		TrustMode:              model.TrustPeerObserved,
		TrustStatus:            "unavailable",
		TrustBasis:             "partial_boundary",
		CheckpointInterval:     manifestCheckpoint,
		TrustReason:            "partial-history boundary awaits bootstrap agreement",
		EvidenceState:          "none",
		ServableFloor:          authorityHead{Point: start},
		Physical:               authorityHead{Point: start},
		Effective:              authorityHead{Point: start},
		VisibilityGeneration:   1,
		WriterID:               &writer,
		WriterBuild:            "integration",
		SourceBuild:            "integration",
		CreatedAt:              at,
		UpdatedAt:              at,
	}
	if err := finalizeAuthorityRecord(&initial); err != nil {
		t.Fatal(err)
	}
	if err := verifyAuthorityRecord(initial); err != nil {
		t.Fatalf("canonical integration initialize: %v", err)
	}
	records := []authorityRecord{initial}
	appendRevision := func(
		kind string,
		updatedAt time.Time,
		mutate func(*authorityRecord),
	) {
		latest := records[len(records)-1]
		next := latest
		next.Revision++
		next.TransitionKind = kind
		previous := latest.RowDigest
		next.PreviousRowDigest = &previous
		next.TransitionID = [16]byte{}
		next.RowDigest = authorityHash{}
		next.UpdatedAt = updatedAt
		mutate(&next)
		if err := finalizeAuthorityRecord(&next); err != nil {
			t.Fatal(err)
		}
		if err := verifyAuthorityRecord(next); err != nil {
			t.Fatalf("canonical integration revision %d: %v", next.Revision, err)
		}
		if next.PreviousRowDigest == nil ||
			*next.PreviousRowDigest != latest.RowDigest ||
			!sameAuthorityIdentity(next, latest) {
			t.Fatalf("canonical integration revision %d broke its predecessor", next.Revision)
		}
		records = append(records, next)
	}
	appendRevision(
		"physical_adoption",
		integrationBlockTime(1),
		func(next *authorityRecord) {
			next.Physical = authorityHead{EventSeq: 1, Point: first}
			next.TrustReason = "physical adoption event 1 awaits bootstrap agreement"
		},
	)
	appendRevision(
		"physical_adoption",
		integrationBlockTime(2),
		func(next *authorityRecord) {
			next.Physical = head
			next.TrustReason = "physical adoption event 2 awaits bootstrap agreement"
		},
	)
	started := at.Add(250 * time.Millisecond)
	completed := at.Add(3 * time.Second)
	appendRevision(
		"bootstrap_agreed",
		completed,
		func(next *authorityRecord) {
			next.TrustStatus = "agreed"
			next.TrustBasis = "sampled_peer"
			next.CheckID = &checkID
			next.AgreementGroup = &group
			next.CheckAttempt = 1
			next.CorroborationRequired = 2
			next.CorroborationConfirmed = 2
			next.TrustReason = "integration event-2 bootstrap authority"
			next.CheckStartedAt = &started
			next.CheckCompletedAt = &completed
			next.EvidenceState = "frozen"
			next.EvidenceCount = commitment.Count
			next.EvidenceDigest = &commitment.Digest
			next.Checked = &head
			next.LastAgreed = &head
			next.LastAgreedAt = &completed
			next.LastAgreedEvidence = &authorityEvidenceReference{
				CheckID:   checkID,
				Group:     group,
				Attempt:   1,
				Required:  2,
				Confirmed: 2,
				Checked:   head,
				Count:     commitment.Count,
				Digest:    commitment.Digest,
			}
			next.Effective = next.Physical
			next.Servable = true
			next.VisibilityGeneration++
		},
	)
	return records, evidence
}

func canonicalIntegrationRollbackEvidence(
	t *testing.T,
	fixture fixture,
	checkID [16]byte,
	group [16]byte,
	attempt uint32,
	checked authorityHead,
	identity string,
	observedAt time.Time,
) []authorityObservationRow {
	t.Helper()
	if checked.Point.Origin || strings.TrimSpace(identity) == "" {
		t.Fatal("rollback evidence requires a non-Origin point and identity")
	}
	checkedSlot := checked.Point.Slot
	checkedHash := checked.Point.Hash
	checkedNumber := checked.Point.BlockNumber
	checkedIsByron := checked.Point.IsByronEBB
	tipHash := authorityHash(fixture.block2)
	rows := make([]authorityObservationRow, 0, 2)
	for ordinal := uint32(1); ordinal <= 2; ordinal++ {
		suffix := string(rune('a' + ordinal - 1))
		observation := authorityObservation{
			Kind:                   "rollback",
			PeerHost:               identity + "-peer-" + suffix,
			PeerAddress:            "198.51.100." + string(rune('0'+ordinal)) + ":3001",
			Operator:               identity + "-operator-" + suffix,
			N2NVersion:             15,
			NetworkMagic:           764824073,
			TipSlot:                2,
			TipHash:                tipHash,
			TipBlockNumber:         2,
			CheckpointSlot:         &checkedSlot,
			CheckpointHash:         &checkedHash,
			CheckpointBlockNumber:  &checkedNumber,
			CheckpointIsByronEBB:   &checkedIsByron,
			CheckID:                checkID,
			AgreementGroup:         group,
			CheckAttempt:           attempt,
			EvidenceOrdinal:        ordinal,
			ProofMethod:            "paired_chain_sync_singleton",
			CorroborationRequired:  2,
			CheckedEventSeq:        checked.EventSeq,
			CheckedPointSlot:       &checkedSlot,
			CheckedPointHash:       &checkedHash,
			CheckedBlockNumber:     &checkedNumber,
			CheckedPointIsByronEBB: checkedIsByron,
			PointVerified:          true,
			Result:                 "agreed",
			Reason:                 "integration " + identity + " agreement",
			ObservedAt: observedAt.Add(
				time.Duration(ordinal-1) * time.Microsecond,
			),
		}
		if err := finalizeAuthorityObservationIdentity(&observation); err != nil {
			t.Fatal(err)
		}
		payload, err := canonicalAuthorityObservationPayload(observation)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, authorityObservationRow{
			Observation: observation,
			OperatorKey: strings.ToLower(strings.TrimSpace(observation.Operator)),
			Digest:      sha256.Sum256([]byte(payload)),
		})
	}
	return rows
}

func TestCanonicalIntegrationRollbackEvidenceSerialization(t *testing.T) {
	fixture := newFixture()
	checkID := authorityFill16(0x51)
	group := authorityFill16(0x52)
	const attempt = uint32(1)
	rows := canonicalIntegrationRollbackEvidence(
		t,
		fixture,
		checkID,
		group,
		attempt,
		authorityHead{
			EventSeq: 1,
			Point: authorityPoint{
				Slot:        1,
				Hash:        authorityHash(fixture.block1),
				BlockNumber: 1,
			},
		},
		"rollback",
		integrationAuthorityTime().Add(4*time.Second),
	)
	commitment, err := canonicalAuthorityEvidenceCommitment(rows, group, attempt)
	if err != nil {
		t.Fatal(err)
	}
	if commitment.Count != 2 || commitment.Digest == (authorityHash{}) {
		t.Fatalf("unexpected rollback evidence commitment: %#v", commitment)
	}
	checked := authorityHead{
		EventSeq: 1,
		Point: authorityPoint{
			Slot:        1,
			Hash:        authorityHash(fixture.block1),
			BlockNumber: 1,
		},
	}
	bound, err := bindAuthorityRollbackEvidence(
		rows,
		checkID,
		group,
		attempt,
		2,
		checked,
	)
	if err != nil {
		t.Fatal(err)
	}
	if bound.Outcome.Confirmed != 2 ||
		bound.Outcome.Disagreement ||
		bound.Commitment.Digest != commitment.Digest {
		t.Fatalf("unexpected rollback evidence binding: %#v", bound)
	}
	for index, row := range rows {
		roundTrip, err := integrationAuthorityObservationDBRow(row).row()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(roundTrip, row) {
			t.Fatalf("rollback evidence DB round trip changed row %d", index)
		}
	}
}

func TestCanonicalIntegrationNoOpRollbackEvidenceSerialization(t *testing.T) {
	fixture := newFixture()
	checkID := authorityFill16(0x61)
	group := authorityFill16(0x62)
	const attempt = uint32(1)
	checked := authorityHead{
		EventSeq: 6,
		Point: authorityPoint{
			Slot:        2,
			Hash:        authorityHash(fixture.block2),
			BlockNumber: 2,
		},
	}
	rows := canonicalIntegrationRollbackEvidence(
		t,
		fixture,
		checkID,
		group,
		attempt,
		checked,
		"noop",
		integrationBlockTime(7).Add(-time.Second),
	)
	commitment, err := canonicalAuthorityEvidenceCommitment(rows, group, attempt)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := bindAuthorityRollbackEvidence(
		rows,
		checkID,
		group,
		attempt,
		2,
		checked,
	)
	if err != nil {
		t.Fatal(err)
	}
	if commitment.Count != 2 ||
		commitment.Digest == (authorityHash{}) ||
		bound.Outcome.Confirmed != 2 ||
		bound.Outcome.Disagreement ||
		bound.Commitment.Digest != commitment.Digest ||
		len(rows) != 2 ||
		rows[0].Observation.ID == rows[1].Observation.ID ||
		rows[0].OperatorKey == rows[1].OperatorKey ||
		rows[0].Observation.PeerHost == rows[1].Observation.PeerHost {
		t.Fatalf("unexpected no-op rollback evidence: %#v", bound)
	}
	for index, row := range rows {
		if row.Observation.CheckID != checkID ||
			row.Observation.AgreementGroup != group ||
			row.Observation.CheckedEventSeq != checked.EventSeq ||
			authorityObservationPoint(row.Observation) != checked.Point ||
			!strings.HasPrefix(row.OperatorKey, "noop-operator-") ||
			!strings.HasPrefix(row.Observation.PeerHost, "noop-peer-") {
			t.Fatalf("no-op evidence row %d has wrong identity: %#v", index, row)
		}
		roundTrip, err := integrationAuthorityObservationDBRow(row).row()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(roundTrip, row) {
			t.Fatalf("no-op evidence DB round trip changed row %d", index)
		}
	}
}

func canonicalIntegrationPendingRollback(
	t *testing.T,
	fixture fixture,
	previous authorityRecord,
) (authorityRecord, []authorityObservationRow) {
	t.Helper()
	checkID := authorityFill16(0x51)
	group := authorityFill16(0x52)
	const attempt = uint32(1)
	startedAt := integrationAuthorityTime().Add(4 * time.Second)
	recordedAt := integrationAuthorityTime().Add(5 * time.Second)
	rows := canonicalIntegrationRollbackEvidence(
		t,
		fixture,
		checkID,
		group,
		attempt,
		authorityHead{
			EventSeq: 1,
			Point: authorityPoint{
				Slot:        1,
				Hash:        authorityHash(fixture.block1),
				BlockNumber: 1,
			},
		},
		"rollback",
		startedAt,
	)
	commitment, err := canonicalAuthorityEvidenceCommitment(rows, group, attempt)
	if err != nil {
		t.Fatal(err)
	}
	target := authorityPoint{
		Slot:        1,
		Hash:        authorityHash(fixture.block1),
		BlockNumber: 1,
	}
	checked := authorityHead{EventSeq: 1, Point: target}
	binding, err := bindAuthorityRollbackEvidence(
		rows,
		checkID,
		group,
		attempt,
		2,
		checked,
	)
	if err != nil {
		t.Fatal(err)
	}
	peers := make([]string, 0, binding.Outcome.Confirmed)
	operators := make([]string, 0, binding.Outcome.Confirmed)
	for _, row := range binding.Commitment.Rows {
		if row.Observation.Result != "agreed" {
			continue
		}
		operator := row.OperatorKey
		operators = append(operators, operator)
		peers = append(peers, binding.Agreed[operator])
	}
	if len(peers) != 2 || len(operators) != 2 {
		t.Fatalf("rollback evidence observer map is incomplete: %#v", binding)
	}
	if previous.WriterID == nil {
		t.Fatal("previous integration authority has no writer")
	}
	writer := *previous.WriterID
	next := previous
	next.Revision++
	next.TransitionKind = "rollback_reserved"
	previousDigest := previous.RowDigest
	next.PreviousRowDigest = &previousDigest
	next.TransitionID = [16]byte{}
	next.RowDigest = authorityHash{}
	next.TrustStatus = "checking"
	next.TrustBasis = "sampled_peer"
	next.CheckID = &checkID
	next.AgreementGroup = &group
	next.CheckAttempt = attempt
	next.CorroborationRequired = 2
	next.CorroborationConfirmed = 0
	next.Disagreement = false
	next.TrustReason = "corroborated rollback reserved before physical invalidations"
	next.CheckStartedAt = &startedAt
	next.CheckCompletedAt = nil
	next.EvidenceState = "frozen"
	next.EvidenceCount = commitment.Count
	next.EvidenceDigest = &commitment.Digest
	next.PendingEvidenceWrite = nil
	next.Checked = &checked
	next.Effective = previous.Physical
	next.Servable = true
	next.VisibilityGeneration++
	next.PendingRollback = &authorityPendingRollback{
		State:           "reserved",
		ID:              authorityUUID(uuid.MustParse(fixture.rollbackID)),
		EventSeq:        3,
		To:              target,
		OldPhysical:     previous.Physical,
		Depth:           1,
		Reason:          "integration corroborated rollback",
		Peers:           peers,
		Operators:       operators,
		Required:        2,
		CheckID:         checkID,
		Group:           group,
		CheckAttempt:    attempt,
		CheckedEventSeq: checked.EventSeq,
		EvidenceCount:   commitment.Count,
		EvidenceDigest:  commitment.Digest,
		WriterID:        writer,
		StartedAt:       recordedAt,
	}
	next.WriterID = &writer
	next.UpdatedAt = recordedAt
	if err := finalizeAuthorityRecord(&next); err != nil {
		t.Fatal(err)
	}
	if err := verifyAuthorityRecord(next); err != nil {
		t.Fatalf("canonical integration pending rollback: %v", err)
	}
	if next.Revision != previous.Revision+1 ||
		next.PreviousRowDigest == nil ||
		*next.PreviousRowDigest != previous.RowDigest ||
		!sameAuthorityIdentity(next, previous) {
		t.Fatal("canonical integration pending rollback broke its predecessor")
	}
	return next, rows
}

func canonicalIntegrationNoOpRollback(
	t *testing.T,
	fixture fixture,
	previous authorityRecord,
) (authorityRecord, []authorityObservationRow) {
	t.Helper()
	if previous.Physical.EventSeq != 6 ||
		previous.LastAgreed == nil ||
		previous.LastAgreedEvidence == nil ||
		previous.WriterID == nil {
		t.Fatal("no-op rollback lacks re-adopted predecessor authority")
	}
	checkID := authorityFill16(0x61)
	group := authorityFill16(0x62)
	const attempt = uint32(1)
	target := authorityPoint{
		Slot:        2,
		Hash:        authorityHash(fixture.block2),
		BlockNumber: 2,
	}
	checked := authorityHead{EventSeq: 6, Point: target}
	startedAt := integrationBlockTime(7).Add(-time.Second)
	recordedAt := integrationBlockTime(7)
	rows := canonicalIntegrationRollbackEvidence(
		t,
		fixture,
		checkID,
		group,
		attempt,
		checked,
		"noop",
		startedAt,
	)
	commitment, err := canonicalAuthorityEvidenceCommitment(rows, group, attempt)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := bindAuthorityRollbackEvidence(
		rows,
		checkID,
		group,
		attempt,
		2,
		checked,
	)
	if err != nil {
		t.Fatal(err)
	}
	peers := make([]string, 0, binding.Outcome.Confirmed)
	operators := make([]string, 0, binding.Outcome.Confirmed)
	for _, row := range binding.Commitment.Rows {
		if row.Observation.Result != "agreed" {
			continue
		}
		operator := row.OperatorKey
		operators = append(operators, operator)
		peers = append(peers, binding.Agreed[operator])
	}
	if len(peers) != 2 || len(operators) != 2 {
		t.Fatalf("no-op rollback observer map is incomplete: %#v", binding)
	}
	writer := *previous.WriterID
	next := previous
	next.Revision++
	next.TransitionKind = "rollback_reserved"
	previousDigest := previous.RowDigest
	next.PreviousRowDigest = &previousDigest
	next.TransitionID = [16]byte{}
	next.RowDigest = authorityHash{}
	next.TrustStatus = "checking"
	next.TrustBasis = "sampled_peer"
	next.CheckID = &checkID
	next.AgreementGroup = &group
	next.CheckAttempt = attempt
	next.CorroborationRequired = 2
	next.CorroborationConfirmed = 0
	next.Disagreement = false
	next.TrustReason = "corroborated rollback reserved before physical invalidations"
	next.CheckStartedAt = &startedAt
	next.CheckCompletedAt = nil
	next.EvidenceState = "frozen"
	next.EvidenceCount = commitment.Count
	next.EvidenceDigest = &commitment.Digest
	next.PendingEvidenceWrite = nil
	next.Checked = &checked
	next.Effective = *previous.LastAgreed
	next.Servable = true
	next.VisibilityGeneration++
	next.PendingRollback = &authorityPendingRollback{
		State:           "reserved",
		ID:              authorityUUID(uuid.MustParse(integrationNoOpRollbackUUID)),
		EventSeq:        7,
		To:              target,
		OldPhysical:     previous.Physical,
		Depth:           0,
		Reason:          "integration corroborated no-op rollback",
		Peers:           peers,
		Operators:       operators,
		Required:        2,
		CheckID:         checkID,
		Group:           group,
		CheckAttempt:    attempt,
		CheckedEventSeq: checked.EventSeq,
		EvidenceCount:   commitment.Count,
		EvidenceDigest:  commitment.Digest,
		WriterID:        writer,
		StartedAt:       recordedAt,
	}
	next.WriterID = &writer
	next.UpdatedAt = recordedAt
	if err := finalizeAuthorityRecord(&next); err != nil {
		t.Fatal(err)
	}
	if err := verifyAuthorityRecord(next); err != nil {
		t.Fatalf("canonical integration no-op rollback: %v", err)
	}
	if next.Revision != previous.Revision+1 ||
		next.PreviousRowDigest == nil ||
		*next.PreviousRowDigest != previous.RowDigest ||
		!sameAuthorityIdentity(next, previous) {
		t.Fatal("canonical no-op rollback broke its predecessor")
	}
	return next, rows
}

func canonicalIntegrationInvalidationsWritten(
	t *testing.T,
	previous authorityRecord,
) authorityRecord {
	t.Helper()
	if previous.PendingRollback == nil ||
		previous.PendingRollback.State != "reserved" {
		t.Fatal("invalidations-written transition lacks reserved predecessor")
	}
	next := previous
	next.Revision++
	next.TransitionKind = "rollback_invalidations_written"
	previousDigest := previous.RowDigest
	next.PreviousRowDigest = &previousDigest
	next.TransitionID = [16]byte{}
	next.RowDigest = authorityHash{}
	pending := *previous.PendingRollback
	pending.State = "invalidations_written"
	next.PendingRollback = &pending
	next.TrustReason = "rollback invalidations written; header/finalization pending"
	next.UpdatedAt = pending.StartedAt
	if err := finalizeAuthorityRecord(&next); err != nil {
		t.Fatal(err)
	}
	if err := verifyAuthorityRecord(next); err != nil {
		t.Fatalf("canonical integration invalidations-written rollback: %v", err)
	}
	if next.Revision != previous.Revision+1 ||
		next.PreviousRowDigest == nil ||
		*next.PreviousRowDigest != previous.RowDigest ||
		!sameAuthorityIdentity(next, previous) {
		t.Fatal("canonical invalidations-written rollback broke its predecessor")
	}
	return next
}

func TestCanonicalIntegrationPendingRollbackSerialization(t *testing.T) {
	fixture := newFixture()
	records, bootstrapEvidence := canonicalIntegrationAuthority(t, fixture)
	previous := records[len(records)-1]
	pending, rows := canonicalIntegrationPendingRollback(t, fixture, previous)
	if pending.Revision != 5 ||
		pending.TransitionKind != "rollback_reserved" ||
		pending.VisibilityGeneration != 3 ||
		pending.TrustStatus != "checking" ||
		pending.TrustBasis != "sampled_peer" ||
		pending.CorroborationConfirmed != 0 ||
		pending.CheckCompletedAt != nil ||
		pending.EvidenceState != "frozen" ||
		pending.Effective != previous.Physical ||
		!pending.Servable ||
		pending.LastAgreed == nil ||
		*pending.LastAgreed != previous.Physical ||
		pending.LastAgreedEvidence == nil ||
		pending.PendingRollback == nil {
		t.Fatalf("unexpected pending authority: %#v", pending)
	}
	target := authorityPoint{
		Slot:        1,
		Hash:        authorityHash(fixture.block1),
		BlockNumber: 1,
	}
	expectedChecked := authorityHead{EventSeq: 1, Point: target}
	rollback := pending.PendingRollback
	if pending.Checked == nil ||
		*pending.Checked != expectedChecked ||
		rollback.State != "reserved" ||
		rollback.EventSeq != 3 ||
		rollback.To != target ||
		rollback.OldPhysical != previous.Physical ||
		rollback.Depth != 1 ||
		rollback.CheckID != *pending.CheckID ||
		rollback.Group != *pending.AgreementGroup ||
		rollback.CheckAttempt != pending.CheckAttempt ||
		rollback.CheckedEventSeq != expectedChecked.EventSeq ||
		rollback.EvidenceCount != pending.EvidenceCount ||
		rollback.EvidenceDigest != *pending.EvidenceDigest ||
		rollback.WriterID != *pending.WriterID ||
		rollback.WriterID != integrationAuthorityWriterID() ||
		len(rollback.Peers) != 2 ||
		len(rollback.Operators) != 2 {
		t.Fatalf("unexpected pending rollback: %#v", rollback)
	}
	binding, err := bindAuthorityRollbackEvidence(
		rows,
		rollback.CheckID,
		rollback.Group,
		rollback.CheckAttempt,
		rollback.Required,
		expectedChecked,
	)
	if err != nil {
		t.Fatal(err)
	}
	if binding.Outcome.Confirmed != 2 ||
		binding.Outcome.Disagreement ||
		binding.Commitment.Count != rollback.EvidenceCount ||
		binding.Commitment.Digest != rollback.EvidenceDigest {
		t.Fatalf("unexpected pending evidence binding: %#v", binding)
	}
	if err := bindAuthorityEvidence(
		pending,
		rows,
		bootstrapEvidence,
	); err != nil {
		t.Fatal(err)
	}
	roundTrip, err := integrationAuthorityDBRow(pending).record()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(roundTrip, pending) {
		t.Fatal("pending authority DB round trip changed record")
	}
	written := canonicalIntegrationInvalidationsWritten(t, pending)
	if written.Revision != 6 ||
		written.TransitionKind != "rollback_invalidations_written" ||
		written.VisibilityGeneration != pending.VisibilityGeneration ||
		written.PendingRollback == nil ||
		written.PendingRollback.State != "invalidations_written" {
		t.Fatalf("unexpected invalidations-written authority: %#v", written)
	}
	expectedPending := *pending.PendingRollback
	expectedPending.State = "invalidations_written"
	if !reflect.DeepEqual(*written.PendingRollback, expectedPending) {
		t.Fatalf(
			"invalidations-written transition changed reservation: %#v",
			written.PendingRollback,
		)
	}
	writtenRoundTrip, err := integrationAuthorityDBRow(written).record()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(writtenRoundTrip, written) {
		t.Fatal("invalidations-written authority DB round trip changed record")
	}
}

func canonicalIntegrationFinalizedRollback(
	t *testing.T,
	previous authorityRecord,
) authorityRecord {
	t.Helper()
	if previous.PendingRollback == nil ||
		previous.PendingRollback.State != "invalidations_written" ||
		previous.Checked == nil ||
		previous.EvidenceDigest == nil {
		t.Fatal("finalized rollback lacks invalidations-written authority")
	}
	pending := *previous.PendingRollback
	target := pending.To
	next := previous
	next.Revision++
	next.TransitionKind = "rollback_finalized"
	previousDigest := previous.RowDigest
	next.PreviousRowDigest = &previousDigest
	next.TransitionID = [16]byte{}
	next.RowDigest = authorityHash{}
	next.Physical = authorityHead{EventSeq: pending.EventSeq, Point: target}
	next.Effective = next.Physical
	next.TrustStatus = "agreed"
	next.TrustBasis = "sampled_peer"
	next.CorroborationConfirmed = 2
	next.Disagreement = false
	next.TrustReason = "corroborated rollback header committed"
	completedAt := pending.StartedAt
	next.CheckCompletedAt = &completedAt
	agreed := next.Physical
	next.LastAgreed = &agreed
	next.LastAgreedAt = &completedAt
	next.LastAgreedEvidence = &authorityEvidenceReference{
		CheckID:   pending.CheckID,
		Group:     pending.Group,
		Attempt:   pending.CheckAttempt,
		Required:  pending.Required,
		Confirmed: 2,
		Checked:   *previous.Checked,
		Count:     pending.EvidenceCount,
		Digest:    pending.EvidenceDigest,
	}
	next.PendingRollback = nil
	next.Servable = true
	next.PrimarySuffix = 0
	next.UpdatedAt = completedAt
	if err := finalizeAuthorityRecord(&next); err != nil {
		t.Fatal(err)
	}
	if err := verifyAuthorityRecord(next); err != nil {
		t.Fatalf("canonical integration finalized rollback: %v", err)
	}
	if next.Revision != previous.Revision+1 ||
		next.PreviousRowDigest == nil ||
		*next.PreviousRowDigest != previous.RowDigest ||
		!sameAuthorityIdentity(next, previous) {
		t.Fatal("canonical finalized rollback broke its predecessor")
	}
	return next
}

func TestCanonicalIntegrationFinalizedRollbackSerialization(t *testing.T) {
	fixture := newFixture()
	records, _ := canonicalIntegrationAuthority(t, fixture)
	reserved, rows := canonicalIntegrationPendingRollback(
		t,
		fixture,
		records[len(records)-1],
	)
	written := canonicalIntegrationInvalidationsWritten(t, reserved)
	finalized := canonicalIntegrationFinalizedRollback(t, written)
	target := authorityPoint{
		Slot:        1,
		Hash:        authorityHash(fixture.block1),
		BlockNumber: 1,
	}
	if finalized.Revision != 7 ||
		finalized.TransitionKind != "rollback_finalized" ||
		finalized.VisibilityGeneration != 3 ||
		finalized.Physical != (authorityHead{EventSeq: 3, Point: target}) ||
		finalized.Effective != finalized.Physical ||
		finalized.Checked == nil ||
		*finalized.Checked != (authorityHead{EventSeq: 1, Point: target}) ||
		finalized.TrustStatus != "agreed" ||
		finalized.TrustBasis != "sampled_peer" ||
		finalized.CorroborationConfirmed != 2 ||
		finalized.CheckCompletedAt == nil ||
		!finalized.CheckCompletedAt.Equal(
			written.PendingRollback.StartedAt,
		) ||
		finalized.LastAgreed == nil ||
		*finalized.LastAgreed != finalized.Physical ||
		finalized.LastAgreedAt == nil ||
		!finalized.LastAgreedAt.Equal(
			written.PendingRollback.StartedAt,
		) ||
		finalized.LastAgreedEvidence == nil ||
		finalized.LastAgreedEvidence.Checked != *finalized.Checked ||
		finalized.LastAgreedEvidence.Confirmed != 2 ||
		finalized.LastAgreedEvidence.Count != finalized.EvidenceCount ||
		finalized.LastAgreedEvidence.Digest != *finalized.EvidenceDigest ||
		finalized.PendingRollback != nil ||
		!finalized.Servable ||
		finalized.WriterID == nil ||
		*finalized.WriterID != integrationAuthorityWriterID() {
		t.Fatalf("unexpected finalized rollback: %#v", finalized)
	}
	if err := bindAuthorityEvidence(finalized, rows, rows); err != nil {
		t.Fatal(err)
	}
	binding, err := bindAuthorityRollbackEvidence(
		rows,
		finalized.LastAgreedEvidence.CheckID,
		finalized.LastAgreedEvidence.Group,
		finalized.LastAgreedEvidence.Attempt,
		finalized.LastAgreedEvidence.Required,
		finalized.LastAgreedEvidence.Checked,
	)
	if err != nil {
		t.Fatal(err)
	}
	if binding.Outcome.Confirmed != 2 ||
		binding.Outcome.Disagreement ||
		binding.Commitment.Count != finalized.LastAgreedEvidence.Count ||
		binding.Commitment.Digest != finalized.LastAgreedEvidence.Digest {
		t.Fatalf("unexpected finalized evidence binding: %#v", binding)
	}
	roundTrip, err := integrationAuthorityDBRow(finalized).record()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(roundTrip, finalized) {
		t.Fatal("finalized authority DB round trip changed record")
	}
}

func canonicalIntegrationReadoption(
	t *testing.T,
	fixture fixture,
	previous authorityRecord,
) authorityRecord {
	t.Helper()
	if previous.PendingRollback != nil ||
		previous.Physical.EventSeq != 3 ||
		previous.LastAgreed == nil ||
		previous.LastAgreedEvidence == nil {
		t.Fatal("re-adoption lacks finalized rollback predecessor")
	}
	next := previous
	next.Revision++
	next.TransitionKind = "physical_adoption"
	previousDigest := previous.RowDigest
	next.PreviousRowDigest = &previousDigest
	next.TransitionID = [16]byte{}
	next.RowDigest = authorityHash{}
	next.Physical = authorityHead{
		EventSeq: 6,
		Point: authorityPoint{
			Slot:        2,
			Hash:        authorityHash(fixture.block2),
			BlockNumber: 2,
		},
	}
	next.Effective = next.Physical
	next.TrustStatus = "agreed"
	next.TrustBasis = "primary_only"
	next.PrimarySuffix++
	next.Servable = true
	next.UpdatedAt = integrationBlockTime(6)
	if err := finalizeAuthorityRecord(&next); err != nil {
		t.Fatal(err)
	}
	if err := verifyAuthorityRecord(next); err != nil {
		t.Fatalf("canonical integration re-adoption: %v", err)
	}
	if next.Revision != previous.Revision+1 ||
		next.PreviousRowDigest == nil ||
		*next.PreviousRowDigest != previous.RowDigest ||
		!sameAuthorityIdentity(next, previous) {
		t.Fatal("canonical integration re-adoption broke its predecessor")
	}
	return next
}

func TestCanonicalIntegrationReadoptionSerialization(t *testing.T) {
	fixture := newFixture()
	records, _ := canonicalIntegrationAuthority(t, fixture)
	reserved, rows := canonicalIntegrationPendingRollback(
		t,
		fixture,
		records[len(records)-1],
	)
	written := canonicalIntegrationInvalidationsWritten(t, reserved)
	finalized := canonicalIntegrationFinalizedRollback(t, written)
	readopted := canonicalIntegrationReadoption(t, fixture, finalized)
	expectedPhysical := authorityHead{
		EventSeq: 6,
		Point: authorityPoint{
			Slot:        2,
			Hash:        authorityHash(fixture.block2),
			BlockNumber: 2,
		},
	}
	if readopted.Revision != 8 ||
		readopted.TransitionKind != "physical_adoption" ||
		readopted.Physical != expectedPhysical ||
		readopted.Effective != expectedPhysical ||
		readopted.TrustStatus != "agreed" ||
		readopted.TrustBasis != "primary_only" ||
		readopted.TrustReason != finalized.TrustReason ||
		readopted.PrimarySuffix != finalized.PrimarySuffix+1 ||
		readopted.VisibilityGeneration != 3 ||
		!readopted.Servable ||
		readopted.PendingRollback != nil ||
		readopted.WriterID == nil ||
		*readopted.WriterID != integrationAuthorityWriterID() ||
		!readopted.UpdatedAt.Equal(integrationBlockTime(6)) ||
		readopted.LastAgreed == nil ||
		!reflect.DeepEqual(readopted.LastAgreed, finalized.LastAgreed) ||
		!reflect.DeepEqual(
			readopted.LastAgreedEvidence,
			finalized.LastAgreedEvidence,
		) ||
		!reflect.DeepEqual(readopted.Checked, finalized.Checked) ||
		readopted.CheckID == nil ||
		*readopted.CheckID != *finalized.CheckID ||
		readopted.EvidenceDigest == nil ||
		*readopted.EvidenceDigest != *finalized.EvidenceDigest {
		t.Fatalf("unexpected re-adoption authority: %#v", readopted)
	}
	if err := bindAuthorityEvidence(readopted, rows, rows); err != nil {
		t.Fatal(err)
	}
	roundTrip, err := integrationAuthorityDBRow(readopted).record()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(roundTrip, readopted) {
		t.Fatal("re-adoption authority DB round trip changed record")
	}
}

func TestCanonicalIntegrationNoOpRollbackSerialization(t *testing.T) {
	fixture := newFixture()
	records, _ := canonicalIntegrationAuthority(t, fixture)
	reserved, rollbackRows := canonicalIntegrationPendingRollback(
		t,
		fixture,
		records[len(records)-1],
	)
	written := canonicalIntegrationInvalidationsWritten(t, reserved)
	finalized := canonicalIntegrationFinalizedRollback(t, written)
	readopted := canonicalIntegrationReadoption(t, fixture, finalized)
	noOp, noOpRows := canonicalIntegrationNoOpRollback(t, fixture, readopted)
	target := authorityPoint{
		Slot:        2,
		Hash:        authorityHash(fixture.block2),
		BlockNumber: 2,
	}
	checked := authorityHead{EventSeq: 6, Point: target}
	clamp := authorityHead{
		EventSeq: 3,
		Point: authorityPoint{
			Slot:        1,
			Hash:        authorityHash(fixture.block1),
			BlockNumber: 1,
		},
	}
	if noOp.Revision != 9 ||
		noOp.TransitionKind != "rollback_reserved" ||
		noOp.VisibilityGeneration != 4 ||
		noOp.Physical != (authorityHead{EventSeq: 6, Point: target}) ||
		noOp.Effective != clamp ||
		noOp.TrustStatus != "checking" ||
		noOp.TrustBasis != "sampled_peer" ||
		noOp.TrustReason !=
			"corroborated rollback reserved before physical invalidations" ||
		noOp.CorroborationConfirmed != 0 ||
		noOp.CheckCompletedAt != nil ||
		noOp.EvidenceState != "frozen" ||
		noOp.Checked == nil ||
		*noOp.Checked != checked ||
		noOp.LastAgreed == nil ||
		*noOp.LastAgreed != clamp ||
		!reflect.DeepEqual(noOp.LastAgreed, readopted.LastAgreed) ||
		!reflect.DeepEqual(
			noOp.LastAgreedEvidence,
			readopted.LastAgreedEvidence,
		) ||
		!noOp.Servable ||
		noOp.PendingRollback == nil {
		t.Fatalf("unexpected no-op reservation: %#v", noOp)
	}
	pending := noOp.PendingRollback
	if pending.State != "reserved" ||
		pending.ID != authorityUUID(uuid.MustParse(integrationNoOpRollbackUUID)) ||
		pending.EventSeq != 7 ||
		pending.To != target ||
		pending.OldPhysical != (authorityHead{EventSeq: 6, Point: target}) ||
		pending.Depth != 0 ||
		pending.CheckID != *noOp.CheckID ||
		pending.Group != *noOp.AgreementGroup ||
		pending.CheckAttempt != noOp.CheckAttempt ||
		pending.CheckedEventSeq != checked.EventSeq ||
		pending.EvidenceCount != noOp.EvidenceCount ||
		pending.EvidenceDigest != *noOp.EvidenceDigest ||
		pending.WriterID != integrationAuthorityWriterID() ||
		!pending.StartedAt.Equal(integrationBlockTime(7)) ||
		len(pending.Peers) != 2 ||
		len(pending.Operators) != 2 {
		t.Fatalf("unexpected no-op pending authority: %#v", pending)
	}
	if err := bindAuthorityEvidence(noOp, noOpRows, rollbackRows); err != nil {
		t.Fatal(err)
	}
	binding, err := bindAuthorityRollbackEvidence(
		noOpRows,
		pending.CheckID,
		pending.Group,
		pending.CheckAttempt,
		pending.Required,
		checked,
	)
	if err != nil {
		t.Fatal(err)
	}
	if binding.Outcome.Confirmed != 2 ||
		binding.Outcome.Disagreement ||
		binding.Commitment.Count != pending.EvidenceCount ||
		binding.Commitment.Digest != pending.EvidenceDigest {
		t.Fatalf("unexpected no-op evidence binding: %#v", binding)
	}
	header := authorityPhysicalRollbackFromPending(*pending)
	emptyComplete, err := validateAuthorityRollbackInvalidationSet(
		context.Background(),
		header,
		func(
			context.Context,
			authorityRollbackDescendantVisitor,
		) (uint32, error) {
			return 0, nil
		},
		func(
			context.Context,
			uint64,
			uint64,
		) (authorityPhysicalMembershipRow, authorityPoint, bool, error) {
			return authorityPhysicalMembershipRow{}, authorityPoint{}, false,
				errors.New("depth-zero rollback loaded an invalidation")
		},
		func(
			context.Context,
			uint64,
			bool,
			uint64,
		) (authorityPhysicalMembershipRow, authorityPoint, bool, error) {
			return authorityPhysicalMembershipRow{}, authorityPoint{}, false, nil
		},
	)
	if err != nil || !emptyComplete {
		t.Fatalf("no-op empty invalidation set complete=%t err=%v", emptyComplete, err)
	}
	noOpRoundTrip, err := integrationAuthorityDBRow(noOp).record()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(noOpRoundTrip, noOp) {
		t.Fatal("no-op reservation DB round trip changed record")
	}

	noOpWritten := canonicalIntegrationInvalidationsWritten(t, noOp)
	expectedPending := *pending
	expectedPending.State = "invalidations_written"
	if noOpWritten.Revision != 10 ||
		noOpWritten.TransitionKind != "rollback_invalidations_written" ||
		noOpWritten.VisibilityGeneration != 4 ||
		noOpWritten.Physical != noOp.Physical ||
		noOpWritten.Effective != clamp ||
		noOpWritten.Checked == nil ||
		*noOpWritten.Checked != checked ||
		noOpWritten.PendingRollback == nil ||
		!reflect.DeepEqual(*noOpWritten.PendingRollback, expectedPending) ||
		noOpWritten.PreviousRowDigest == nil ||
		*noOpWritten.PreviousRowDigest != noOp.RowDigest ||
		!sameAuthorityIdentity(noOpWritten, noOp) {
		t.Fatalf("unexpected no-op invalidations-written cut: %#v", noOpWritten)
	}
	writtenRoundTrip, err := integrationAuthorityDBRow(noOpWritten).record()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(writtenRoundTrip, noOpWritten) {
		t.Fatal("no-op invalidations-written DB round trip changed record")
	}

	noOpFinalized := canonicalIntegrationFinalizedRollback(t, noOpWritten)
	finalHead := authorityHead{EventSeq: 7, Point: target}
	if noOpFinalized.Revision != 11 ||
		noOpFinalized.TransitionKind != "rollback_finalized" ||
		noOpFinalized.VisibilityGeneration != 4 ||
		noOpFinalized.Physical != finalHead ||
		noOpFinalized.Effective != finalHead ||
		noOpFinalized.LastAgreed == nil ||
		*noOpFinalized.LastAgreed != finalHead ||
		noOpFinalized.Checked == nil ||
		*noOpFinalized.Checked != checked ||
		noOpFinalized.TrustStatus != "agreed" ||
		noOpFinalized.TrustBasis != "sampled_peer" ||
		noOpFinalized.CorroborationConfirmed != 2 ||
		noOpFinalized.PrimarySuffix != 0 ||
		noOpFinalized.PendingRollback != nil ||
		noOpFinalized.PreviousRowDigest == nil ||
		*noOpFinalized.PreviousRowDigest != noOpWritten.RowDigest ||
		!sameAuthorityIdentity(noOpFinalized, noOpWritten) {
		t.Fatalf("unexpected finalized no-op rollback: %#v", noOpFinalized)
	}
	if err := bindAuthorityEvidence(
		noOpFinalized,
		noOpRows,
		noOpRows,
	); err != nil {
		t.Fatal(err)
	}
	finalRoundTrip, err := integrationAuthorityDBRow(noOpFinalized).record()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(finalRoundTrip, noOpFinalized) {
		t.Fatal("finalized no-op DB round trip changed record")
	}
}

func TestCanonicalIntegrationAuthoritySerialization(t *testing.T) {
	fixture := newFixture()
	records, evidence := canonicalIntegrationAuthority(t, fixture)
	kinds := []string{
		"initialize",
		"physical_adoption",
		"physical_adoption",
		"bootstrap_agreed",
	}
	if len(records) != len(kinds) || len(evidence) != 2 {
		t.Fatalf("records=%d evidence=%d", len(records), len(evidence))
	}
	expectedPhysical := []authorityHead{
		{Point: authorityPoint{Hash: authorityHash(fixture.start)}},
		{
			EventSeq: 1,
			Point: authorityPoint{
				Slot:        1,
				Hash:        authorityHash(fixture.block1),
				BlockNumber: 1,
			},
		},
		{
			EventSeq: 2,
			Point: authorityPoint{
				Slot:        2,
				Hash:        authorityHash(fixture.block2),
				BlockNumber: 2,
			},
		},
		{
			EventSeq: 2,
			Point: authorityPoint{
				Slot:        2,
				Hash:        authorityHash(fixture.block2),
				BlockNumber: 2,
			},
		},
	}
	expectedGeneration := []uint64{1, 1, 1, 2}
	for index, record := range records {
		if record.Revision != uint64(index+1) ||
			record.TransitionKind != kinds[index] ||
			record.Physical != expectedPhysical[index] ||
			record.VisibilityGeneration != expectedGeneration[index] ||
			record.WriterID == nil ||
			*record.WriterID != integrationAuthorityWriterID() {
			t.Fatalf("record %d = %#v", index, record)
		}
		if index > 0 &&
			(record.PreviousRowDigest == nil ||
				*record.PreviousRowDigest != records[index-1].RowDigest ||
				!sameAuthorityIdentity(record, records[index-1])) {
			t.Fatalf("record %d is not digest-linked", index)
		}
		roundTrip, err := integrationAuthorityDBRow(record).record()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(roundTrip, record) {
			t.Fatalf("authority DB round trip changed record %d", index)
		}
	}
	for index, row := range evidence {
		roundTrip, err := integrationAuthorityObservationDBRow(row).row()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(roundTrip, row) {
			t.Fatalf("authority evidence DB round trip changed row %d", index)
		}
	}
}

func insertCanonicalAuthorityRevision(
	t *testing.T,
	store *Store,
	record authorityRecord,
	evidence []authorityObservationRow,
) {
	t.Helper()
	raw := integrationAuthorityDBRow(record)
	roundTrip, err := raw.record()
	if err != nil {
		t.Fatalf("round-trip canonical authority revision: %v", err)
	}
	if !reflect.DeepEqual(roundTrip, record) {
		t.Fatalf("authority DB serializer changed revision %d", record.Revision)
	}
	insertIntegrationAuthorityEvidence(t, store, evidence)
	batch, err := store.conn.PrepareBatch(
		context.Background(),
		"INSERT INTO dataset_manifest",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.AppendStruct(&raw); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

func integrationAuthorityTime() time.Time {
	return time.Date(2026, 7, 23, 18, 0, 0, 123456000, time.UTC)
}

const (
	integrationWriterUUID       = "22222222-2222-4222-8222-222222222222"
	integrationNoOpRollbackUUID = "55555555-5555-4555-8555-555555555555"
)

func integrationAuthorityWriterID() [16]byte {
	return authorityUUID(uuid.MustParse(integrationWriterUUID))
}

func integrationBlockTime(blockNumber uint64) time.Time {
	return integrationAuthorityTime().Add(time.Duration(blockNumber) * time.Second)
}

func integrationAuthorityPointDBValues(point authorityPoint) (
	origin bool,
	slot *uint64,
	hash *string,
	number *uint64,
	isByronEBB bool,
) {
	origin = point.Origin
	if point.Origin {
		return
	}
	slotValue := point.Slot
	hashValue := string(point.Hash[:])
	numberValue := point.BlockNumber
	return false, &slotValue, &hashValue, &numberValue, point.IsByronEBB
}

func integrationAuthorityOptionalHeadDBValues(head *authorityHead) (
	eventSeq *uint64,
	origin *bool,
	slot *uint64,
	hash *string,
	number *uint64,
	isByronEBB *bool,
) {
	if head == nil {
		return
	}
	eventValue := head.EventSeq
	originValue, slot, hash, number, ebbValue :=
		integrationAuthorityPointDBValues(head.Point)
	return &eventValue, &originValue, slot, hash, number, &ebbValue
}

func integrationAuthorityUUIDPointer(value *[16]byte) *uuid.UUID {
	if value == nil {
		return nil
	}
	result := uuid.UUID(*value)
	return &result
}

func integrationAuthorityDBRow(record authorityRecord) authorityDBRow {
	startKind := "intersection"
	startOrigin, startSlot, startHash, startNumber, startEBB :=
		integrationAuthorityPointDBValues(record.Start)
	if startOrigin {
		startKind = "origin"
	}
	checkedEvent, checkedOrigin, checkedSlot, checkedHash, checkedNumber, checkedEBB :=
		integrationAuthorityOptionalHeadDBValues(record.Checked)
	agreedEvent, agreedOrigin, agreedSlot, agreedHash, agreedNumber, agreedEBB :=
		integrationAuthorityOptionalHeadDBValues(record.LastAgreed)
	var (
		lastCheckedEvent  *uint64
		lastCheckedOrigin *bool
		lastCheckedSlot   *uint64
		lastCheckedHash   *string
		lastCheckedNumber *uint64
		lastCheckedEBB    *bool
	)
	if record.LastAgreedEvidence != nil {
		checked := record.LastAgreedEvidence.Checked
		lastCheckedEvent, lastCheckedOrigin, lastCheckedSlot, lastCheckedHash,
			lastCheckedNumber, lastCheckedEBB =
			integrationAuthorityOptionalHeadDBValues(&checked)
	}
	floorOrigin, floorSlot, floorHash, floorNumber, floorEBB :=
		integrationAuthorityPointDBValues(record.ServableFloor.Point)
	physicalOrigin, physicalSlot, physicalHash, physicalNumber, physicalEBB :=
		integrationAuthorityPointDBValues(record.Physical.Point)
	effectiveOrigin, effectiveSlot, effectiveHash, effectiveNumber, effectiveEBB :=
		integrationAuthorityPointDBValues(record.Effective.Point)
	raw := authorityDBRow{
		ManifestKey:                      record.ManifestKey,
		Revision:                         record.Revision,
		TransitionID:                     uuid.UUID(record.TransitionID),
		TransitionKind:                   record.TransitionKind,
		RowDigest:                        string(record.RowDigest[:]),
		DatasetID:                        uuid.UUID(record.DatasetID),
		SchemaContractHash:               string(record.SchemaContractHash[:]),
		NetworkMagic:                     record.NetworkMagic,
		NetworkName:                      record.NetworkName,
		ByronGenesisID:                   string(record.ByronGenesisID[:]),
		ByronGenesisJSONHash:             string(record.ByronGenesisJSONHash[:]),
		ShelleyGenesisID:                 string(record.ShelleyGenesisID[:]),
		ShelleyGenesisJSONHash:           string(record.ShelleyGenesisJSONHash[:]),
		StartKind:                        startKind,
		StartSlot:                        startSlot,
		StartHash:                        startHash,
		StartBlockNumber:                 startNumber,
		StartIsByronEBB:                  startEBB,
		GenesisSeeded:                    record.GenesisSeeded,
		CompleteHistory:                  record.CompleteHistory,
		TrustMode:                        record.TrustMode,
		TrustStatus:                      record.TrustStatus,
		TrustBasis:                       record.TrustBasis,
		CheckID:                          integrationAuthorityUUIDPointer(record.CheckID),
		AgreementGroup:                   integrationAuthorityUUIDPointer(record.AgreementGroup),
		CheckAttempt:                     record.CheckAttempt,
		CorroborationRequired:            record.CorroborationRequired,
		CorroborationConfirmed:           record.CorroborationConfirmed,
		CheckpointInterval:               record.CheckpointInterval,
		PrimarySuffix:                    record.PrimarySuffix,
		Disagreement:                     record.Disagreement,
		TrustReason:                      record.TrustReason,
		CheckStartedAt:                   record.CheckStartedAt,
		CheckCompletedAt:                 record.CheckCompletedAt,
		EvidenceState:                    record.EvidenceState,
		EvidenceCount:                    record.EvidenceCount,
		CheckedEventSeq:                  checkedEvent,
		CheckedPointOrigin:               checkedOrigin,
		CheckedPointSlot:                 checkedSlot,
		CheckedPointHash:                 checkedHash,
		CheckedPointBlockNumber:          checkedNumber,
		CheckedPointIsByronEBB:           checkedEBB,
		LastAgreedEventSeq:               agreedEvent,
		LastAgreedPointOrigin:            agreedOrigin,
		LastAgreedPointSlot:              agreedSlot,
		LastAgreedPointHash:              agreedHash,
		LastAgreedPointBlockNumber:       agreedNumber,
		LastAgreedPointIsByronEBB:        agreedEBB,
		LastAgreedAt:                     record.LastAgreedAt,
		LastAgreedCheckedEventSeq:        lastCheckedEvent,
		LastAgreedCheckedOrigin:          lastCheckedOrigin,
		LastAgreedCheckedSlot:            lastCheckedSlot,
		LastAgreedCheckedHash:            lastCheckedHash,
		LastAgreedCheckedNumber:          lastCheckedNumber,
		LastAgreedCheckedIsByronEBB:      lastCheckedEBB,
		ServableFloorEventSeq:            record.ServableFloor.EventSeq,
		ServableFloorOrigin:              floorOrigin,
		ServableFloorSlot:                floorSlot,
		ServableFloorHash:                floorHash,
		ServableFloorBlockNumber:         floorNumber,
		ServableFloorIsByronEBB:          floorEBB,
		ServableFloorPermanent:           record.ServableFloorPermanent,
		PhysicalEventSeq:                 record.Physical.EventSeq,
		PhysicalTipOrigin:                physicalOrigin,
		PhysicalTipSlot:                  physicalSlot,
		PhysicalTipHash:                  physicalHash,
		PhysicalTipBlockNumber:           physicalNumber,
		PhysicalTipIsByronEBB:            physicalEBB,
		EffectiveEventSeq:                record.Effective.EventSeq,
		EffectiveTipOrigin:               effectiveOrigin,
		EffectiveTipSlot:                 effectiveSlot,
		EffectiveTipHash:                 effectiveHash,
		EffectiveTipBlockNumber:          effectiveNumber,
		EffectiveTipIsByronEBB:           effectiveEBB,
		Servable:                         record.Servable,
		VisibilityGeneration:             record.VisibilityGeneration,
		PendingRollbackState:             "none",
		PendingRollbackObservedPeers:     []string{},
		PendingRollbackObservedOperators: []string{},
		WriterID:                         integrationAuthorityUUIDPointer(record.WriterID),
		WriterBuild:                      record.WriterBuild,
		SourceBuild:                      record.SourceBuild,
		CreatedAt:                        record.CreatedAt,
		UpdatedAt:                        record.UpdatedAt,
	}
	if record.PreviousRowDigest != nil {
		value := string(record.PreviousRowDigest[:])
		raw.PreviousRowDigest = &value
	}
	if record.EvidenceDigest != nil {
		value := string(record.EvidenceDigest[:])
		raw.EvidenceDigest = &value
	}
	if record.LastAgreedEvidence != nil {
		reference := record.LastAgreedEvidence
		raw.LastAgreedCheckID = integrationAuthorityUUIDPointer(&reference.CheckID)
		raw.LastAgreedAgreementGroup = integrationAuthorityUUIDPointer(&reference.Group)
		raw.LastAgreedCheckAttempt = reference.Attempt
		raw.LastAgreedRequired = reference.Required
		raw.LastAgreedConfirmed = reference.Confirmed
		raw.LastAgreedEvidenceCount = reference.Count
		value := string(reference.Digest[:])
		raw.LastAgreedEvidenceDigest = &value
	}
	if record.PendingEvidenceWrite != nil {
		pending := record.PendingEvidenceWrite
		raw.PendingEvidenceID = integrationAuthorityUUIDPointer(&pending.Observation.ID)
		value := string(pending.Digest[:])
		raw.PendingEvidenceDigest = &value
		raw.PendingEvidencePayload = pending.Payload
		raw.PendingEvidenceWriter = integrationAuthorityUUIDPointer(&pending.WriterID)
		reservedAt := pending.ReservedAt
		raw.PendingEvidenceAt = &reservedAt
	}
	if record.PendingRollback != nil {
		pending := record.PendingRollback
		toOrigin, toSlot, toHash, toNumber, toEBB :=
			integrationAuthorityPointDBValues(pending.To)
		oldOrigin, oldSlot, oldHash, oldNumber, oldEBB :=
			integrationAuthorityPointDBValues(pending.OldPhysical.Point)
		event := pending.EventSeq
		oldEvent := pending.OldPhysical.EventSeq
		depth := pending.Depth
		required := pending.Required
		attempt := pending.CheckAttempt
		checkedEvent := pending.CheckedEventSeq
		evidenceCount := pending.EvidenceCount
		evidenceDigest := string(pending.EvidenceDigest[:])
		startedAt := pending.StartedAt
		raw.PendingRollbackState = pending.State
		raw.PendingRollbackID = integrationAuthorityUUIDPointer(&pending.ID)
		raw.PendingRollbackEventSeq = &event
		raw.PendingRollbackToOrigin = &toOrigin
		raw.PendingRollbackToSlot = toSlot
		raw.PendingRollbackToHash = toHash
		raw.PendingRollbackToBlockNumber = toNumber
		raw.PendingRollbackToIsByronEBB = &toEBB
		raw.PendingRollbackOldPhysicalEventSeq = &oldEvent
		raw.PendingRollbackOldPhysicalOrigin = &oldOrigin
		raw.PendingRollbackOldPhysicalSlot = oldSlot
		raw.PendingRollbackOldPhysicalHash = oldHash
		raw.PendingRollbackOldPhysicalBlockNumber = oldNumber
		raw.PendingRollbackOldPhysicalIsByronEBB = &oldEBB
		raw.PendingRollbackDepth = &depth
		raw.PendingRollbackReason = pending.Reason
		raw.PendingRollbackObservedPeers = append([]string(nil), pending.Peers...)
		raw.PendingRollbackObservedOperators = append([]string(nil), pending.Operators...)
		raw.PendingRollbackRequired = &required
		raw.PendingRollbackCheckID = integrationAuthorityUUIDPointer(&pending.CheckID)
		raw.PendingRollbackAgreementGroup = integrationAuthorityUUIDPointer(&pending.Group)
		raw.PendingRollbackCheckAttempt = &attempt
		raw.PendingRollbackCheckedEventSeq = &checkedEvent
		raw.PendingRollbackEvidenceCount = &evidenceCount
		raw.PendingRollbackEvidenceDigest = &evidenceDigest
		raw.PendingRollbackWriterID = integrationAuthorityUUIDPointer(&pending.WriterID)
		raw.PendingRollbackStartedAt = &startedAt
	}
	return raw
}

func integrationAuthorityHashPointer(value *authorityHash) *string {
	if value == nil {
		return nil
	}
	result := string(value[:])
	return &result
}

func integrationAuthorityObservationDBRow(
	row authorityObservationRow,
) authorityObservationDBRow {
	observation := row.Observation
	return authorityObservationDBRow{
		ObservationID:      uuid.UUID(observation.ID),
		ObservationDigest:  string(row.Digest[:]),
		EvidenceIdentity:   string(observation.EvidenceIdentity[:]),
		Kind:               observation.Kind,
		PeerHost:           observation.PeerHost,
		PeerAddress:        observation.PeerAddress,
		Operator:           observation.Operator,
		OperatorKey:        strings.ToLower(strings.TrimSpace(observation.Operator)),
		N2NVersion:         observation.N2NVersion,
		NetworkMagic:       observation.NetworkMagic,
		TipSlot:            observation.TipSlot,
		TipHash:            string(observation.TipHash[:]),
		TipBlockNumber:     observation.TipBlockNumber,
		CheckpointSlot:     observation.CheckpointSlot,
		CheckpointHash:     integrationAuthorityHashPointer(observation.CheckpointHash),
		CheckpointNumber:   observation.CheckpointBlockNumber,
		CheckpointIsByron:  observation.CheckpointIsByronEBB,
		CheckID:            uuid.UUID(observation.CheckID),
		AgreementGroup:     uuid.UUID(observation.AgreementGroup),
		CheckAttempt:       observation.CheckAttempt,
		EvidenceOrdinal:    observation.EvidenceOrdinal,
		ProofMethod:        observation.ProofMethod,
		Required:           observation.CorroborationRequired,
		CheckedEventSeq:    observation.CheckedEventSeq,
		CheckedOrigin:      observation.CheckedPointOrigin,
		CheckedSlot:        observation.CheckedPointSlot,
		CheckedHash:        integrationAuthorityHashPointer(observation.CheckedPointHash),
		CheckedBlockNumber: observation.CheckedBlockNumber,
		CheckedIsByronEBB:  observation.CheckedPointIsByronEBB,
		SelectedBodySource: observation.SelectedBodySource,
		BodyHashVerified:   observation.BodyHashVerified,
		PointVerified:      observation.PointVerified,
		ParentVerified:     observation.ParentVerified,
		Result:             observation.Result,
		Reason:             observation.Reason,
		ObservedAt:         observation.ObservedAt,
	}
}

func insertIntegrationAuthorityEvidence(
	t *testing.T,
	store *Store,
	rows []authorityObservationRow,
) {
	t.Helper()
	if len(rows) == 0 {
		return
	}
	batch, err := store.conn.PrepareBatch(
		context.Background(),
		"INSERT INTO peer_observations",
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		raw := integrationAuthorityObservationDBRow(row)
		roundTrip, err := raw.row()
		if err != nil {
			t.Fatalf("round-trip canonical authority evidence: %v", err)
		}
		if !reflect.DeepEqual(roundTrip, row) {
			t.Fatalf(
				"authority evidence DB serializer changed ordinal %d",
				row.Observation.EvidenceOrdinal,
			)
		}
		if err := batch.AppendStruct(&raw); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

func (fixture fixture) insert(t *testing.T, store *Store) authorityRecord {
	t.Helper()
	ctx := context.Background()
	exec := func(query string, args ...any) {
		t.Helper()
		if err := store.conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	writer := integrationWriterUUID
	keyReward := append([]byte{0xe1}, fixture.policy[:]...)
	authorities, evidence := canonicalIntegrationAuthority(t, fixture)
	for _, block := range []struct {
		publication  uint64
		hash         model.Hash32
		parent       *model.Hash32
		slot         uint64
		number       uint64
		transactions uint32
		inputs       uint32
		outputs      uint32
		datums       uint32
		withdrawals  uint32
		redeemers    uint32
		metadata     uint32
	}{
		{fixture.pub1, fixture.block1, &fixture.start, 1, 1, 2, 2, 3, 0, 1, 0, 0},
		{fixture.pub2, fixture.block2, &fixture.block1, 2, 2, 1, 1, 2, 2, 1, 1, 1},
	} {
		blockAt := integrationBlockTime(block.number)
		var parent any
		if block.parent != nil {
			parent = hashArgument(*block.parent)
		}
		exec(`INSERT INTO blocks
            (publication_id,block_hash,parent_hash,slot,block_number,era,block_type,
             transaction_count,input_count,output_count,datum_observation_count,
             withdrawal_count,redeemer_count,metadata_count,synthetic,source_peer,
             source_address,source_operator,n2n_version,network_magic,body_hash_verified,
             transaction_hashes_verified,facts_digest,writer_id,observed_at,inserted_at)
	            VALUES (?,?,?, ?,?,'conway',0,?,?,?,?,?,?,?,false,'peer','127.0.0.1',
	                    'test',15,764824073,true,true,?,toUUID(?),?,?)`,
			block.publication, hashArgument(block.hash), parent, block.slot, block.number,
			block.transactions, block.inputs, block.outputs, block.datums,
			block.withdrawals, block.redeemers, block.metadata,
			hashArgument(block.hash), writer, blockAt, blockAt)
		exec(`INSERT INTO chain_events
            (event_seq,publication_id,event_kind,active,rollback_id,block_hash,slot,
             block_number,is_byron_ebb,writer_id,recorded_at)
	            VALUES (?,?, 'adoption',true,NULL,?,?,?,false,toUUID(?),?)`,
			block.number, block.publication, hashArgument(block.hash), block.slot,
			block.number, writer, blockAt)
	}
	for index, authority := range authorities {
		var revisionEvidence []authorityObservationRow
		if index == len(authorities)-1 {
			revisionEvidence = evidence
		}
		insertCanonicalAuthorityRevision(t, store, authority, revisionEvidence)
	}
	exec(`INSERT INTO transactions
        (publication_id,block_number,tx_hash,tx_order,parent_tx_hash,subtransaction_index,
         era,phase2_valid,flow_kind,declared_fee_lovelace,effective_fee_lovelace,
         mint_is_applied,mint_policy_ids,mint_asset_names,mint_quantities,
         regular_input_count,collateral_input_count,reference_input_count,
         produced_output_count,withdrawal_count,redeemer_count,metadata_present,
         datum_observation_count)
        VALUES (101,1,?,0,NULL,NULL,'conway',true,'regular',0,0,true,[],[],[],
                0,0,0,2,0,0,false,0)`, hashArgument(fixture.txA))
	exec(`INSERT INTO transactions
        (publication_id,block_number,tx_hash,tx_order,parent_tx_hash,subtransaction_index,
         era,phase2_valid,flow_kind,declared_fee_lovelace,effective_fee_lovelace,
         mint_is_applied,mint_policy_ids,mint_asset_names,mint_quantities,
         regular_input_count,collateral_input_count,reference_input_count,
         produced_output_count,withdrawal_count,redeemer_count,metadata_present,
         datum_observation_count)
        VALUES (102,2,?,0,NULL,NULL,'conway',true,'regular',200000,200000,true,[?],[?],[7],
                1,0,0,2,1,1,true,2)`, hashArgument(fixture.txB), string(fixture.policy[:]), string([]byte{0x00, 0xff}))
	exec(`INSERT INTO transactions
        (publication_id,block_number,tx_hash,tx_order,parent_tx_hash,subtransaction_index,
         era,phase2_valid,flow_kind,declared_fee_lovelace,effective_fee_lovelace,
         mint_is_applied,mint_policy_ids,mint_asset_names,mint_quantities,
         regular_input_count,collateral_input_count,reference_input_count,
         produced_output_count,withdrawal_count,redeemer_count,metadata_present,
         datum_observation_count)
        VALUES (101,1,?,1,NULL,NULL,'conway',false,'collateral',200000,300000,
                false,[?],[?],[99],1,1,0,1,1,0,false,0)`,
		hashArgument(fixture.txC), string(fixture.policy[:]), string([]byte{0x00, 0xff}))
	exec(`INSERT INTO inputs
        (publication_id,block_number,tx_hash,tx_order,source_tx_hash,source_output_index,
         body_ordinal,role,is_consumed,source_is_resolved)
        VALUES (102,2,?,0,?,0,0,'regular',true,true)`,
		hashArgument(fixture.txB), hashArgument(fixture.txA))
	exec(`INSERT INTO inputs
        (publication_id,block_number,tx_hash,tx_order,source_tx_hash,source_output_index,
         body_ordinal,role,is_consumed,source_is_resolved)
        VALUES
        (101,1,?,1,?,0,0,'regular',false,false),
        (101,1,?,1,?,1,0,'collateral',true,false)`,
		hashArgument(fixture.txC), hashArgument(fixture.txA),
		hashArgument(fixture.txC), hashArgument(fixture.txA))
	exec(`INSERT INTO outputs
        (publication_id,block_number,tx_hash,tx_order,output_index,body_ordinal,output_kind,
         address,payment_credential_kind,payment_credential_hash,
         lovelace,asset_policy_ids,asset_names,asset_quantities,datum_kind,datum_hash,
         reference_script_hash,reference_script_language)
        VALUES (101,1,?,0,0,0,'regular',?,'script',?,5000000,[?],[?],[9],
                'none',NULL,NULL,NULL)`,
		hashArgument(fixture.txA), string(fixture.addressA), string(fixture.policy[:]),
		string(fixture.policy[:]), string([]byte{0x00, 0xff}))
	exec(`INSERT INTO outputs
        (publication_id,block_number,tx_hash,tx_order,output_index,body_ordinal,output_kind,
         address,payment_credential_kind,payment_credential_hash,
         lovelace,asset_policy_ids,asset_names,asset_quantities,datum_kind,datum_hash,
         reference_script_hash,reference_script_language)
        VALUES (101,1,?,0,1,1,'regular',?,'script',?,1000000,[],[],[],
                'none',NULL,NULL,NULL)`,
		hashArgument(fixture.txA), string(fixture.addressA), string(fixture.policy[:]))
	exec(`INSERT INTO outputs
        (publication_id,block_number,tx_hash,tx_order,output_index,body_ordinal,output_kind,
         address,payment_credential_kind,payment_credential_hash,
         lovelace,asset_policy_ids,asset_names,asset_quantities,datum_kind,datum_hash,
         reference_script_hash,reference_script_language)
        VALUES (102,2,?,0,0,0,'regular',?,'key',?,5600000,[?],[?],[16],
                'inline',?,?, 'plutus_v2')`,
		hashArgument(fixture.txB), string(fixture.addressB), string(fixture.policy[:]),
		string(fixture.policy[:]), string([]byte{0x00, 0xff}),
		hashArgument(fixture.datumHash), string(fixture.policy[:]))
	exec(`INSERT INTO outputs
        (publication_id,block_number,tx_hash,tx_order,output_index,body_ordinal,output_kind,
         address,payment_credential_kind,payment_credential_hash,
         lovelace,asset_policy_ids,asset_names,asset_quantities,datum_kind,datum_hash,
         reference_script_hash,reference_script_language)
        VALUES (102,2,?,0,1,1,'regular',?,'key',?,1000000,[],[],[],
                'inline',?,NULL,NULL)`,
		hashArgument(fixture.txB), string(fixture.addressB), string(fixture.policy[:]),
		hashArgument(fixture.datumHash))
	exec(`INSERT INTO outputs
        (publication_id,block_number,tx_hash,tx_order,output_index,body_ordinal,output_kind,
         address,payment_credential_kind,payment_credential_hash,
         lovelace,asset_policy_ids,asset_names,asset_quantities,datum_kind,datum_hash,
         reference_script_hash,reference_script_language)
        VALUES (101,1,?,1,0,0,'collateral_return',?,'script',?,700000,[],[],[],
                'none',NULL,NULL,NULL)`,
		hashArgument(fixture.txC), string(fixture.addressA), string(fixture.policy[:]))
	exec(`INSERT INTO datum_bodies
        (datum_hash,datum_cbor,byte_length,content_hash,first_publication_id,first_seen_at)
        VALUES (?,?,?, ?,102,now64(6))`,
		hashArgument(fixture.datumHash), string(fixture.datumCBOR), len(fixture.datumCBOR), hashArgument(fixture.datumHash))
	exec(`INSERT INTO datum_observations
        (publication_id,block_number,datum_hash,tx_hash,tx_order,source_kind,source_ordinal,
         output_index)
        VALUES (102,2,?,?,0,'inline_output',0,0)`,
		hashArgument(fixture.datumHash), hashArgument(fixture.txB))
	exec(`INSERT INTO datum_observations
        (publication_id,block_number,datum_hash,tx_hash,tx_order,source_kind,source_ordinal,
         output_index)
        VALUES (102,2,?,?,0,'inline_output',1,1)`,
		hashArgument(fixture.datumHash), hashArgument(fixture.txB))
	exec(`INSERT INTO withdrawals
        (publication_id,block_number,tx_hash,tx_order,body_ordinal,reward_account,lovelace,
         is_applied,credential_kind,credential_hash)
        VALUES (102,2,?,0,0,?,123,true,'key',?)`,
		hashArgument(fixture.txB), string(keyReward), string(fixture.policy[:]))
	exec(`INSERT INTO withdrawals
        (publication_id,block_number,tx_hash,tx_order,body_ordinal,reward_account,lovelace,
         is_applied,credential_kind,credential_hash)
        VALUES (101,1,?,1,0,?,999,false,'key',?)`,
		hashArgument(fixture.txC), string(keyReward), string(fixture.policy[:]))
	exec(`INSERT INTO redeemers
        (publication_id,block_number,tx_hash,tx_order,raw_purpose_tag,purpose,redeemer_index,
         data_cbor,data_byte_length,data_hash,ex_units_memory,ex_units_steps,is_applied,
         resolution_status,target_tx_hash,target_output_index,target_policy_id,
         target_reward_account,target_body_ordinal,target_identity,resolved_script_hash)
        VALUES (102,2,?,0,0,'spend',0,?,?,?,10,20,true,'resolved',?,0,NULL,NULL,NULL,NULL,?)`,
		hashArgument(fixture.txB), string(fixture.redeemerCBOR), len(fixture.redeemerCBOR),
		hashArgument(fixture.dataHash), hashArgument(fixture.txA), string(fixture.policy[:]))
	exec(`INSERT INTO transaction_metadata
        (publication_id,block_number,tx_hash,tx_order,labels,metadata_cbor,byte_length,
         content_hash)
		VALUES (102,2,?,0,[1],?,?,?)`,
		hashArgument(fixture.txB), string(fixture.metadataCBOR), len(fixture.metadataCBOR), hashArgument(fixture.metadataHash))
	return authorities[len(authorities)-1]
}

func (fixture fixture) insertUnpublishedAddressCandidate(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()
	writer := "22222222-2222-4222-8222-222222222222"
	block := repeatedHash(0x09)
	tx := repeatedHash(0x01)
	missingDatum := repeatedHash(0xaa)
	if err := store.conn.Exec(ctx, `INSERT INTO blocks
        (publication_id,block_hash,parent_hash,slot,block_number,era,block_type,
         transaction_count,input_count,output_count,datum_observation_count,
         withdrawal_count,redeemer_count,metadata_count,synthetic,source_peer,
         source_address,source_operator,n2n_version,network_magic,body_hash_verified,
         transaction_hashes_verified,facts_digest,writer_id,observed_at,inserted_at)
        VALUES (50,?,NULL,0,0,'conway',0,1,0,1,0,0,0,0,false,'peer',
                '127.0.0.1','test',15,764824073,true,true,?,toUUID(?),
                now64(6),now64(6))`,
		hashArgument(block),
		hashArgument(block),
		writer,
	); err != nil {
		t.Fatal(err)
	}
	if err := store.conn.Exec(ctx, `INSERT INTO outputs
        (publication_id,block_number,tx_hash,tx_order,output_index,body_ordinal,
         output_kind,address,payment_credential_kind,payment_credential_hash,
         lovelace,asset_policy_ids,asset_names,asset_quantities,datum_kind,datum_hash,
         reference_script_hash,reference_script_language)
        VALUES (50,0,?,0,0,0,'regular',?,'script',?,1,[],[],[],
                'inline',?,NULL,NULL)`,
		hashArgument(tx),
		string(fixture.addressA),
		string(fixture.policy[:]),
		hashArgument(missingDatum),
	); err != nil {
		t.Fatal(err)
	}
}

func (fixture fixture) insertPendingRollbackReservation(
	t *testing.T,
	store *Store,
	previous authorityRecord,
) authorityRecord {
	t.Helper()
	pending, evidence := canonicalIntegrationPendingRollback(t, fixture, previous)
	insertIntegrationAuthorityEvidence(t, store, evidence)
	rollback := pending.PendingRollback
	if rollback == nil {
		t.Fatal("canonical pending rollback has no reservation")
	}
	insertCanonicalAuthorityRevision(t, store, pending, nil)
	return pending
}

func (fixture fixture) insertPendingRollbackInvalidations(
	t *testing.T,
	store *Store,
	reserved authorityRecord,
) authorityRecord {
	t.Helper()
	rollback := reserved.PendingRollback
	if rollback == nil || rollback.State != "reserved" {
		t.Fatal("pending invalidation insert lacks reserved authority")
	}
	if err := store.conn.Exec(context.Background(), `INSERT INTO chain_events
		        (event_seq,publication_id,event_kind,active,rollback_id,block_hash,slot,
		         block_number,is_byron_ebb,writer_id,recorded_at)
		        VALUES (?,?, 'invalidation',false,toUUID(?),?,2,2,false,
		                toUUID(?),fromUnixTimestamp64Micro(?))`,
		rollback.EventSeq,
		fixture.pub2,
		uuid.UUID(rollback.ID).String(),
		hashArgument(fixture.block2),
		uuid.UUID(rollback.WriterID).String(),
		rollback.StartedAt.UnixMicro(),
	); err != nil {
		t.Fatal(err)
	}
	written := canonicalIntegrationInvalidationsWritten(t, reserved)
	insertCanonicalAuthorityRevision(t, store, written, nil)
	return written
}

func (fixture fixture) insertRollbackHeader(
	t *testing.T,
	store *Store,
	written authorityRecord,
) {
	t.Helper()
	if written.PendingRollback == nil ||
		written.PendingRollback.State != "invalidations_written" {
		t.Fatal("rollback header lacks invalidations-written authority")
	}
	pending := *written.PendingRollback
	raw := authorityPhysicalRollbackFromPending(pending)
	decoded, to, oldTip, digest, found, err :=
		decodeAuthorityPhysicalRollbackRows(
			[]authorityPhysicalRollbackRow{raw},
			pending.EventSeq,
		)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("canonical rollback header did not decode")
	}
	if err := validateAuthorityPendingRollbackHeader(
		pending,
		decoded,
		to,
		oldTip,
		digest,
	); err != nil {
		t.Fatal(err)
	}
	batch, err := store.conn.PrepareBatch(
		context.Background(),
		"INSERT INTO rollbacks",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.AppendStruct(&raw); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

func (fixture fixture) freshReadopt(
	t *testing.T,
	store *Store,
	previous authorityRecord,
) authorityRecord {
	t.Helper()
	if previous.WriterID == nil ||
		*previous.WriterID != integrationAuthorityWriterID() {
		t.Fatal("re-adoption predecessor has a different writer")
	}
	ctx := context.Background()
	insertedAt := integrationBlockTime(6)
	exec := func(query string, args ...any) {
		t.Helper()
		if err := store.conn.Exec(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`INSERT INTO blocks
        (publication_id,block_hash,parent_hash,slot,block_number,era,block_type,
         transaction_count,input_count,output_count,datum_observation_count,
         withdrawal_count,redeemer_count,metadata_count,synthetic,source_peer,
	         source_address,source_operator,n2n_version,network_magic,body_hash_verified,
	         transaction_hashes_verified,facts_digest,writer_id,observed_at,inserted_at)
        SELECT ?,block_hash,parent_hash,slot,block_number,era,block_type,
         transaction_count,input_count,output_count,datum_observation_count,
         withdrawal_count,redeemer_count,metadata_count,synthetic,source_peer,
	         source_address,source_operator,n2n_version,network_magic,body_hash_verified,
	         transaction_hashes_verified,facts_digest,writer_id,observed_at,
	         fromUnixTimestamp64Micro(?)
	        FROM blocks WHERE publication_id = ?`,
		fixture.pub3, insertedAt.UnixMicro(), fixture.pub2)
	exec(`INSERT INTO transactions
        (publication_id,block_number,tx_hash,tx_order,parent_tx_hash,subtransaction_index,
         era,phase2_valid,flow_kind,declared_fee_lovelace,effective_fee_lovelace,
         mint_is_applied,mint_policy_ids,mint_asset_names,mint_quantities,
         regular_input_count,collateral_input_count,reference_input_count,
         produced_output_count,withdrawal_count,redeemer_count,metadata_present,
         datum_observation_count)
        SELECT ?,block_number,tx_hash,tx_order,parent_tx_hash,subtransaction_index,
         era,phase2_valid,flow_kind,declared_fee_lovelace,effective_fee_lovelace,
         mint_is_applied,mint_policy_ids,mint_asset_names,mint_quantities,
         regular_input_count,collateral_input_count,reference_input_count,
         produced_output_count,withdrawal_count,redeemer_count,metadata_present,
         datum_observation_count
        FROM transactions WHERE publication_id = ?`,
		fixture.pub3, fixture.pub2)
	exec(`INSERT INTO inputs
        (publication_id,block_number,tx_hash,tx_order,source_tx_hash,
         source_output_index,body_ordinal,role,is_consumed,source_is_resolved)
        SELECT ?,block_number,tx_hash,tx_order,source_tx_hash,
         source_output_index,body_ordinal,role,is_consumed,source_is_resolved
        FROM inputs WHERE publication_id = ?`,
		fixture.pub3, fixture.pub2)
	exec(`INSERT INTO outputs
        (publication_id,block_number,tx_hash,tx_order,output_index,body_ordinal,
         output_kind,address,payment_credential_kind,payment_credential_hash,
         lovelace,asset_policy_ids,asset_names,asset_quantities,datum_kind,datum_hash,
         reference_script_hash,reference_script_language)
        SELECT ?,block_number,tx_hash,tx_order,output_index,body_ordinal,
         output_kind,address,payment_credential_kind,payment_credential_hash,
         lovelace,asset_policy_ids,asset_names,asset_quantities,datum_kind,datum_hash,
         reference_script_hash,reference_script_language
        FROM outputs WHERE publication_id = ?`,
		fixture.pub3, fixture.pub2)
	exec(`INSERT INTO withdrawals
        (publication_id,block_number,tx_hash,tx_order,body_ordinal,reward_account,
         lovelace,is_applied,credential_kind,credential_hash)
        SELECT ?,block_number,tx_hash,tx_order,body_ordinal,reward_account,
         lovelace,is_applied,credential_kind,credential_hash
	        FROM withdrawals WHERE publication_id = ?`,
		fixture.pub3, fixture.pub2)
	exec(`INSERT INTO datum_observations
	        (publication_id,block_number,datum_hash,tx_hash,tx_order,source_kind,
	         source_ordinal,output_index)
	        SELECT ?,block_number,datum_hash,tx_hash,tx_order,source_kind,
	         source_ordinal,output_index
	        FROM datum_observations WHERE publication_id = ?`,
		fixture.pub3, fixture.pub2)
	exec(`INSERT INTO redeemers
	        (publication_id,block_number,tx_hash,tx_order,raw_purpose_tag,purpose,
	         redeemer_index,data_cbor,data_byte_length,data_hash,ex_units_memory,
	         ex_units_steps,is_applied,resolution_status,target_tx_hash,
	         target_output_index,target_policy_id,target_reward_account,
	         target_body_ordinal,target_identity,resolved_script_hash)
	        SELECT ?,block_number,tx_hash,tx_order,raw_purpose_tag,purpose,
	         redeemer_index,data_cbor,data_byte_length,data_hash,ex_units_memory,
	         ex_units_steps,is_applied,resolution_status,target_tx_hash,
	         target_output_index,target_policy_id,target_reward_account,
	         target_body_ordinal,target_identity,resolved_script_hash
	        FROM redeemers WHERE publication_id = ?`,
		fixture.pub3, fixture.pub2)
	exec(`INSERT INTO transaction_metadata
	        (publication_id,block_number,tx_hash,tx_order,labels,metadata_cbor,
	         byte_length,content_hash)
	        SELECT ?,block_number,tx_hash,tx_order,labels,metadata_cbor,
	         byte_length,content_hash
	        FROM transaction_metadata WHERE publication_id = ?`,
		fixture.pub3, fixture.pub2)
	exec(`INSERT INTO chain_events
	        (event_seq,publication_id,event_kind,active,rollback_id,block_hash,slot,
	         block_number,is_byron_ebb,writer_id,recorded_at)
	        VALUES (6,?,'adoption',true,NULL,?,2,2,false,
	                toUUID(?),fromUnixTimestamp64Micro(?))`,
		fixture.pub3,
		hashArgument(fixture.block2),
		integrationWriterUUID,
		insertedAt.UnixMicro())
	readopted := canonicalIntegrationReadoption(t, fixture, previous)
	insertCanonicalAuthorityRevision(t, store, readopted, nil)
	return readopted
}

func (fixture fixture) insertNoOpRollbackReservation(
	t *testing.T,
	store *Store,
	previous authorityRecord,
) authorityRecord {
	t.Helper()
	reserved, evidence := canonicalIntegrationNoOpRollback(
		t,
		fixture,
		previous,
	)
	insertIntegrationAuthorityEvidence(t, store, evidence)
	insertCanonicalAuthorityRevision(t, store, reserved, nil)
	return reserved
}

func (fixture fixture) insertNoOpRollbackInvalidationsWritten(
	t *testing.T,
	store *Store,
	reserved authorityRecord,
) authorityRecord {
	t.Helper()
	written := canonicalIntegrationInvalidationsWritten(t, reserved)
	insertCanonicalAuthorityRevision(t, store, written, nil)
	return written
}

func (fixture fixture) insertPostWatermarkShadow(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()
	exec := func(query string, args ...any) {
		t.Helper()
		if err := store.conn.Exec(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	writer := "22222222-2222-4222-8222-222222222222"
	shadowBlock := repeatedHash(0x66)
	exec(`INSERT INTO blocks
        (publication_id,block_hash,parent_hash,slot,block_number,era,block_type,
         transaction_count,input_count,output_count,datum_observation_count,
         withdrawal_count,redeemer_count,metadata_count,synthetic,source_peer,
         source_address,source_operator,n2n_version,network_magic,body_hash_verified,
         transaction_hashes_verified,facts_digest,writer_id,observed_at,inserted_at)
        VALUES (999,?,NULL,4,4,'conway',0,1,0,1,0,0,0,0,false,'peer',
                '127.0.0.1','test',15,764824073,true,true,?,toUUID(?),now64(6),now64(6))`,
		hashArgument(shadowBlock), hashArgument(shadowBlock), writer)
	// This simulates a row becoming visible after the request captured its
	// snapshot tuple. Event filtering alone would accept the backfilled event;
	// the captured publication watermark must exclude it.
	exec(`INSERT INTO chain_events
        (event_seq,publication_id,event_kind,active,rollback_id,block_hash,slot,
         block_number,is_byron_ebb,writer_id,recorded_at)
	        VALUES (5,999,'adoption',true,NULL,?,4,4,false,toUUID(?),now64(6))`,
		hashArgument(shadowBlock), writer)
	exec(`INSERT INTO outputs
        (publication_id,block_number,tx_hash,tx_order,output_index,body_ordinal,
         output_kind,address,payment_credential_kind,payment_credential_hash,
         lovelace,asset_policy_ids,asset_names,asset_quantities,datum_kind,datum_hash,
         reference_script_hash,reference_script_language)
        VALUES (999,4,?,0,0,0,'regular',?,'script',?,999999,[],[],[],
                'none',NULL,NULL,NULL)`,
		hashArgument(fixture.txA),
		string(fixture.addressA),
		string(fixture.policy[:]))
}

func (fixture fixture) inflateIrrelevantHistory(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()
	exec := func(query string) {
		t.Helper()
		if err := store.conn.Exec(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	const writer = "22222222-2222-4222-8222-222222222222"
	exec(`INSERT INTO blocks
        (publication_id,block_hash,parent_hash,slot,block_number,era,block_type,
         transaction_count,input_count,output_count,datum_observation_count,
         withdrawal_count,redeemer_count,metadata_count,synthetic,source_peer,
         source_address,source_operator,n2n_version,network_magic,body_hash_verified,
         transaction_hashes_verified,facts_digest,writer_id,observed_at,inserted_at)
        SELECT
            number + 10000,
            unhex(leftPad(hex(number + 10000), 64, '0')),
            NULL,
            number + 10000,
            number + 10000,
            'conway',
            0,
            0, 1, 0, 0, 0, 0, 0,
            true,
            'inflated', '127.0.0.1', 'inflated',
            15, 764824073, true, true,
            unhex(leftPad(hex(number + 10000), 64, '0')),
            toUUID('` + writer + `'),
            now64(6), now64(6)
        FROM numbers(100000)`)
	exec(`INSERT INTO chain_events
        (event_seq,publication_id,event_kind,active,rollback_id,block_hash,slot,
         block_number,is_byron_ebb,writer_id,recorded_at)
        SELECT
            number + 10000,
            number + 10000,
            'adoption',
            true,
            NULL,
            unhex(leftPad(hex(number + 10000), 64, '0')),
            number + 10000,
            number + 10000,
            false,
            toUUID('` + writer + `'),
            now64(6)
        FROM numbers(100000)`)
	exec(`INSERT INTO inputs
        (publication_id,block_number,tx_hash,tx_order,source_tx_hash,
         source_output_index,body_ordinal,role,is_consumed,source_is_resolved)
        SELECT
            number + 10000,
            number + 10000,
            unhex(leftPad(hex(number + 200000), 64, '0')),
            0,
            unhex(leftPad(hex(number + 300000), 64, '0')),
            0, 0, 'regular', true, true
        FROM numbers(100000)`)
}

func (fixture fixture) insertDuplicateEvent(
	t *testing.T,
	store *Store,
	written authorityRecord,
) {
	t.Helper()
	if written.PendingRollback == nil ||
		written.PendingRollback.State != "invalidations_written" ||
		written.PendingRollback.EventSeq != 7 {
		t.Fatal("duplicate event source is not the canonical event-7 header")
	}
	conflict := authorityPhysicalRollbackFromPending(
		*written.PendingRollback,
	)
	conflict.RollbackID = uuid.MustParse(
		"77777777-7777-4777-8777-777777777777",
	)
	batch, err := store.conn.PrepareBatch(
		context.Background(),
		"INSERT INTO rollbacks",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.AppendStruct(&conflict); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

func (fixture fixture) assertDatumDuplicateSemantics(
	t *testing.T,
	store *Store,
	snapshot model.Snapshot,
) {
	t.Helper()
	ctx := context.Background()
	insert := `INSERT INTO datum_bodies
        (datum_hash,datum_cbor,byte_length,content_hash,first_publication_id,first_seen_at)
        VALUES (?,?,?,?,?,now64(6))`
	if err := store.conn.Exec(
		ctx,
		insert,
		hashArgument(fixture.datumHash),
		string(fixture.datumCBOR),
		len(fixture.datumCBOR),
		hashArgument(fixture.datumHash),
		999,
	); err != nil {
		t.Fatal(err)
	}
	datum, err := store.Datum(ctx, snapshot, fixture.datumHash)
	if err != nil || string(datum.BodyCBOR) != string(fixture.datumCBOR) {
		t.Fatalf("byte-identical lost-response duplicate changed datum: %#v, %v", datum, err)
	}
	conflict := append([]byte(nil), fixture.datumCBOR...)
	conflict[len(conflict)-1] ^= 1
	if err := store.conn.Exec(
		ctx,
		insert,
		hashArgument(fixture.datumHash),
		string(conflict),
		len(conflict),
		hashArgument(fixture.datumHash),
		1000,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Datum(ctx, snapshot, fixture.datumHash); !errors.Is(err, ErrConflictingRow) {
		t.Fatalf("conflicting physical datum body was not rejected: %v", err)
	}
}
