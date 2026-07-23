package clickhouse

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

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
	fixture.insert(t, store)

	snapshot, err := store.Snapshot(ctx, model.AtPoint{Tip: true})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Event != 2 || snapshot.PublicationWatermark != fixture.pub2 ||
		snapshot.CompleteHistory ||
		snapshot.TrustMode != model.TrustPeerObserved {
		t.Fatalf("unexpected snapshot: %#v", snapshot)
	}
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
	page, _, err := store.Address(ctx, snapshot, repository.AddressQuery{
		Address: fixture.addressA,
		State:   "history",
		Limit:   1,
	})
	if err != nil || len(page.Items) != 1 || page.Cursor == "" {
		t.Fatalf("unexpected address page: %#v, %v", page, err)
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
	forward, boundaries, err := store.ExpandForward(
		ctx,
		snapshot,
		[]model.UTxORef{fixture.source},
		model.AssetSelector{ADA: true},
	)
	if err != nil || len(boundaries) != 0 || len(forward) != 1 ||
		forward[0].Transaction != fixture.txB || len(forward[0].ProducedOutputs) != 2 ||
		len(forward[0].AppliedWithdrawals) != 1 || forward[0].FeeSink == nil {
		t.Fatalf("unexpected forward edge: %#v, %#v, %v", forward, boundaries, err)
	}
	reverse, boundaries, err := store.ExpandReverse(
		ctx,
		snapshot,
		[]model.UTxORef{fixture.destination},
		model.AssetSelector{ADA: true},
	)
	if err != nil || len(boundaries) != 0 || len(reverse) != 1 ||
		len(reverse[0].ConsumedInputValues) != 1 ||
		reverse[0].ConsumedInputValues[0].Ref != fixture.source {
		t.Fatalf("unexpected reverse edge: %#v, %#v, %v", reverse, boundaries, err)
	}
	invalidTx, _, err := store.Transaction(ctx, snapshot, fixture.txC)
	if err != nil || invalidTx.Phase2Valid || invalidTx.MintApplied ||
		len(invalidTx.Inputs) != 2 || invalidTx.Inputs[0].IsConsumed ||
		!invalidTx.Inputs[1].IsConsumed || !invalidTx.Inputs[1].SourceResolved {
		t.Fatalf("invalid transaction semantics wrong: %#v, %v", invalidTx, err)
	}
	invalidEdges, _, err := store.ExpandForward(
		ctx,
		snapshot,
		[]model.UTxORef{fixture.collateral},
		model.AssetSelector{ADA: true},
	)
	if err != nil || len(invalidEdges) != 1 || len(invalidEdges[0].MintDeltas) != 0 ||
		len(invalidEdges[0].AppliedWithdrawals) != 0 ||
		invalidEdges[0].FeeSink == nil || invalidEdges[0].FeeSink.Lovelace != 300000 ||
		len(invalidEdges[0].ConsumedInputs) != 1 ||
		invalidEdges[0].ConsumedInputs[0] != fixture.collateral ||
		len(invalidEdges[0].ProducedOutputs) != 1 ||
		invalidEdges[0].ProducedOutputs[0].Kind != model.OutputCollateralReturn {
		t.Fatalf("invalid collateral hyperedge wrong: %#v, %v", invalidEdges, err)
	}

	fixture.insertHeaderlessRollback(t, store)
	snapshot, err = store.Snapshot(ctx, model.AtPoint{Tip: true})
	if err != nil || snapshot.Event != 2 {
		t.Fatalf("headerless invalidation changed snapshot: %#v, %v", snapshot, err)
	}
	fixture.commitRollback(t, store)
	snapshot, err = store.Snapshot(ctx, model.AtPoint{Tip: true})
	if err != nil || snapshot.Event != 3 {
		t.Fatalf("rollback header did not commit snapshot: %#v, %v", snapshot, err)
	}
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

	fixture.readopt(t, store)
	snapshot, err = store.Snapshot(ctx, model.AtPoint{Tip: true})
	if err != nil || snapshot.Event != 6 {
		t.Fatalf("re-adoption failed: %#v, %v", snapshot, err)
	}
	sourceState, _, err = store.UTxO(ctx, snapshot, fixture.source)
	if err != nil || sourceState.IsCurrent {
		t.Fatalf("re-adoption did not restore spend: %#v, %v", sourceState, err)
	}
	fixture.commitNoOpRollback(t, store)
	snapshot, err = store.Snapshot(ctx, model.AtPoint{Tip: true})
	if err != nil || snapshot.Event != 7 {
		t.Fatalf("no-op rollback header did not advance snapshot: %#v, %v", snapshot, err)
	}
	sourceState, _, err = store.UTxO(ctx, snapshot, fixture.source)
	if err != nil || sourceState.IsCurrent {
		t.Fatalf("no-op rollback changed active membership: %#v, %v", sourceState, err)
	}
	pinnedEvent := uint64(7)
	pinned, err := store.Snapshot(ctx, model.AtPoint{Event: &pinnedEvent})
	if err != nil || pinned.Event != 7 || pinned.PublicationWatermark != fixture.pub2 {
		t.Fatalf("depth-zero rollback event was not pinnable: %#v, %v", pinned, err)
	}
	fixture.insertPostWatermarkShadow(t, store)
	sourceState, _, err = store.UTxO(ctx, pinned, fixture.source)
	if err != nil || sourceState.Output.BlockHash != fixture.block1 {
		t.Fatalf("post-watermark row leaked into captured snapshot: %#v, %v", sourceState, err)
	}
	fixture.invalidatePostWatermarkShadow(t, store)

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
	fixture.insertDuplicateEvent(t, store)
	duplicateEvent := uint64(7)
	if _, err := store.Snapshot(
		ctx,
		model.AtPoint{Event: &duplicateEvent},
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
		!strings.Contains(plan, "publication_id in 1-element set") {
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
		snapshot.Event,
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
		!strings.Contains(latestRollbackPlan, "ReadFromMergeTree (clicksync.rollbacks)") {
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
		snapshot.Event,
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
	pub1, pub2     uint64
	block1, block2 model.Hash32
	txA, txB, txC  model.Hash32
	source         model.UTxORef
	collateral     model.UTxORef
	destination    model.UTxORef
	datumHash      model.Hash32
	dataHash       model.Hash32
	metadataHash   model.Hash32
	policy         model.PolicyID
	addressA       []byte
	addressB       []byte
	datumCBOR      []byte
	redeemerCBOR   []byte
	metadataCBOR   []byte
	rollbackID     string
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

func (fixture fixture) insert(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()
	exec := func(query string, args ...any) {
		t.Helper()
		if err := store.conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	writer := "22222222-2222-4222-8222-222222222222"
	dataset := "33333333-3333-4333-8333-333333333333"
	zero := make([]byte, 32)
	keyReward := append([]byte{0xe1}, fixture.policy[:]...)
	exec(`INSERT INTO dataset_manifest
        (manifest_key,revision,dataset_id,network_magic,network_name,
         byron_genesis_id,byron_genesis_json_hash,shelley_genesis_id,shelley_genesis_json_hash,
         start_kind,start_slot,start_hash,start_block_number,start_is_byron_ebb,
         genesis_seeded,complete_history,trust_mode,
         committed_event_seq,committed_tip_origin,committed_tip_slot,committed_tip_hash,
         committed_tip_block_number,committed_tip_is_byron_ebb,
         writer_id,writer_build,source_build,created_at,updated_at)
        VALUES
        (1,1,toUUID(?),764824073,'mainnet',?,?,?,?,
         'intersection',1,?,1,false,false,false,?,
         2,false,2,?,2,false,toUUID(?),'test','test',now64(6),now64(6))`,
		dataset,
		string(zero),
		string(zero),
		string(zero),
		string(zero),
		hashArgument(fixture.block1),
		model.TrustPeerObserved,
		hashArgument(fixture.block2),
		writer,
	)
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
		{fixture.pub1, fixture.block1, nil, 1, 1, 2, 2, 3, 0, 1, 0, 0},
		{fixture.pub2, fixture.block2, &fixture.block1, 2, 2, 1, 1, 2, 2, 1, 1, 1},
	} {
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
                    'test',15,764824073,true,true,?,toUUID(?),now64(6),now64(6))`,
			block.publication, hashArgument(block.hash), parent, block.slot, block.number,
			block.transactions, block.inputs, block.outputs, block.datums,
			block.withdrawals, block.redeemers, block.metadata,
			hashArgument(block.hash), writer)
		exec(`INSERT INTO chain_events
            (event_seq,publication_id,event_kind,active,rollback_id,block_hash,slot,
             block_number,is_byron_ebb,writer_id,recorded_at)
            VALUES (?,?, 'adoption',true,NULL,?,?,?,false,toUUID(?),now64(6))`,
			block.number, block.publication, hashArgument(block.hash), block.slot, block.number, writer)
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
        (101,1,?,1,?,1,1,'collateral',true,false)`,
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
}

func (fixture fixture) insertHeaderlessRollback(t *testing.T, store *Store) {
	t.Helper()
	if err := store.conn.Exec(context.Background(), `INSERT INTO chain_events
        (event_seq,publication_id,event_kind,active,rollback_id,block_hash,slot,
         block_number,is_byron_ebb,writer_id,recorded_at)
        VALUES (3,102,'invalidation',false,toUUID(?),?,2,2,false,
                toUUID('22222222-2222-4222-8222-222222222222'),now64(6))`,
		fixture.rollbackID, hashArgument(fixture.block2)); err != nil {
		t.Fatal(err)
	}
}

func (fixture fixture) commitRollback(t *testing.T, store *Store) {
	t.Helper()
	if err := store.conn.Exec(context.Background(), `INSERT INTO rollbacks
        (rollback_id,event_seq,rollback_to_origin,rollback_to_slot,rollback_to_hash,
         rollback_to_block_number,rollback_to_is_byron_ebb,
         old_tip_slot,old_tip_hash,old_tip_block_number,old_tip_is_byron_ebb,
         depth,reason,observed_peers,observed_operators,corroboration_required,
         agreement_group,writer_id,recorded_at)
        VALUES (toUUID(?),3,false,1,?,1,false,2,?,2,false,1,'test',
                ['peer-a','peer-b'],['operator-a','operator-b'],2,NULL,
                toUUID('22222222-2222-4222-8222-222222222222'),now64(6))`,
		fixture.rollbackID, hashArgument(fixture.block1), hashArgument(fixture.block2)); err != nil {
		t.Fatal(err)
	}
}

func (fixture fixture) readopt(t *testing.T, store *Store) {
	t.Helper()
	if err := store.conn.Exec(context.Background(), `INSERT INTO chain_events
        (event_seq,publication_id,event_kind,active,rollback_id,block_hash,slot,
         block_number,is_byron_ebb,writer_id,recorded_at)
	        VALUES (6,102,'adoption',true,NULL,?,2,2,false,
                toUUID('22222222-2222-4222-8222-222222222222'),now64(6))`,
		hashArgument(fixture.block2)); err != nil {
		t.Fatal(err)
	}
}

func (fixture fixture) commitNoOpRollback(t *testing.T, store *Store) {
	t.Helper()
	if err := store.conn.Exec(context.Background(), `INSERT INTO rollbacks
        (rollback_id,event_seq,rollback_to_origin,rollback_to_slot,rollback_to_hash,
         rollback_to_block_number,rollback_to_is_byron_ebb,
         old_tip_slot,old_tip_hash,old_tip_block_number,old_tip_is_byron_ebb,
         depth,reason,observed_peers,observed_operators,corroboration_required,
         agreement_group,writer_id,recorded_at)
        VALUES (
            toUUID('55555555-5555-4555-8555-555555555555'),
	            7,false,2,?,2,false,2,?,2,false,0,'no-op corroboration',
            ['peer-a','peer-b'],['operator-a','operator-b'],2,NULL,
            toUUID('22222222-2222-4222-8222-222222222222'),now64(6)
        )`,
		hashArgument(fixture.block2),
		hashArgument(fixture.block2),
	); err != nil {
		t.Fatal(err)
	}
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

func (fixture fixture) invalidatePostWatermarkShadow(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()
	const (
		rollbackID = "66666666-6666-4666-8666-666666666666"
		writer     = "22222222-2222-4222-8222-222222222222"
	)
	shadowBlock := repeatedHash(0x66)
	if err := store.conn.Exec(ctx, `INSERT INTO chain_events
        (event_seq,publication_id,event_kind,active,rollback_id,block_hash,slot,
         block_number,is_byron_ebb,writer_id,recorded_at)
	        VALUES (8,999,'invalidation',false,toUUID(?),?,4,4,false,
                toUUID(?),now64(6))`,
		rollbackID, hashArgument(shadowBlock), writer,
	); err != nil {
		t.Fatal(err)
	}
	if err := store.conn.Exec(ctx, `INSERT INTO rollbacks
        (rollback_id,event_seq,rollback_to_origin,rollback_to_slot,rollback_to_hash,
         rollback_to_block_number,rollback_to_is_byron_ebb,
         old_tip_slot,old_tip_hash,old_tip_block_number,old_tip_is_byron_ebb,
         depth,reason,observed_peers,observed_operators,corroboration_required,
         agreement_group,writer_id,recorded_at)
        VALUES (
	            toUUID(?),8,false,2,?,2,false,4,?,4,false,1,'test fixture cleanup',
            ['peer-a','peer-b'],['operator-a','operator-b'],2,NULL,toUUID(?),now64(6)
        )`,
		rollbackID,
		hashArgument(fixture.block2),
		hashArgument(shadowBlock),
		writer,
	); err != nil {
		t.Fatal(err)
	}
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
	exec(`INSERT INTO rollbacks
        (rollback_id,event_seq,rollback_to_origin,rollback_to_slot,rollback_to_hash,
         rollback_to_block_number,rollback_to_is_byron_ebb,
         old_tip_slot,old_tip_hash,old_tip_block_number,old_tip_is_byron_ebb,
         depth,reason,observed_peers,observed_operators,corroboration_required,
         agreement_group,writer_id,recorded_at)
        SELECT
            generateUUIDv4(),
            number + 200000,
            false,
            number + 10000,
            unhex(leftPad(hex(number + 10000), 64, '0')),
            number + 10000,
            false,
            NULL, NULL, NULL, false,
            0,
            'inflated depth-zero',
            ['peer-a','peer-b'],
            ['operator-a','operator-b'],
            2,
            NULL,
            toUUID('` + writer + `'),
            now64(6)
        FROM numbers(50000)`)
}

func (fixture fixture) insertDuplicateEvent(t *testing.T, store *Store) {
	t.Helper()
	if err := store.conn.Exec(context.Background(), `INSERT INTO rollbacks
        (rollback_id,event_seq,rollback_to_origin,rollback_to_slot,rollback_to_hash,
         rollback_to_block_number,rollback_to_is_byron_ebb,
         old_tip_slot,old_tip_hash,old_tip_block_number,old_tip_is_byron_ebb,
         depth,reason,observed_peers,observed_operators,corroboration_required,
         agreement_group,writer_id,recorded_at)
        VALUES (
            toUUID('77777777-7777-4777-8777-777777777777'),
	            7,false,2,?,2,false,NULL,NULL,NULL,false,0,'duplicate event',
            ['peer-a','peer-b'],['operator-a','operator-b'],2,NULL,
            toUUID('22222222-2222-4222-8222-222222222222'),now64(6)
        )`, hashArgument(fixture.block2)); err != nil {
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
