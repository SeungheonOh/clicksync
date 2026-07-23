package clickhouse

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

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
	if snapshot.Event != 2 || snapshot.CompleteHistory ||
		snapshot.TrustMode != model.TrustPeerObserved {
		t.Fatalf("unexpected snapshot: %#v", snapshot)
	}
	sourceState, boundaries, err := store.UTxO(ctx, snapshot, fixture.source)
	if err != nil || len(boundaries) != 0 || sourceState.IsCurrent ||
		sourceState.SpentBy == nil || *sourceState.SpentBy != fixture.txB {
		t.Fatalf("unexpected source state: %#v, %#v, %v", sourceState, boundaries, err)
	}
	transaction, boundaries, err := store.Transaction(ctx, snapshot, fixture.txB)
	if err != nil || len(boundaries) != 0 || len(transaction.Inputs) != 1 ||
		len(transaction.Outputs) != 1 || transaction.Mint[0].Quantity != 7 {
		t.Fatalf("unexpected transaction: %#v, %#v, %v", transaction, boundaries, err)
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
	if err != nil || !datum.BodyVerified || len(datum.ActiveObservations) != 1 ||
		string(datum.BodyCBOR) != string(fixture.datumCBOR) {
		t.Fatalf("unexpected datum: %#v, %v", datum, err)
	}
	redeemers, boundaries, err := store.Redeemers(ctx, snapshot, fixture.txB)
	if err != nil || len(boundaries) != 0 || len(redeemers) != 1 ||
		redeemers[0].Target.SourceOutput == nil {
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
		forward[0].Transaction != fixture.txB || len(forward[0].ProducedOutputs) != 1 ||
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
	if err != nil || snapshot.Event != 4 {
		t.Fatalf("re-adoption failed: %#v, %v", snapshot, err)
	}
	sourceState, _, err = store.UTxO(ctx, snapshot, fixture.source)
	if err != nil || sourceState.IsCurrent {
		t.Fatalf("re-adoption did not restore spend: %#v, %v", sourceState, err)
	}

	assertPrimaryKeyPlan(t, store, snapshot, fixture.source)
	fixture.assertDatumDuplicateSemantics(t, store, snapshot)
}

func assertPrimaryKeyPlan(t *testing.T, store *Store, snapshot model.Snapshot, ref model.UTxORef) {
	t.Helper()
	queryCtx, finish := store.instrument(context.Background(), "explain_source_spend")
	defer finish()
	rows, err := store.conn.Query(
		queryCtx,
		"EXPLAIN indexes = 1 "+spendByRefSQL,
		activeArguments(snapshot, hashArgument(ref.TxHash), ref.Index)...,
	)
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
	if !strings.Contains(plan.String(), "PrimaryKey") {
		t.Fatalf("source-spend plan did not use a primary key:\n%s", plan.String())
	}
	t.Logf("source-spend EXPLAIN indexes=1:\n%s", plan.String())
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
		addressA:     []byte{0x01, 0xff, 0x80, 0x41},
		addressB:     []byte{0x02, 0xfe, 0x81, 0x42},
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
	exec(`INSERT INTO dataset_manifest
        (revision,dataset_id,network_magic,network_name,byron_genesis_hash,shelley_genesis_hash,
         start_kind,start_slot,start_hash,genesis_seeded,complete_history,trust_mode,
         committed_event_seq,committed_tip_origin,committed_tip_slot,committed_tip_hash,
         committed_tip_block_number,storage_state,active_data_bytes,database_filesystem_bytes,
         merge_reserve_bytes,logs_runtime_bytes,build_cache_bytes,total_project_bytes,
         high_water_active_bytes,high_water_total_bytes,warning_bytes,active_data_limit_bytes,
         project_limit_bytes,writer_id,writer_build,source_build,created_at,updated_at)
        VALUES
        (1,toUUID(?),764824073,'mainnet',?,?, 'intersection',1,?,false,false,?,
         2,false,2,?,2,'ok',0,0,0,0,0,0,0,0,64424509440,75161927680,
         107374182400,toUUID(?),'test','test',now64(6),now64(6))`,
		dataset, string(zero), string(zero), hashArgument(fixture.block1), model.TrustPeerObserved, hashArgument(fixture.block2), writer)
	for _, block := range []struct {
		publication uint64
		hash        model.Hash32
		parent      *model.Hash32
		slot        uint64
		number      uint64
	}{
		{fixture.pub1, fixture.block1, nil, 1, 1},
		{fixture.pub2, fixture.block2, &fixture.block1, 2, 2},
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
            VALUES (?,?,?, ?,?,'conway',0,1,1,2,1,1,1,1,false,'peer','127.0.0.1',
                    'test',15,764824073,true,true,?,toUUID(?),now64(6),now64(6))`,
			block.publication, hashArgument(block.hash), parent, block.slot, block.number,
			hashArgument(block.hash), writer)
		exec(`INSERT INTO chain_events
            (event_seq,publication_id,event_kind,active,rollback_id,block_hash,slot,
             block_number,writer_id,recorded_at)
            VALUES (?,?, 'adoption',true,NULL,?,?,?,toUUID(?),now64(6))`,
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
                1,0,0,1,1,1,true,1)`, hashArgument(fixture.txB), string(fixture.policy[:]), string([]byte{0x00, 0xff}))
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
         address,lovelace,asset_policy_ids,asset_names,asset_quantities,datum_kind,datum_hash,
         reference_script_hash,reference_script_language)
        VALUES (101,1,?,0,0,0,'regular',?,5000000,[?],[?],[9],'none',NULL,NULL,NULL)`,
		hashArgument(fixture.txA), string(fixture.addressA), string(fixture.policy[:]), string([]byte{0x00, 0xff}))
	exec(`INSERT INTO outputs
        (publication_id,block_number,tx_hash,tx_order,output_index,body_ordinal,output_kind,
         address,lovelace,asset_policy_ids,asset_names,asset_quantities,datum_kind,datum_hash,
         reference_script_hash,reference_script_language)
        VALUES (101,1,?,0,1,1,'regular',?,1000000,[],[],[],'none',NULL,NULL,NULL)`,
		hashArgument(fixture.txA), string(fixture.addressA))
	exec(`INSERT INTO outputs
        (publication_id,block_number,tx_hash,tx_order,output_index,body_ordinal,output_kind,
         address,lovelace,asset_policy_ids,asset_names,asset_quantities,datum_kind,datum_hash,
         reference_script_hash,reference_script_language)
        VALUES (102,2,?,0,0,0,'regular',?,5600000,[?],[?],[16],'inline',?,?, 'plutus_v2')`,
		hashArgument(fixture.txB), string(fixture.addressB), string(fixture.policy[:]), string([]byte{0x00, 0xff}),
		hashArgument(fixture.datumHash), string(fixture.policy[:]))
	exec(`INSERT INTO outputs
        (publication_id,block_number,tx_hash,tx_order,output_index,body_ordinal,output_kind,
         address,lovelace,asset_policy_ids,asset_names,asset_quantities,datum_kind,datum_hash,
         reference_script_hash,reference_script_language)
        VALUES (101,1,?,1,0,0,'collateral_return',?,700000,[],[],[],
                'none',NULL,NULL,NULL)`,
		hashArgument(fixture.txC), string(fixture.addressA))
	exec(`INSERT INTO datum_bodies
        (datum_hash,datum_cbor,byte_length,content_hash,first_publication_id,first_seen_at)
        VALUES (?,?,?, ?,102,now64(6))`,
		hashArgument(fixture.datumHash), string(fixture.datumCBOR), len(fixture.datumCBOR), hashArgument(fixture.datumHash))
	exec(`INSERT INTO datum_observations
        (publication_id,block_number,datum_hash,tx_hash,tx_order,source_kind,source_ordinal,
         output_index)
        VALUES (102,2,?,?,0,'inline_output',0,0)`,
		hashArgument(fixture.datumHash), hashArgument(fixture.txB))
	exec(`INSERT INTO withdrawals
        (publication_id,block_number,tx_hash,tx_order,body_ordinal,reward_account,lovelace,
         is_applied,credential_kind,credential_hash)
        VALUES (102,2,?,0,0,?,123,true,'key',?)`,
		hashArgument(fixture.txB), string([]byte{0xe1, 0xff}), string(fixture.policy[:]))
	exec(`INSERT INTO withdrawals
        (publication_id,block_number,tx_hash,tx_order,body_ordinal,reward_account,lovelace,
         is_applied,credential_kind,credential_hash)
        VALUES (101,1,?,1,0,?,999,false,'key',?)`,
		hashArgument(fixture.txC), string([]byte{0xe1, 0x01}), string(fixture.policy[:]))
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
         block_number,writer_id,recorded_at)
        VALUES (3,102,'invalidation',false,toUUID(?),?,2,2,
                toUUID('22222222-2222-4222-8222-222222222222'),now64(6))`,
		fixture.rollbackID, hashArgument(fixture.block2)); err != nil {
		t.Fatal(err)
	}
}

func (fixture fixture) commitRollback(t *testing.T, store *Store) {
	t.Helper()
	if err := store.conn.Exec(context.Background(), `INSERT INTO rollbacks
        (rollback_id,event_seq,rollback_to_origin,rollback_to_slot,rollback_to_hash,
         old_tip_slot,old_tip_hash,old_tip_block_number,depth,reason,observed_peers,
         agreement_group,writer_id,recorded_at)
        VALUES (toUUID(?),3,false,1,?,2,?,2,1,'test',['peer-a','peer-b'],NULL,
                toUUID('22222222-2222-4222-8222-222222222222'),now64(6))`,
		fixture.rollbackID, hashArgument(fixture.block1), hashArgument(fixture.block2)); err != nil {
		t.Fatal(err)
	}
}

func (fixture fixture) readopt(t *testing.T, store *Store) {
	t.Helper()
	if err := store.conn.Exec(context.Background(), `INSERT INTO chain_events
        (event_seq,publication_id,event_kind,active,rollback_id,block_hash,slot,
         block_number,writer_id,recorded_at)
        VALUES (4,102,'adoption',true,NULL,?,2,2,
                toUUID('22222222-2222-4222-8222-222222222222'),now64(6))`,
		hashArgument(fixture.block2)); err != nil {
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
