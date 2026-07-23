package ingest

import (
	"context"
	"testing"
	"time"

	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"

	"clicksync/internal/model"
	"clicksync/internal/n2n"
	"clicksync/internal/syncer"
)

type collectingObservationSink struct {
	rows []model.PeerObservation
}

func (sink *collectingObservationSink) InsertPeerObservations(
	_ context.Context,
	rows []model.PeerObservation,
) error {
	sink.rows = append(sink.rows, rows...)
	return nil
}

func TestObserverPersistsUnavailableProbeWithoutInventingTipProvenance(t *testing.T) {
	sink := &collectingObservationSink{}
	observer, err := NewObserver(sink, n2n.MainnetNetworkMagic)
	if err != nil {
		t.Fatal(err)
	}
	isByronEBB := false
	checkpointHash := adapterHash(0x10)
	if err := observer.Observe(context.Background(), syncer.Observation{
		Kind: "checkpoint",
		Peer: n2n.Peer{
			Host:     "relay-a:3001",
			Operator: "operator-a",
		},
		Checkpoint:            pcommon.NewPoint(10, checkpointHash.Bytes()),
		CheckpointBlockNumber: 10,
		CheckpointIsByronEBB:  &isByronEBB,
		Result:                "unavailable",
		Reason:                "dial timeout",
		ObservedAt:            time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if len(sink.rows) != 1 {
		t.Fatalf("rows = %d", len(sink.rows))
	}
	row := sink.rows[0]
	if row.PeerAddress != "" ||
		row.N2NVersion != 0 ||
		row.TipHash != (model.Hash32{}) ||
		row.PointVerified {
		t.Fatalf("unavailable observation invented provenance: %#v", row)
	}
	if row.CheckpointHash == nil ||
		*row.CheckpointHash != model.Hash32(checkpointHash) ||
		row.CheckpointIsByronEBB == nil ||
		*row.CheckpointIsByronEBB {
		t.Fatalf("checkpoint metadata = %#v", row)
	}
}

func TestObserverRequiresActualProvenanceForAgreement(t *testing.T) {
	observer, err := NewObserver(
		&collectingObservationSink{},
		n2n.MainnetNetworkMagic,
	)
	if err != nil {
		t.Fatal(err)
	}
	isByronEBB := false
	if err := observer.Observe(context.Background(), syncer.Observation{
		Kind: "checkpoint",
		Peer: n2n.Peer{
			Host:     "relay-a:3001",
			Operator: "operator-a",
		},
		Checkpoint:            pcommon.NewPoint(10, adapterHash(0x10).Bytes()),
		CheckpointBlockNumber: 10,
		CheckpointIsByronEBB:  &isByronEBB,
		Result:                "agreed",
		ObservedAt:            time.Now().UTC(),
	}); err == nil {
		t.Fatal("agreed observation accepted without actual address/version/tip")
	}
}
