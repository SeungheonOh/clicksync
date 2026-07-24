package clickhouse

import (
	"errors"
	"testing"

	"github.com/clicksync-project/clickout/internal/cursor"
	"github.com/clicksync-project/clickout/internal/model"
	"github.com/clicksync-project/clickout/internal/repository"
)

func traceCursorSnapshot(t *testing.T) model.Snapshot {
	t.Helper()
	record, lease, _, _, _ := authoritySnapshotAdoptionTestState(t)
	record.TrustMode = model.TrustPeerObserved
	lease.Identity = authoritySnapshotIdentityFromRecord(record)
	lease.Diagnostics = authoritySnapshotDiagnostics{
		Physical:    record.Effective,
		TrustStatus: "agreed",
		TrustBasis:  "official_genesis",
	}
	snapshot, err := modelAuthoritySnapshot(lease)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestRewrapTraceAddressContinuationBindsScopeAndFullPin(t *testing.T) {
	t.Parallel()
	address := []byte{0x61, 0x01}
	asset := model.AssetSelector{ADA: true}
	snapshot := traceCursorSnapshot(t)
	inner, err := cursor.Encode(cursor.Value{
		Scope:    addressScope(address, "history"),
		Snapshot: snapshot,
		LastKey:  "physical-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	lastKey, encoded, err := rewrapTraceAddressContinuation(
		inner,
		address,
		repository.Forward,
		asset,
		snapshot,
	)
	if err != nil || lastKey != "physical-key" {
		t.Fatalf("rewrap = %q, %v", lastKey, err)
	}
	value, err := cursor.Decode(
		encoded,
		TraceAddressScope(address, repository.Forward, asset),
	)
	if err != nil || !value.Snapshot.SamePin(snapshot) ||
		value.LastKey != lastKey {
		t.Fatalf("trace cursor = %+v, %v", value, err)
	}
	for _, wrongScope := range []string{
		addressScope(address, "history"),
		TraceAddressScope(address, repository.Reverse, asset),
		TraceAddressScope(
			address,
			repository.Forward,
			model.AssetSelector{PolicyID: model.PolicyID{1}},
		),
	} {
		if _, err := cursor.Decode(encoded, wrongScope); err == nil {
			t.Fatalf("trace cursor accepted under %q", wrongScope)
		}
	}
}

func TestRewrapTraceAddressContinuationRejectsEveryMutatedPinClass(
	t *testing.T,
) {
	t.Parallel()
	address := []byte{0x61, 0x02}
	canonical := traceCursorSnapshot(t)
	tests := map[string]func(*model.Snapshot){
		"identity": func(value *model.Snapshot) {
			value.Identity.DatasetID[0]++
		},
		"generation": func(value *model.Snapshot) {
			value.VisibilityGeneration++
		},
		"head": func(value *model.Snapshot) {
			point := value.QueryHead.Point
			point.Slot++
			point.Hash[0]++
			point.BlockNumber++
			head := model.Head{
				EventSeq: value.QueryHead.EventSeq + 1,
				Point:    point,
			}
			value.AuthorityEffective = head
			value.QueryHead = head
			value.Cutoff.AdoptionEventSeq = head.EventSeq
			value.Diagnostics.Physical = head
		},
		"cutoff": func(value *model.Snapshot) {
			value.Cutoff.PublicationID++
		},
		"selector": func(value *model.Snapshot) {
			requested := model.Hash32{0x77}
			selected := value.QueryHead.Point
			value.Selector = model.SnapshotSelector{
				Mode:                  model.SnapshotAtBlock,
				RequestedBlockHash:    &requested,
				SelectedPublicationID: value.Cutoff.PublicationID,
				SelectedPoint:         &selected,
			}
		},
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			changed := canonical
			mutate(&changed)
			if !changed.Valid() {
				t.Fatal("mutation is not structurally valid")
			}
			inner, err := cursor.Encode(cursor.Value{
				Scope:    addressScope(address, "history"),
				Snapshot: changed,
				LastKey:  "physical-key",
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := rewrapTraceAddressContinuation(
				inner,
				address,
				repository.Forward,
				model.AssetSelector{ADA: true},
				canonical,
			); !errors.Is(err, cursor.ErrInvalid) {
				t.Fatalf("mutated pin error = %v", err)
			}
		})
	}
}
