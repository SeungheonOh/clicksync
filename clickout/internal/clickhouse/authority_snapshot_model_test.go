package clickhouse

import (
	"context"
	"errors"
	"testing"

	"github.com/clicksync-project/clickout/internal/model"
)

func TestAuthoritySnapshotModelRoundTrip(t *testing.T) {
	t.Parallel()
	record, atTip, _, _, _ := authoritySnapshotAdoptionTestState(t)
	record.TrustMode = model.TrustPeerObserved
	atTip.Identity = authoritySnapshotIdentityFromRecord(record)
	atTip.Diagnostics = authoritySnapshotDiagnostics{
		Physical:    record.Effective,
		TrustStatus: "agreed",
		TrustBasis:  "official_genesis",
		TrustReason: "captured reason",
	}
	atBlock := atTip
	atBlock.Mode = authoritySnapshotAtBlock
	atBlock.BlockHash = atBlock.QueryHead.Point.Hash
	atBlock.SelectedPublicationID = atBlock.Cutoff.PublicationID
	atBlock.SelectedPoint = atBlock.QueryHead.Point

	for name, lease := range map[string]authoritySnapshotLease{
		"tip":   atTip,
		"block": atBlock,
	} {
		name, lease := name, lease
		t.Run(name, func(t *testing.T) {
			exposed, err := modelAuthoritySnapshot(lease)
			if err != nil {
				t.Fatal(err)
			}
			if !exposed.Valid() ||
				exposed.Diagnostics.TrustReason != "captured reason" ||
				exposed.Identity.DatasetID != model.DatasetID(record.DatasetID) {
				t.Fatalf("invalid/lossy public snapshot: %+v", exposed)
			}
			decoded, err := internalAuthoritySnapshot(exposed)
			if err != nil {
				t.Fatal(err)
			}
			if decoded != lease {
				t.Fatalf("snapshot round trip differs:\n got %+v\nwant %+v", decoded, lease)
			}
		})
	}
}

func TestInternalAuthoritySnapshotRejectsInvalidPublicShape(t *testing.T) {
	t.Parallel()
	record, lease, _, _, _ := authoritySnapshotAdoptionTestState(t)
	record.TrustMode = model.TrustPeerObserved
	lease.Identity = authoritySnapshotIdentityFromRecord(record)
	lease.Diagnostics = authoritySnapshotDiagnostics{
		Physical:    record.Effective,
		TrustStatus: "agreed",
		TrustBasis:  "official_genesis",
	}
	exposed, err := modelAuthoritySnapshot(lease)
	if err != nil {
		t.Fatal(err)
	}
	exposed.Identity.DatasetID = model.DatasetID{}
	if _, err := internalAuthoritySnapshot(exposed); !errors.Is(
		err,
		ErrInvalidDataset,
	) {
		t.Fatalf("invalid public snapshot error = %v", err)
	}
}

func TestSnapshotUnavailableErrorCarriesStableDiagnostics(t *testing.T) {
	t.Parallel()
	record, _, _, _, _ := authoritySnapshotAdoptionTestState(t)
	record.Physical = record.Effective
	record.TrustStatus = "disputed"
	record.TrustBasis = "sampled_peer"
	record.TrustReason = "peer disagreement"
	err := newSnapshotUnavailableError("not servable", &record)
	if !errors.Is(err, ErrSnapshotUnavailable) ||
		err.TrustStatus != record.TrustStatus ||
		err.TrustBasis != record.TrustBasis ||
		err.TrustReason != record.TrustReason ||
		err.Physical != modelAuthorityHead(record.Physical) ||
		err.Effective != modelAuthorityHead(record.Effective) {
		t.Fatalf("unavailable diagnostics = %+v", err)
	}
}

func TestRefreshModelAuthoritySnapshotCanonicalizesDiagnostics(
	t *testing.T,
) {
	t.Parallel()
	record, lease, readers, _, _ := authoritySnapshotAdoptionTestState(t)
	record.TrustMode = model.TrustPeerObserved
	lease.Identity = authoritySnapshotIdentityFromRecord(record)
	record.Physical = record.Effective
	record.TrustStatus = "agreed"
	record.TrustBasis = "official_genesis"
	record.TrustReason = "canonical"
	lease.Diagnostics = authoritySnapshotDiagnostics{
		Physical:    record.Physical,
		TrustStatus: record.TrustStatus,
		TrustBasis:  record.TrustBasis,
		TrustReason: record.TrustReason,
	}
	stable := authorityHeadAttempt{
		Observation: authorityHeadObservation{
			Present:  true,
			Revision: 8,
		},
		Latest: record,
		Found:  true,
	}
	readers.readHead = func(context.Context) (authorityHeadAttempt, error) {
		return stable, nil
	}
	exposed, err := modelAuthoritySnapshot(lease)
	if err != nil {
		t.Fatal(err)
	}
	canonical := model.SnapshotDiagnostics{
		Physical:    modelAuthorityHead(record.Physical),
		TrustStatus: record.TrustStatus,
		TrustBasis:  record.TrustBasis,
		TrustReason: record.TrustReason,
	}
	tests := map[string]func(*model.SnapshotDiagnostics){
		"later physical": func(value *model.SnapshotDiagnostics) {
			value.Physical = model.Head{
				EventSeq: record.Physical.EventSeq + 100,
				Point: model.Point{
					Slot:        999,
					Hash:        model.Hash32{0x71},
					BlockNumber: 999,
				},
			}
		},
		"trust status": func(value *model.SnapshotDiagnostics) {
			value.TrustStatus = "checking"
		},
		"trust basis": func(value *model.SnapshotDiagnostics) {
			value.TrustBasis = "sampled_peer"
		},
		"trust reason": func(value *model.SnapshotDiagnostics) {
			value.TrustReason = "forged"
		},
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			forged := exposed
			mutate(&forged.Diagnostics)
			if !forged.Valid() {
				t.Fatal("structurally valid forged diagnostics rejected before refresh")
			}
			refreshed, err := refreshModelAuthoritySnapshotWithReaders(
				context.Background(),
				forged,
				readers,
			)
			if err != nil {
				t.Fatal(err)
			}
			if refreshed.Diagnostics != canonical {
				t.Fatalf(
					"diagnostics were not canonicalized: %+v",
					refreshed.Diagnostics,
				)
			}
			refreshed.Diagnostics = model.SnapshotDiagnostics{}
			forged.Diagnostics = model.SnapshotDiagnostics{}
			if refreshed != forged {
				t.Fatalf(
					"refresh changed immutable/query pin:\n got %+v\nwant %+v",
					refreshed,
					forged,
				)
			}
		})
	}
}

func TestRefreshModelAuthoritySnapshotAllowsProgressAndRefreshesDiagnostics(
	t *testing.T,
) {
	t.Parallel()
	record, lease, readers, _, _ := authoritySnapshotAdoptionTestState(t)
	record.TrustMode = model.TrustPeerObserved
	lease.Identity = authoritySnapshotIdentityFromRecord(record)
	record.Physical = record.Effective
	record.TrustStatus = "agreed"
	record.TrustBasis = "official_genesis"
	lease.Diagnostics = authoritySnapshotDiagnostics{
		Physical:    record.Physical,
		TrustStatus: record.TrustStatus,
		TrustBasis:  record.TrustBasis,
	}
	exposed, err := modelAuthoritySnapshot(lease)
	if err != nil {
		t.Fatal(err)
	}

	fresh := record
	fresh.Effective = authorityHead{
		EventSeq: record.Effective.EventSeq + 1,
		Point: authorityPoint{
			Slot:        88,
			Hash:        authorityFill32(0x72),
			BlockNumber: 88,
		},
	}
	fresh.Physical = fresh.Effective
	fresh.TrustStatus = "checking"
	fresh.TrustBasis = "primary_only"
	fresh.TrustReason = "new authority observation"
	stable := authorityHeadAttempt{
		Observation: authorityHeadObservation{
			Present:  true,
			Revision: 9,
		},
		Latest: fresh,
		Found:  true,
	}
	readers.readHead = func(context.Context) (authorityHeadAttempt, error) {
		return stable, nil
	}
	refreshed, err := refreshModelAuthoritySnapshotWithReaders(
		context.Background(),
		exposed,
		readers,
	)
	if err != nil {
		t.Fatal(err)
	}
	if refreshed.AuthorityEffective != exposed.AuthorityEffective ||
		refreshed.QueryHead != exposed.QueryHead ||
		refreshed.Diagnostics.Physical != modelAuthorityHead(fresh.Physical) ||
		refreshed.Diagnostics.TrustStatus != fresh.TrustStatus ||
		refreshed.Diagnostics.TrustBasis != fresh.TrustBasis ||
		refreshed.Diagnostics.TrustReason != fresh.TrustReason {
		t.Fatalf("progress refresh = %+v", refreshed)
	}
}

func TestSnapshotValidRejectsInvalidDiagnosticsShape(t *testing.T) {
	t.Parallel()
	record, lease, _, _, _ := authoritySnapshotAdoptionTestState(t)
	record.TrustMode = model.TrustPeerObserved
	lease.Identity = authoritySnapshotIdentityFromRecord(record)
	lease.Diagnostics = authoritySnapshotDiagnostics{
		Physical:    record.Effective,
		TrustStatus: "agreed",
		TrustBasis:  "official_genesis",
	}
	exposed, err := modelAuthoritySnapshot(lease)
	if err != nil {
		t.Fatal(err)
	}
	tests := map[string]func(*model.Snapshot){
		"zero physical": func(value *model.Snapshot) {
			value.Diagnostics.Physical = model.Head{}
		},
		"same-event different point": func(value *model.Snapshot) {
			value.Diagnostics.Physical = value.AuthorityEffective
			value.Diagnostics.Physical.Point.Hash[0]++
		},
		"zero status": func(value *model.Snapshot) {
			value.Diagnostics.TrustStatus = ""
		},
		"zero basis": func(value *model.Snapshot) {
			value.Diagnostics.TrustBasis = ""
		},
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			value := exposed
			mutate(&value)
			if value.Valid() {
				t.Fatal("invalid diagnostics accepted")
			}
			if _, err := internalAuthoritySnapshot(value); !errors.Is(
				err,
				ErrInvalidDataset,
			) {
				t.Fatalf("invalid diagnostics conversion error = %v", err)
			}
		})
	}
}
