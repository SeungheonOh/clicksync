package store

import (
	"testing"

	"clicksync/internal/model"
	"clicksync/internal/publication"
	"clicksync/internal/syncer"
)

func TestTrustEvidenceProofMethodResultFlagMatrix(t *testing.T) {
	checked := publication.Point{
		Slot:        10,
		Hash:        manifestHash(0x51),
		BlockNumber: 10,
	}
	base := func(kind, method, result string) model.PeerObservation {
		slot := checked.Slot
		hash := checked.Hash
		number := checked.BlockNumber
		isByronEBB := false
		return model.PeerObservation{
			Kind:                   kind,
			ProofMethod:            method,
			PeerHost:               "relay-a",
			PeerAddress:            "192.0.2.1:3001",
			Operator:               "operator-a",
			N2NVersion:             15,
			NetworkMagic:           mainnetMagic,
			TipSlot:                checked.Slot,
			TipHash:                checked.Hash,
			TipBlockNumber:         checked.BlockNumber,
			CheckpointSlot:         &slot,
			CheckpointHash:         &hash,
			CheckpointBlockNumber:  &number,
			CheckpointIsByronEBB:   &isByronEBB,
			CheckedPointSlot:       &slot,
			CheckedPointHash:       &hash,
			CheckedBlockNumber:     &number,
			CheckedPointIsByronEBB: false,
			Result:                 result,
		}
	}
	chain := base("checkpoint", syncer.ObservationProofChainSyncSingleton, "agreed")
	chain.PointVerified = true
	paired := base("rollback", syncer.ObservationProofPairedChainSyncSingleton, "agreed")
	paired.PointVerified = true
	follow := base("rollback", syncer.ObservationProofFollowBlockFetch, "agreed")
	follow.SelectedBodySource = true
	follow.BodyHashVerified = true
	follow.PointVerified = true
	boundary := base("checkpoint", syncer.ObservationProofBoundarySingletonFetch, "agreed")
	boundary.SelectedBodySource = true
	boundary.BodyHashVerified = true
	boundary.PointVerified = true
	diagnostic := base("source_change", syncer.ObservationProofNone, "unavailable")

	for name, row := range map[string]model.PeerObservation{
		"chain_singleton":    chain,
		"paired_singleton":   paired,
		"follow_block_fetch": follow,
		"boundary_fetch":     boundary,
		"diagnostic_none":    diagnostic,
	} {
		t.Run(name, func(t *testing.T) {
			eligible, err := validateTrustEvidenceProvenance(row, checked)
			if err != nil {
				t.Fatal(err)
			}
			if eligible != (row.Kind != "source_change") {
				t.Fatalf("eligible=%t row=%+v", eligible, row)
			}
		})
	}

	for name, row := range map[string]model.PeerObservation{
		"chain":  chain,
		"paired": paired,
		"follow": follow,
	} {
		for flag := range 4 {
			t.Run(name+"_flip_"+string(rune('0'+flag)), func(t *testing.T) {
				mutated := row
				flipObservationProofFlag(&mutated, flag)
				if _, err := validateTrustEvidenceProvenance(mutated, checked); err == nil {
					t.Fatalf("one-bit proof mutation was accepted: %+v", mutated)
				}
			})
		}
	}
	for _, flag := range []int{1, 2, 3} {
		t.Run("boundary_flip_"+string(rune('0'+flag)), func(t *testing.T) {
			mutated := boundary
			flipObservationProofFlag(&mutated, flag)
			if _, err := validateTrustEvidenceProvenance(mutated, checked); err == nil {
				t.Fatalf("one-bit boundary mutation was accepted: %+v", mutated)
			}
		})
	}
	for _, method := range []string{
		syncer.ObservationProofChainSyncSingleton,
		syncer.ObservationProofPairedChainSyncSingleton,
	} {
		failed := base("rollback", method, "unavailable")
		if method == syncer.ObservationProofChainSyncSingleton {
			failed.Kind = "checkpoint"
		}
		if _, err := validateTrustEvidenceProvenance(failed, checked); err != nil {
			t.Fatal(err)
		}
		for flag := range 4 {
			mutated := failed
			flipObservationProofFlag(&mutated, flag)
			if _, err := validateTrustEvidenceProvenance(mutated, checked); err == nil {
				t.Fatalf("failed singleton carried flag %d: %+v", flag, mutated)
			}
		}
	}
	for _, method := range []string{
		syncer.ObservationProofBoundarySingletonFetch,
		syncer.ObservationProofFollowBlockFetch,
	} {
		failed := base("checkpoint", method, "unavailable")
		if method == syncer.ObservationProofFollowBlockFetch {
			failed.Kind = "rollback"
		}
		if _, err := validateTrustEvidenceProvenance(failed, checked); err == nil {
			t.Fatalf("failed completed-fetch method %q was accepted", method)
		}
	}
	disagreement := chain
	disagreement.Kind = "disagreement"
	if _, err := validateTrustEvidenceProvenance(disagreement, checked); err == nil {
		t.Fatal("disagreement kind claimed agreed result")
	}
	diagnostic.Result = "agreed"
	if _, err := validateTrustEvidenceProvenance(diagnostic, checked); err == nil {
		t.Fatal("diagnostic none claimed authoritative agreement")
	}
}

func flipObservationProofFlag(row *model.PeerObservation, flag int) {
	switch flag {
	case 0:
		row.SelectedBodySource = !row.SelectedBodySource
	case 1:
		row.BodyHashVerified = !row.BodyHashVerified
	case 2:
		row.PointVerified = !row.PointVerified
	case 3:
		row.ParentVerified = !row.ParentVerified
	}
}
