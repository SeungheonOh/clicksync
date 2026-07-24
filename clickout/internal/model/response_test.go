package model

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func validSnapshotForTest() Snapshot {
	point := Point{
		Slot:        41,
		Hash:        Hash32{1},
		BlockNumber: 39,
	}
	return Snapshot{
		Identity: SnapshotIdentity{
			DatasetID:              DatasetID{1},
			SchemaContractHash:     Hash32{2},
			NetworkMagic:           764824073,
			NetworkName:            "mainnet",
			ByronGenesisID:         Hash32{3},
			ByronGenesisJSONHash:   Hash32{4},
			ShelleyGenesisID:       Hash32{5},
			ShelleyGenesisJSONHash: Hash32{6},
			Start: Point{
				Slot:        1,
				Hash:        Hash32{7},
				BlockNumber: 1,
			},
			TrustMode:       TrustPeerObserved,
			CreatedAt:       time.Date(2026, 7, 23, 1, 2, 3, 0, time.UTC),
			CompleteHistory: false,
		},
		VisibilityGeneration: 9,
		AuthorityEffective:   Head{EventSeq: 41, Point: point},
		QueryHead:            Head{EventSeq: 41, Point: point},
		Cutoff: Cutoff{
			AdoptionEventSeq: 41,
			PublicationID:    39,
		},
		Selector: SnapshotSelector{Mode: SnapshotAtTip},
		Diagnostics: SnapshotDiagnostics{
			Physical:    Head{EventSeq: 43, Point: point},
			TrustStatus: "agreed",
			TrustBasis:  "partial_boundary",
		},
	}
}

func TestEveryResponseCarriesRequiredDatasetSemantics(t *testing.T) {
	t.Parallel()
	snapshot := validSnapshotForTest()
	if !snapshot.Valid() {
		t.Fatal("fixture snapshot is invalid")
	}
	response := NewResponse(snapshot, struct {
		OK bool `json:"ok"`
	}{OK: true})
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	text := string(encoded)
	for _, required := range []string{
		`"dataset_id":"01000000000000000000000000000000"`,
		`"visibility_generation":9`,
		`"authority_effective":{"event_seq":41`,
		`"query_head":{"event_seq":41`,
		`"cutoff":{"adoption_event_seq":41,"publication_id":39}`,
		`"selector":{"mode":"tip"`,
		`"physical":{"event_seq":43`,
		`"complete_history":false`,
		`"trust_mode":"peer_observed_structurally_verified"`,
		`"truncated":false`,
		`"continuation_frontier":[]`,
		`"lossless_resume":false`,
		`"unresolved_partial_history":[]`,
		`"peer_observed_disclaimer":`,
		`"excluded_non_utxo_deltas":`,
	} {
		if !strings.Contains(text, required) {
			t.Errorf("response missing %s: %s", required, text)
		}
	}
}

func TestTruncationReasonsAreStableAndStateValidated(t *testing.T) {
	t.Parallel()
	reasons := []struct {
		reason TruncationReason
		text   string
	}{
		{TruncationAddressSeedLimit, "address_seed_limit"},
		{TruncationAddressPageLimit, "address_page_limit"},
		{TruncationMaxNodes, "max_nodes"},
		{TruncationMaxEdges, "max_edges"},
		{TruncationLayerTimeout, "layer_timeout"},
		{TruncationMaxDepth, "max_depth"},
	}
	for _, test := range reasons {
		if string(test.reason) != test.text || !test.reason.Valid() {
			t.Errorf("unstable truncation reason %q, want %q", test.reason, test.text)
		}
		value := Truncation{Truncated: true, Reason: test.reason}
		switch test.reason {
		case TruncationAddressSeedLimit:
			value.ContinuationCursor = "cursor"
		case TruncationAddressPageLimit:
			value.LosslessResume = true
		case TruncationMaxNodes,
			TruncationMaxEdges,
			TruncationLayerTimeout,
			TruncationMaxDepth:
			value.ContinuationFrontier = []UTxORef{{TxHash: Hash32{1}}}
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("marshal %q: %v", test.reason, err)
		}
		var decoded Truncation
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatalf("unmarshal %q: %v", test.reason, err)
		}
		if decoded.Reason != test.reason || !decoded.Valid() {
			t.Errorf("round trip %q = %#v", test.reason, decoded)
		}
	}

	if !(Truncation{}).Valid() {
		t.Fatal("untruncated empty reason rejected")
	}
	for _, invalid := range []Truncation{
		{Truncated: true},
		{Reason: TruncationMaxNodes},
		{Truncated: true, Reason: "unknown"},
		{ContinuationCursor: "cursor"},
		{
			Truncated:          true,
			Reason:             TruncationAddressSeedLimit,
			ContinuationCursor: "cursor",
			ContinuationFrontier: []UTxORef{{
				TxHash: Hash32{1},
			}},
		},
		{
			Truncated:          true,
			Reason:             TruncationAddressPageLimit,
			ContinuationCursor: "cursor",
			LosslessResume:     true,
		},
		{Truncated: true, Reason: TruncationMaxEdges},
		{
			Truncated:          true,
			Reason:             TruncationMaxDepth,
			ContinuationCursor: "cursor",
		},
	} {
		if invalid.Valid() {
			t.Errorf("invalid truncation accepted: %#v", invalid)
		}
		if _, err := json.Marshal(invalid); err == nil {
			t.Errorf("invalid truncation marshaled: %#v", invalid)
		}
	}
	for _, encoded := range []string{
		`{"truncated":true}`,
		`{"truncated":false,"reason":"max_nodes"}`,
		`{"truncated":true,"reason":"unknown"}`,
	} {
		var decoded Truncation
		if err := json.Unmarshal([]byte(encoded), &decoded); err == nil {
			t.Errorf("invalid JSON accepted: %s", encoded)
		}
	}
}

func TestSnapshotValidSelectorAndHistoryRules(t *testing.T) {
	t.Parallel()
	valid := validSnapshotForTest()
	block := valid
	hash := block.QueryHead.Point.Hash
	point := block.QueryHead.Point
	block.Selector = SnapshotSelector{
		Mode:                  SnapshotAtBlock,
		RequestedBlockHash:    &hash,
		SelectedPublicationID: block.Cutoff.PublicationID,
		SelectedPoint:         &point,
	}
	if !block.Valid() {
		t.Fatal("valid AtBlock snapshot rejected")
	}

	partialBoundary := valid
	partialBoundary.AuthorityEffective = Head{
		Point: partialBoundary.Identity.Start,
	}
	partialBoundary.QueryHead = partialBoundary.AuthorityEffective
	partialBoundary.Cutoff = Cutoff{}
	if !partialBoundary.Valid() {
		t.Fatal("partial non-Origin event-zero snapshot rejected")
	}

	completeOrigin := valid
	completeOrigin.Identity.Start = Point{Origin: true}
	completeOrigin.Identity.CompleteHistory = true
	completeOrigin.AuthorityEffective = Head{
		Point: completeOrigin.Identity.Start,
	}
	completeOrigin.QueryHead = completeOrigin.AuthorityEffective
	completeOrigin.Cutoff = Cutoff{}
	completeOrigin.Diagnostics.Physical = Head{
		EventSeq: 1,
		Point: Point{
			Slot:        1,
			Hash:        Hash32{8},
			BlockNumber: 1,
		},
	}
	if completeOrigin.Valid() {
		t.Fatal("complete-history event-zero snapshot accepted")
	}
}

func TestSnapshotValidRejectsIdentityAndShapeMutations(t *testing.T) {
	t.Parallel()
	tests := map[string]func(*Snapshot){
		"zero dataset": func(value *Snapshot) {
			value.Identity.DatasetID = DatasetID{}
		},
		"zero schema": func(value *Snapshot) {
			value.Identity.SchemaContractHash = Hash32{}
		},
		"zero network magic": func(value *Snapshot) {
			value.Identity.NetworkMagic = 0
		},
		"empty network name": func(value *Snapshot) {
			value.Identity.NetworkName = " "
		},
		"zero genesis": func(value *Snapshot) {
			value.Identity.ShelleyGenesisJSONHash = Hash32{}
		},
		"invalid start": func(value *Snapshot) {
			value.Identity.Start = Point{}
		},
		"wrong trust mode": func(value *Snapshot) {
			value.Identity.TrustMode = "other"
		},
		"zero created at": func(value *Snapshot) {
			value.Identity.CreatedAt = time.Time{}
		},
		"history/start mismatch": func(value *Snapshot) {
			value.Identity.CompleteHistory = true
		},
		"partial cutoff": func(value *Snapshot) {
			value.Cutoff.PublicationID = 0
		},
		"cutoff beyond head": func(value *Snapshot) {
			value.Cutoff.AdoptionEventSeq++
		},
		"tip head mismatch": func(value *Snapshot) {
			value.QueryHead.EventSeq--
		},
		"tip requested block": func(value *Snapshot) {
			hash := Hash32{9}
			value.Selector.RequestedBlockHash = &hash
		},
		"invalid diagnostics": func(value *Snapshot) {
			value.Diagnostics.TrustStatus = ""
		},
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			value := validSnapshotForTest()
			mutate(&value)
			if value.Valid() {
				t.Fatal("mutated snapshot accepted")
			}
		})
	}
}

func TestSnapshotSamePinComparesEveryImmutableAndQueryField(t *testing.T) {
	t.Parallel()
	base := validSnapshotForTest()
	diagnostics := base
	diagnostics.Diagnostics.Physical.EventSeq += 10
	diagnostics.Diagnostics.Physical.Point = Point{
		Slot:        99,
		Hash:        Hash32{99},
		BlockNumber: 99,
	}
	diagnostics.Diagnostics.TrustStatus = "checking"
	diagnostics.Diagnostics.TrustBasis = "primary_only"
	diagnostics.Diagnostics.TrustReason = "changed"
	if !base.SamePin(diagnostics) {
		t.Fatal("refreshable diagnostics changed the pin")
	}
	sameInstant := base
	sameInstant.Identity.CreatedAt = base.Identity.CreatedAt.In(
		time.FixedZone("same", -5*60*60),
	)
	if !base.SamePin(sameInstant) {
		t.Fatal("CreatedAt.Equal representation changed the pin")
	}

	tests := map[string]func(*Snapshot){
		"dataset ID": func(value *Snapshot) {
			value.Identity.DatasetID[0]++
		},
		"schema hash": func(value *Snapshot) {
			value.Identity.SchemaContractHash[0]++
		},
		"network magic": func(value *Snapshot) {
			value.Identity.NetworkMagic++
		},
		"network name": func(value *Snapshot) {
			value.Identity.NetworkName += "-other"
		},
		"Byron ID": func(value *Snapshot) {
			value.Identity.ByronGenesisID[0]++
		},
		"Byron JSON": func(value *Snapshot) {
			value.Identity.ByronGenesisJSONHash[0]++
		},
		"Shelley ID": func(value *Snapshot) {
			value.Identity.ShelleyGenesisID[0]++
		},
		"Shelley JSON": func(value *Snapshot) {
			value.Identity.ShelleyGenesisJSONHash[0]++
		},
		"start": func(value *Snapshot) {
			value.Identity.Start.Hash[0]++
		},
		"trust mode": func(value *Snapshot) {
			value.Identity.TrustMode += "-other"
		},
		"created at": func(value *Snapshot) {
			value.Identity.CreatedAt =
				value.Identity.CreatedAt.Add(time.Nanosecond)
		},
		"complete history": func(value *Snapshot) {
			value.Identity.CompleteHistory =
				!value.Identity.CompleteHistory
		},
		"generation": func(value *Snapshot) {
			value.VisibilityGeneration++
		},
		"authority effective": func(value *Snapshot) {
			value.AuthorityEffective.EventSeq++
		},
		"query head": func(value *Snapshot) {
			value.QueryHead.EventSeq++
		},
		"cutoff": func(value *Snapshot) {
			value.Cutoff.PublicationID++
		},
		"selector": func(value *Snapshot) {
			value.Selector.Mode = SnapshotAtBlock
		},
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			changed := base
			mutate(&changed)
			if base.SamePin(changed) {
				t.Fatal("changed field was ignored")
			}
		})
	}
}

func TestSnapshotSamePinComparesAtBlockOptionalSelectorValues(t *testing.T) {
	t.Parallel()
	base := validSnapshotForTest()
	requested := Hash32{44}
	selected := base.QueryHead.Point
	base.Selector = SnapshotSelector{
		Mode:                  SnapshotAtBlock,
		RequestedBlockHash:    &requested,
		SelectedPublicationID: base.Cutoff.PublicationID,
		SelectedPoint:         &selected,
	}
	tests := map[string]func(*Snapshot){
		"requested hash": func(value *Snapshot) {
			changed := *value.Selector.RequestedBlockHash
			changed[0]++
			value.Selector.RequestedBlockHash = &changed
		},
		"requested presence": func(value *Snapshot) {
			value.Selector.RequestedBlockHash = nil
		},
		"publication": func(value *Snapshot) {
			value.Selector.SelectedPublicationID++
		},
		"point": func(value *Snapshot) {
			changed := *value.Selector.SelectedPoint
			changed.Hash[0]++
			value.Selector.SelectedPoint = &changed
		},
		"point presence": func(value *Snapshot) {
			value.Selector.SelectedPoint = nil
		},
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			changed := base
			mutate(&changed)
			if base.SamePin(changed) {
				t.Fatal("changed selector field was ignored")
			}
		})
	}
}
