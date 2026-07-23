package model

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestEveryResponseCarriesRequiredDatasetSemantics(t *testing.T) {
	t.Parallel()
	snapshot := Snapshot{
		Event:                41,
		PublicationWatermark: 39,
		CompleteHistory:      false,
		TrustMode:            TrustPeerObserved,
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
		`"event":41`,
		`"publication_watermark":39`,
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

func TestOriginSnapshotZeroIsValid(t *testing.T) {
	t.Parallel()
	origin := Snapshot{
		Event:           0,
		CompleteHistory: false,
		TrustMode:       TrustPeerObserved,
	}
	if !origin.Valid() {
		t.Fatal("origin/no-event snapshot must be valid")
	}
	origin.CompleteHistory = true
	if origin.Valid() {
		t.Fatal("complete history cannot exist before a committed genesis adoption")
	}
}
