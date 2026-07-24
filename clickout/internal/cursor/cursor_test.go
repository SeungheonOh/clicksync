package cursor

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/clicksync-project/clickout/internal/model"
)

func cursorSnapshot() model.Snapshot {
	point := model.Point{
		Slot:        9,
		Hash:        model.Hash32{9},
		BlockNumber: 9,
	}
	return model.Snapshot{
		Identity: model.SnapshotIdentity{
			DatasetID:              model.DatasetID{1},
			SchemaContractHash:     model.Hash32{2},
			NetworkMagic:           764824073,
			NetworkName:            "mainnet",
			ByronGenesisID:         model.Hash32{3},
			ByronGenesisJSONHash:   model.Hash32{4},
			ShelleyGenesisID:       model.Hash32{5},
			ShelleyGenesisJSONHash: model.Hash32{6},
			Start: model.Point{
				Slot:        1,
				Hash:        model.Hash32{7},
				BlockNumber: 1,
			},
			TrustMode:       model.TrustPeerObserved,
			CreatedAt:       time.Date(2026, 7, 23, 1, 2, 3, 0, time.UTC),
			CompleteHistory: false,
		},
		VisibilityGeneration: 4,
		AuthorityEffective:   model.Head{EventSeq: 9, Point: point},
		QueryHead:            model.Head{EventSeq: 9, Point: point},
		Cutoff: model.Cutoff{
			AdoptionEventSeq: 9,
			PublicationID:    8,
		},
		Selector: model.SnapshotSelector{Mode: model.SnapshotAtTip},
		Diagnostics: model.SnapshotDiagnostics{
			Physical:    model.Head{EventSeq: 9, Point: point},
			TrustStatus: "agreed",
			TrustBasis:  "partial_boundary",
		},
	}
}

func TestCursorPinsFullSnapshotAndScope(t *testing.T) {
	t.Parallel()
	snapshot := cursorSnapshot()
	encoded, err := Encode(Value{
		Scope:    "address/current/addr_test1",
		Snapshot: snapshot,
		LastKey:  "height:tx:index",
	})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(encoded, "address/current/addr_test1")
	if err != nil {
		t.Fatal(err)
	}
	if decoded.LastKey != "height:tx:index" ||
		!decoded.Snapshot.SamePin(snapshot) {
		t.Fatalf("got %#v", decoded)
	}
	if _, err := Decode(
		encoded,
		"address/history/addr_test1",
	); err == nil {
		t.Fatal("scope mismatch must fail")
	}
}

func TestCursorRejectsTamperingAndInvalidSnapshot(t *testing.T) {
	t.Parallel()
	value := Value{
		Scope:    "address/current/a",
		Snapshot: cursorSnapshot(),
		LastKey:  "k",
	}
	encoded, err := Encode(value)
	if err != nil {
		t.Fatal(err)
	}
	last := encoded[len(encoded)-1]
	replacement := byte('A')
	if last == replacement {
		replacement = 'B'
	}
	tampered := encoded[:len(encoded)-1] + string(replacement)
	if _, err := Decode(tampered, value.Scope); err == nil {
		t.Fatal("tampered cursor must fail")
	}
	value.Snapshot.Identity.DatasetID = model.DatasetID{}
	if _, err := Encode(value); err == nil {
		t.Fatal("invalid snapshot was encoded")
	}
}

func TestCursorRejectsLegacyUnknownDuplicateAndNoncanonicalWire(t *testing.T) {
	t.Parallel()
	value := Value{
		Scope:    "address/current/a",
		Snapshot: cursorSnapshot(),
		LastKey:  "k",
	}
	encoded, err := Encode(value)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(wire, &fields); err != nil {
		t.Fatal(err)
	}
	fields["unknown"] = json.RawMessage("true")
	unknown, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"legacy":       []byte(`{"v":1,"scope":"address/current/a","snapshot_event":9,"last_key":"k","checksum":"x"}`),
		"unknown":      unknown,
		"duplicate":    []byte(strings.Replace(string(wire), `"scope":`, `"scope":"duplicate","scope":`, 1)),
		"noncanonical": append(append([]byte{}, wire...), '\n'),
	}
	for name, raw := range cases {
		name, raw := name, raw
		t.Run(name, func(t *testing.T) {
			encoded := base64.RawURLEncoding.EncodeToString(raw)
			if _, err := Decode(encoded, value.Scope); err == nil {
				t.Fatal("malformed wire accepted")
			}
		})
	}
}

func TestCursorAcceptsPartialHistoryEventZero(t *testing.T) {
	t.Parallel()
	snapshot := cursorSnapshot()
	snapshot.AuthorityEffective = model.Head{Point: snapshot.Identity.Start}
	snapshot.QueryHead = snapshot.AuthorityEffective
	snapshot.Cutoff = model.Cutoff{}
	snapshot.Diagnostics.Physical = snapshot.AuthorityEffective
	encoded, err := Encode(Value{
		Scope:    "address/current/a",
		Snapshot: snapshot,
		LastKey:  "k",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(encoded, "address/current/a"); err != nil {
		t.Fatal(err)
	}
}
