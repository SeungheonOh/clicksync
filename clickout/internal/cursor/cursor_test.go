package cursor

import "testing"

func TestCursorPinsScopeAndSnapshot(t *testing.T) {
	t.Parallel()
	encoded, err := Encode(Value{
		Scope:         "address/current/addr_test1",
		SnapshotEvent: 99,
		LastKey:       "height:tx:index",
	})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(encoded, "address/current/addr_test1", 99)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.LastKey != "height:tx:index" {
		t.Fatalf("got %#v", decoded)
	}
	if _, err := Decode(encoded, "address/history/addr_test1", 99); err == nil {
		t.Fatal("scope mismatch must fail")
	}
	if _, err := Decode(encoded, "address/current/addr_test1", 100); err == nil {
		t.Fatal("snapshot mismatch must fail")
	}
	pinned, err := DecodePinned(encoded, "address/current/addr_test1")
	if err != nil || pinned.SnapshotEvent != 99 {
		t.Fatalf("pinned decode failed: %#v, %v", pinned, err)
	}
}

func TestCursorRejectsTampering(t *testing.T) {
	t.Parallel()
	encoded, err := Encode(Value{
		Scope:         "address/current/a",
		SnapshotEvent: 1,
		LastKey:       "k",
	})
	if err != nil {
		t.Fatal(err)
	}
	last := encoded[len(encoded)-1]
	replacement := byte('A')
	if last == replacement {
		replacement = 'B'
	}
	tampered := encoded[:len(encoded)-1] + string(replacement)
	if _, err := Decode(tampered, "address/current/a", 1); err == nil {
		t.Fatal("tampered cursor must fail")
	}
}

func TestCursorSupportsOriginSnapshot(t *testing.T) {
	t.Parallel()
	encoded, err := Encode(Value{
		Scope:         "address/current/a",
		SnapshotEvent: 0,
		LastKey:       "k",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(encoded, "address/current/a", 0); err != nil {
		t.Fatal(err)
	}
}
