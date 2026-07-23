package address

import (
	"bytes"
	"testing"
)

func TestHexAndBase58RoundTripInputs(t *testing.T) {
	t.Parallel()
	decoded, err := Decode("hex:00ff8041")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, []byte{0, 0xff, 0x80, 0x41}) {
		t.Fatalf("got %x", decoded)
	}
	decoded, err = Decode("1112")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, []byte{0, 0, 0, 1}) {
		t.Fatalf("got %x", decoded)
	}
}

func TestRejectsInvalidAddress(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"", "hex:", "0OIl", "addr1invalid"} {
		if _, err := Decode(value); err == nil {
			t.Fatalf("accepted %q", value)
		}
	}
}
