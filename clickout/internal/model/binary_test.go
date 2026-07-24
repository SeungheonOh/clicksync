package model

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestBinaryRoundTrips(t *testing.T) {
	t.Parallel()
	datasetText := strings.Repeat("12", 16)
	datasetID, err := ParseDatasetID(datasetText)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(datasetID)
	if err != nil {
		t.Fatal(err)
	}
	var decodedDatasetID DatasetID
	if err := json.Unmarshal(encoded, &decodedDatasetID); err != nil {
		t.Fatal(err)
	}
	if decodedDatasetID != datasetID ||
		decodedDatasetID.String() != datasetText {
		t.Fatalf("dataset ID round trip mismatch: %s", decodedDatasetID)
	}

	hashText := strings.Repeat("ab", 32)
	hash, err := ParseHash32(hashText)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err = json.Marshal(hash)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Hash32
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != hash {
		t.Fatalf("hash round trip mismatch: %s", decoded)
	}

	raw := Bytes{0x00, 0xff, 0x80, 0x41}
	encoded, err = json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	var decodedRaw Bytes
	if err := json.Unmarshal(encoded, &decodedRaw); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decodedRaw, raw) {
		t.Fatalf("binary round trip mismatch: %x", decodedRaw)
	}
}

func TestUTxORefAndAssetParsing(t *testing.T) {
	t.Parallel()
	refText := strings.Repeat("01", 32) + "#4294967295"
	ref, err := ParseUTxORef(refText)
	if err != nil {
		t.Fatal(err)
	}
	if ref.String() != refText {
		t.Fatalf("got %q", ref)
	}
	if _, err := ParseUTxORef(strings.Repeat("01", 32) + "#4294967296"); err == nil {
		t.Fatal("expected index overflow rejection")
	}
	assetText := strings.Repeat("02", 28) + ".00ff"
	asset, err := ParseAssetSelector(assetText)
	if err != nil {
		t.Fatal(err)
	}
	if asset.String() != assetText {
		t.Fatalf("got %q", asset.String())
	}
	emptyName := strings.Repeat("02", 28) + "."
	if _, err := ParseAssetSelector(emptyName); err != nil {
		t.Fatalf("empty asset name must be accepted: %v", err)
	}
}

func TestRejectsNonCanonicalBinaryText(t *testing.T) {
	t.Parallel()
	for _, value := range []string{
		strings.Repeat("AB", 16),
		strings.Repeat("00", 15),
		strings.Repeat("0g", 16),
		"12121212-1212-1212-1212-121212121212",
	} {
		if _, err := ParseDatasetID(value); err == nil {
			t.Fatalf("accepted invalid dataset ID %q", value)
		}
	}
	if _, err := DatasetIDFromBytes(make([]byte, 15)); err == nil {
		t.Fatal("accepted a short dataset ID")
	}
	for _, value := range []string{
		strings.Repeat("AB", 32),
		strings.Repeat("00", 31),
		strings.Repeat("0g", 32),
	} {
		if _, err := ParseHash32(value); err == nil {
			t.Fatalf("accepted invalid hash %q", value)
		}
	}
}
