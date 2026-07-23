package clickhouse

import (
	"bytes"
	"strings"
	"testing"

	"github.com/clicksync-project/clickout/internal/cursor"
	"github.com/clicksync-project/clickout/internal/model"
)

func TestAddressRepositoryKeyRoundTripIncludesCompletePhysicalKey(t *testing.T) {
	t.Parallel()
	key := addressKey{
		AddressHash:   91,
		Address:       model.Bytes{0x61, 0x01, 0xff},
		BlockNumber:   42,
		TxHash:        repeatedHash(0x77).String(),
		OutputIndex:   3,
		PublicationID: 104,
	}
	encoded, err := encodeAddressKey(key)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeAddressKey(encoded, key.Address)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.AddressHash != key.AddressHash ||
		!bytes.Equal(decoded.Address, key.Address) ||
		decoded.BlockNumber != key.BlockNumber ||
		decoded.TxHash != key.TxHash ||
		decoded.OutputIndex != key.OutputIndex ||
		decoded.PublicationID != key.PublicationID {
		t.Fatalf("physical key did not round trip: %#v", decoded)
	}
	if _, err := decodeAddressKey(encoded, []byte{0x61, 0x02}); err != cursor.ErrInvalid {
		t.Fatalf("cursor was accepted for another raw address: %v", err)
	}
}

func TestAddressCandidateSQLUsesCompleteOrderingAndPhysicalSentinel(t *testing.T) {
	t.Parallel()
	address := []byte{0x61, 0x01}
	key := addressKey{
		AddressHash:   9,
		Address:       model.Bytes(address),
		BlockNumber:   8,
		TxHash:        repeatedHash(7).String(),
		OutputIndex:   6,
		PublicationID: 5,
	}
	sql, arguments, err := addressCandidateSQL(
		model.Snapshot{PublicationWatermark: 99},
		address,
		10,
		key,
		true,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		"(address_hash, address, block_number, tx_hash, output_index, publication_id)",
		"ORDER BY address_hash, address, block_number, tx_hash, output_index, publication_id",
		"LIMIT ?",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("candidate SQL omitted %q:\n%s", fragment, sql)
		}
	}
	if got, want := arguments[len(arguments)-1], uint64(11); got != want {
		t.Fatalf("candidate query limit = %v, want sentinel limit %v", got, want)
	}
}

func TestAddressKeyOrderingDoesNotTreatHashAsAddressIdentity(t *testing.T) {
	t.Parallel()
	left := addressKey{
		AddressHash: 4,
		Address:     model.Bytes{0x01},
		TxHash:      repeatedHash(1).String(),
	}
	right := left
	right.Address = model.Bytes{0x02}
	if compareAddressKeys(left, right) >= 0 {
		t.Fatal("raw address did not break an address-hash collision")
	}
}
