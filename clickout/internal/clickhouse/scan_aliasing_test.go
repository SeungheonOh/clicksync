package clickhouse

import (
	"testing"

	"github.com/clicksync-project/clickout/internal/model"
)

type aliasingOutputScanner struct {
	tx, block, address []byte
}

func (row *aliasingOutputScanner) Scan(dest ...any) error {
	*(dest[0].(*[]byte)) = row.tx
	*(dest[1].(*uint32)) = 0
	*(dest[2].(*uint32)) = 0
	*(dest[3].(*[]byte)) = row.block
	*(dest[4].(*uint64)) = 1
	*(dest[5].(*string)) = "regular"
	*(dest[6].(*[]byte)) = row.address
	*(dest[7].(*string)) = "none"
	*(dest[8].(**string)) = nil
	*(dest[9].(*uint64)) = 1
	*(dest[10].(*[]string)) = []string{}
	*(dest[11].(*[]string)) = []string{}
	*(dest[12].(*[]uint64)) = []uint64{}
	*(dest[13].(*string)) = "none"
	*(dest[14].(**string)) = nil
	*(dest[15].(**string)) = nil
	*(dest[16].(**string)) = nil
	return nil
}

func TestScanOutputDoesNotRetainDriverBuffers(t *testing.T) {
	t.Parallel()
	row := &aliasingOutputScanner{
		tx:      make([]byte, 32),
		block:   make([]byte, 32),
		address: testByronAddress(),
	}
	for index := range row.tx {
		row.tx[index] = 0x11
		row.block[index] = 0x22
	}
	output, err := scanOutput(row)
	if err != nil {
		t.Fatal(err)
	}
	for index := range row.tx {
		row.tx[index] = 0xff
		row.block[index] = 0xff
	}
	for index := range row.address {
		row.address[index] = 0xff
	}
	if output.Ref.TxHash[0] != 0x11 || output.BlockHash[0] != 0x22 {
		t.Fatalf("fixed hashes retained scanner storage: %#v", output)
	}
	if len(output.Address) != len(testByronAddress()) ||
		output.Address[0] != 0x82 ||
		output.Address[3] != 0x58 {
		t.Fatalf("address retained scanner storage: %x", []byte(output.Address))
	}
}

func TestVerifiedInlineDatumDoesNotRetainDriverBuffer(t *testing.T) {
	t.Parallel()
	body := []byte{0xd8, 0x79, 0x80}
	hash := calculateContentHash(body)
	verified, err := verifyInlineDatumBody(hash, body, uint32(len(body)), hash)
	if err != nil {
		t.Fatal(err)
	}
	body[0] = 0
	if verified[0] != 0xd8 {
		t.Fatalf("verified datum retained driver storage: %x", []byte(verified))
	}
}

type aliasingHashRows struct {
	shared []byte
	next   int
}

func (rows *aliasingHashRows) Next() bool {
	if rows.next == 2 {
		return false
	}
	for index := range rows.shared {
		rows.shared[index] = byte(rows.next + 1)
	}
	rows.next++
	return true
}

func (rows *aliasingHashRows) Scan(dest ...any) error {
	*(dest[0].(*[]byte)) = rows.shared
	return nil
}

func (*aliasingHashRows) Err() error { return nil }

func TestScanHashesCopiesReusedMultiRowBuffer(t *testing.T) {
	t.Parallel()
	rows := &aliasingHashRows{shared: make([]byte, 32)}
	hashes, err := scanHashes(rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(hashes) != 2 || hashes[0] != repeatedHash(1) || hashes[1] != repeatedHash(2) {
		t.Fatalf("multi-row buffer aliasing corrupted results: %#v", hashes)
	}
}

func repeatedHash(value byte) model.Hash32 {
	var result model.Hash32
	for index := range result {
		result[index] = value
	}
	return result
}
