package normalize

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"

	"cardano-clicksync/internal/model"
)

type realBlockFixture struct {
	name      string
	file      string
	blockType uint
	rawSHA256 string
	blockHash string
}

var realBlockFixtures = []realBlockFixture{
	{
		name:      "Byron main",
		file:      "byron-main.cbor.gz",
		blockType: ledger.BlockTypeByronMain,
		rawSHA256: "bd58984bd312930fea49b715af6ddc3b4cfa7da64143fa602edd3af314e7ae63",
		blockHash: "1451a0dbf16cfeddf4991a838961df1b08a68f43a19c0eb3b36cc4029c77a2d8",
	},
	{
		name:      "Byron EBB",
		file:      "byron-ebb-testnet.cbor.gz",
		blockType: ledger.BlockTypeByronEbb,
		rawSHA256: "a4b0f5247bd7fa3be5c8f3b8d328423a3e886c81ff78fba8d92b14812bc7fef9",
		blockHash: "8f8602837f7c6f8b8867dd1cbc1842cf51a27eaed2c70ef48325d00f8efb320f",
	},
	{
		name:      "Shelley",
		file:      "shelley.cbor.gz",
		blockType: ledger.BlockTypeShelley,
		rawSHA256: "6a7b7393e68bdde768f6903dbb9319e4c77e461a5748f8b34f2381f79b638b03",
		blockHash: "2308cdd4c0bf8b8bf92523bdd1dd31640c0f42ff079d985fcc07c36cbf915c2b",
	},
	{
		name:      "Allegra",
		file:      "allegra.cbor.gz",
		blockType: ledger.BlockTypeAllegra,
		rawSHA256: "56c41ce9a67239cd5e2209aae7a61d292f4aeb4249ad6a927bb821ae185523fb",
		blockHash: "8115134ab013f6a5fd88fd2a10825177a2eedcde31cb2f1f35e492df469cf9a8",
	},
	{
		name:      "Mary",
		file:      "mary.cbor.gz",
		blockType: ledger.BlockTypeMary,
		rawSHA256: "07b446bea4f5bd2e44a90548bfdf7219ed55a0d7141e0154e72e1ad73876b543",
		blockHash: "d36ab36f451e9fcbd4247daef45ce5be9a4b918fce5ee97a63b8aeac606fca03",
	},
	{
		name:      "Alonzo",
		file:      "alonzo.cbor.gz",
		blockType: ledger.BlockTypeAlonzo,
		rawSHA256: "8f3a4fdc760f34506c21c114cb3d6e253e1e6637576c0a6b139eb49170ca7bcc",
		blockHash: "1d7974cb01cc9e3fbe9dd7594795a36b21cb1deb2f1b70a0625332c91bd7e5a7",
	},
	{
		name:      "Babbage",
		file:      "babbage.cbor.gz",
		blockType: ledger.BlockTypeBabbage,
		rawSHA256: "9384202e72b6e9c460f38d6836059c9d3769e50b59f3fbac43fed3490765e57a",
		blockHash: "db19fcfaba30607e363113b0a13616e6a9da5aa48b86ec2c033786f0a2e13f7d",
	},
	{
		name:      "Conway",
		file:      "conway.cbor.gz",
		blockType: ledger.BlockTypeConway,
		rawSHA256: "154393dc5c75318fb67148f59677de5afa74aa4e64ee96980043b2f14111eb32",
		blockHash: "27807a70215e3e018eec9be8c619c692e06a78ebcb63daf90d7abe823f3bbf47",
	},
	{
		name:      "Dijkstra Musashi",
		file:      "dijkstra-musashi.cbor.gz",
		blockType: ledger.BlockTypeDijkstra,
		rawSHA256: "465b18ecd55cef0218304861bddcf1dd42d16449c042b9b952898d3993619e56",
		blockHash: "3df256c7ebfd46d2de897dd8bd7cd7c4c5a958176380dbc607c0b929e5227f1a",
	},
}

func TestRealBlocksDecodeAndNormalize(t *testing.T) {
	var observedTransactions int
	var observedInputs int
	var observedOutputs int
	credentialOutputs := map[uint]int{
		ledger.BlockTypeAlonzo:  0,
		ledger.BlockTypeBabbage: 0,
		ledger.BlockTypeConway:  0,
	}
	for _, fixture := range realBlockFixtures {
		t.Run(fixture.name, func(t *testing.T) {
			raw := readFixture(t, fixture)
			facts, err := Decode(fixture.blockType, raw)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			gotHash := hex.EncodeToString(facts.Hash[:])
			if gotHash != fixture.blockHash {
				t.Fatalf("normalized hash = %s, want %s", gotHash, fixture.blockHash)
			}
			if got := hex.EncodeToString(facts.Hash[:]); got != gotHash {
				t.Fatalf("normalized hash = %s, want %s", got, gotHash)
			}
			if fixture.blockType == ledger.BlockTypeByronEbb && len(facts.Transactions) != 0 {
				t.Fatal("Byron EBB unexpectedly produced transaction facts")
			}
			assertEffectiveFlowInvariants(t, facts)
			observedTransactions += len(facts.Transactions)
			for _, tx := range facts.Transactions {
				observedInputs += len(tx.Inputs)
				observedOutputs += len(tx.Outputs)
				for _, output := range tx.Outputs {
					if output.PaymentCredentialKind == nil {
						if output.PaymentCredentialHash != nil {
							t.Fatal("output retained credential hash without a kind")
						}
						continue
					}
					switch *output.PaymentCredentialKind {
					case "none":
						if output.PaymentCredentialHash != nil {
							t.Fatal("payment-credential-none output retained a hash")
						}
					case "key", "script":
						if output.PaymentCredentialHash == nil {
							t.Fatalf(
								"%s output %x#%d lacks payment credential hash",
								fixture.name,
								output.TransactionHash,
								output.Index,
							)
						}
						credentialOutputs[fixture.blockType]++
					default:
						t.Fatalf(
							"%s output %x#%d has unknown payment credential kind %q",
							fixture.name,
							output.TransactionHash,
							output.Index,
							*output.PaymentCredentialKind,
						)
					}
				}
			}
			assertAllowedCBORIntegrity(t, raw, facts)
		})
	}
	if observedTransactions == 0 || observedInputs == 0 || observedOutputs == 0 {
		t.Fatalf(
			"fixture set lacks real flow coverage: transactions=%d inputs=%d outputs=%d",
			observedTransactions,
			observedInputs,
			observedOutputs,
		)
	}
	for blockType, count := range credentialOutputs {
		if count == 0 {
			t.Errorf("era fixture type %d lacks payment credential coverage", blockType)
		}
	}
}

func TestDecodeSkipsBlockBodyHashValidation(t *testing.T) {
	var fixture realBlockFixture
	for _, candidate := range realBlockFixtures {
		if candidate.blockType == ledger.BlockTypeBabbage {
			fixture = candidate
			break
		}
	}
	raw := readFixture(t, fixture)
	decoded, err := ledger.NewBlockFromCbor(
		fixture.blockType,
		raw,
		lcommon.VerifyConfig{
			SkipBodyHashValidation:    true,
			SkipTransactionValidation: true,
			SkipStakePoolValidation:   true,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	withBodyHash, ok := decoded.(interface {
		BlockBodyHash() lcommon.Blake2b256
	})
	if !ok {
		t.Fatalf("decoded block %T does not expose its body hash", decoded)
	}
	expected := withBodyHash.BlockBodyHash()
	offset := bytes.Index(raw, expected.Bytes())
	if offset < 0 {
		t.Fatal("encoded header body hash was not found in raw block")
	}
	mutated := bytes.Clone(raw)
	mutated[offset] ^= 0xff

	if _, err := Decode(fixture.blockType, mutated); err != nil {
		t.Fatalf("non-validating decode rejected body-hash mismatch: %v", err)
	}
	if _, err := ledger.NewBlockFromCbor(
		fixture.blockType,
		mutated,
		lcommon.VerifyConfig{
			SkipTransactionValidation: true,
			SkipStakePoolValidation:   true,
		},
	); err == nil {
		t.Fatal("test mutation did not create a body-hash mismatch")
	}
}

func readFixture(t *testing.T, fixture realBlockFixture) []byte {
	t.Helper()
	path := filepath.Join("..", "..", "testdata", "blocks", fixture.file)
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	if got := hex.EncodeToString(digest[:]); got != fixture.rawSHA256 {
		t.Fatalf("raw SHA-256 = %s, want %s", got, fixture.rawSHA256)
	}
	return raw
}

func assertEffectiveFlowInvariants(t *testing.T, block model.Block) {
	t.Helper()
	for _, tx := range block.Transactions {
		if tx.Phase2Valid && tx.FlowKind != "regular" {
			t.Fatalf("valid transaction %x has flow kind %q", tx.Hash, tx.FlowKind)
		}
		if !tx.Phase2Valid && tx.FlowKind != "collateral" {
			t.Fatalf("invalid transaction %x has flow kind %q", tx.Hash, tx.FlowKind)
		}
		for _, input := range tx.Inputs {
			wantConsumed := false
			switch input.Role {
			case "regular":
				wantConsumed = tx.Phase2Valid
			case "collateral":
				wantConsumed = !tx.Phase2Valid
			case "reference":
			default:
				t.Fatalf("transaction %x has unknown input role %q", tx.Hash, input.Role)
			}
			if input.Consumed != wantConsumed {
				t.Fatalf(
					"transaction %x role %s consumed=%t, want %t",
					tx.Hash,
					input.Role,
					input.Consumed,
					wantConsumed,
				)
			}
		}
		for _, output := range tx.Outputs {
			wantKind := "regular"
			if !tx.Phase2Valid {
				wantKind = "collateral_return"
			}
			if output.Kind != wantKind {
				t.Fatalf(
					"transaction %x output kind=%q, want %q",
					tx.Hash,
					output.Kind,
					wantKind,
				)
			}
		}
	}
}

func assertAllowedCBORIntegrity(t *testing.T, rawBlock []byte, block model.Block) {
	t.Helper()
	for _, datum := range block.Datums {
		if len(datum.CBOR) == 0 || len(datum.CBOR) == len(rawBlock) {
			t.Fatalf("datum %x retained an invalid CBOR fragment length", datum.Hash)
		}
		hash := lcommon.Blake2b256Hash(datum.CBOR)
		if !reflect.DeepEqual(hash.Bytes(), datum.Hash[:]) {
			t.Fatalf("datum %x CBOR hash mismatch", datum.Hash)
		}
	}
	for _, tx := range block.Transactions {
		for _, redeemer := range tx.Redeemers {
			if len(redeemer.DataCBOR) == 0 || len(redeemer.DataCBOR) == len(rawBlock) {
				t.Fatalf("redeemer in %x retained an invalid CBOR fragment length", tx.Hash)
			}
			hash := lcommon.Blake2b256Hash(redeemer.DataCBOR)
			if !reflect.DeepEqual(hash.Bytes(), redeemer.DataHash[:]) {
				t.Fatalf("redeemer in %x CBOR hash mismatch", tx.Hash)
			}
		}
		if tx.Metadata != nil {
			if len(tx.Metadata.CBOR) == 0 || len(tx.Metadata.CBOR) == len(rawBlock) {
				t.Fatalf("metadata in %x retained an invalid CBOR fragment length", tx.Hash)
			}
			hash := lcommon.Blake2b256Hash(tx.Metadata.CBOR)
			if !reflect.DeepEqual(hash.Bytes(), tx.Metadata.ContentHash[:]) {
				t.Fatalf("metadata in %x CBOR hash mismatch", tx.Hash)
			}
		}
	}
}

func TestPersistedModelHasOnlyAllowedCBORFields(t *testing.T) {
	allowed := map[string]bool{
		"DatumBody.CBOR":    false,
		"Redeemer.DataCBOR": false,
		"Metadata.CBOR":     false,
	}
	visited := make(map[reflect.Type]bool)
	var inspect func(reflect.Type)
	inspect = func(value reflect.Type) {
		for value.Kind() == reflect.Pointer || value.Kind() == reflect.Slice {
			value = value.Elem()
		}
		if value.Kind() != reflect.Struct ||
			value.PkgPath() != reflect.TypeOf(model.Block{}).PkgPath() ||
			visited[value] {
			return
		}
		visited[value] = true
		for index := range value.NumField() {
			field := value.Field(index)
			key := value.Name() + "." + field.Name
			if strings.Contains(strings.ToLower(field.Name), "cbor") {
				if _, ok := allowed[key]; !ok {
					t.Errorf("forbidden persisted CBOR field %s", key)
				} else {
					allowed[key] = true
				}
			}
			inspect(field.Type)
		}
	}
	inspect(reflect.TypeOf(model.Block{}))
	for field, found := range allowed {
		if !found {
			t.Errorf("expected allowed CBOR field %s was not found", field)
		}
	}
}

func BenchmarkDecodeAndNormalizeConwayParallel(b *testing.B) {
	var fixture realBlockFixture
	for _, candidate := range realBlockFixtures {
		if candidate.blockType == ledger.BlockTypeConway {
			fixture = candidate
			break
		}
	}
	raw := readFixtureForBenchmark(b, fixture)
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(worker *testing.PB) {
		for worker.Next() {
			if _, err := Decode(fixture.blockType, raw); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkDecodeAndNormalizeLargeConway(b *testing.B) {
	var fixture realBlockFixture
	for _, candidate := range realBlockFixtures {
		if candidate.blockType == ledger.BlockTypeConway {
			fixture = candidate
			break
		}
	}
	raw := largeConwayFixture(b, readFixtureForBenchmark(b, fixture), 256)
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := Decode(fixture.blockType, raw); err != nil {
			b.Fatal(err)
		}
	}
}

func largeConwayFixture(b *testing.B, raw []byte, repeats int) []byte {
	b.Helper()
	block, err := ledger.NewConwayBlockFromCbor(
		raw,
		lcommon.VerifyConfig{
			SkipBodyHashValidation:    true,
			SkipTransactionValidation: true,
			SkipStakePoolValidation:   true,
		},
	)
	if err != nil {
		b.Fatal(err)
	}
	if len(block.TransactionBodies) == 0 ||
		len(block.TransactionBodies) != len(block.TransactionWitnessSets) {
		b.Fatalf(
			"Conway fixture has bodies/witnesses %d/%d",
			len(block.TransactionBodies),
			len(block.TransactionWitnessSets),
		)
	}
	large := &ledger.ConwayBlock{
		BlockHeader:            block.BlockHeader,
		TransactionMetadataSet: block.TransactionMetadataSet,
	}
	for repeat := range repeats {
		offset := repeat * len(block.TransactionBodies)
		large.TransactionBodies = append(
			large.TransactionBodies,
			block.TransactionBodies...,
		)
		large.TransactionWitnessSets = append(
			large.TransactionWitnessSets,
			block.TransactionWitnessSets...,
		)
		for _, index := range block.InvalidTransactions {
			large.InvalidTransactions = append(
				large.InvalidTransactions,
				uint(offset)+index,
			)
		}
	}
	encoded, err := large.MarshalCBOR()
	if err != nil {
		b.Fatal(err)
	}
	return encoded
}

func readFixtureForBenchmark(b *testing.B, fixture realBlockFixture) []byte {
	b.Helper()
	path := filepath.Join("..", "..", "testdata", "blocks", fixture.file)
	file, err := os.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	reader, err := gzip.NewReader(file)
	if err != nil {
		b.Fatal(err)
	}
	raw, err := io.ReadAll(reader)
	if err != nil {
		b.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		b.Fatal(err)
	}
	return raw
}
