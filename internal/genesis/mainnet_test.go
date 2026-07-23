package genesis

import "testing"

func TestEmbeddedMainnetIdentity(t *testing.T) {
	bundle, err := Mainnet()
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.Block.Transactions) != int(AVVMEntries) {
		t.Fatalf(
			"embedded genesis transactions = %d, want %d",
			len(bundle.Block.Transactions),
			AVVMEntries,
		)
	}
	var supply uint64
	for _, transaction := range bundle.Block.Transactions {
		if len(transaction.Outputs) != 1 {
			t.Fatalf(
				"embedded genesis transaction %x has %d outputs",
				transaction.Hash,
				len(transaction.Outputs),
			)
		}
		supply += transaction.Outputs[0].Lovelace
	}
	if supply != InitialSupply {
		t.Fatalf("embedded genesis supply = %d, want %d", supply, InitialSupply)
	}
	if bundle.Proof.ByronJSONHash != byronJSONHash ||
		bundle.Proof.ShelleyJSONHash != shelleyJSONHash {
		t.Fatalf("embedded genesis proof = %+v", bundle.Proof)
	}
}
