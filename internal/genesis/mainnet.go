// Package genesis verifies and materializes the official mainnet initial UTxO
// distribution without running cardano-node or retaining genesis JSON.
package genesis

import (
	"bytes"
	"compress/gzip"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"time"

	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"golang.org/x/crypto/blake2b"

	"clicksync/internal/model"
	"clicksync/internal/publication"
	"clicksync/internal/store"
)

const (
	AVVMEntries   = uint32(14505)
	InitialSupply = uint64(31112484745000000)

	maxByronBytes   = 2 * 1024 * 1024
	maxShelleyBytes = 64 * 1024
)

var (
	byronJSONHash   = mustHash("dbbdaeab0ea4ea58225892d8b1294f178b417f4a9d1ed3bbf629c40d8f74e86b")
	byronGenesisID  = mustHash("5f20df933584822601f9e3f8c024eb5eb252fe8cefb24d1317dc3d432e940ebb")
	shelleyJSONHash = mustHash("1a3be38bcbb7911969283716ad7aa550250226b76a61fc51cc9a9a35d9276d81")

	//go:embed assets/mainnet-byron-genesis.json.gz
	embeddedByronJSONGZIP []byte

	//go:embed assets/mainnet-shelley-genesis.json.gz
	embeddedShelleyJSONGZIP []byte
)

type Bundle struct {
	Block  model.Block
	Source publication.Source
	Proof  store.OriginGenesisProof
}

// Mainnet returns the build-embedded and hash-pinned official mainnet genesis
// distribution. Normal synchronization never fetches genesis over the network.
func Mainnet() (Bundle, error) {
	byronJSON, err := decompressBounded(
		embeddedByronJSONGZIP,
		maxByronBytes,
		"Byron",
	)
	if err != nil {
		return Bundle{}, err
	}
	shelleyJSON, err := decompressBounded(
		embeddedShelleyJSONGZIP,
		maxShelleyBytes,
		"Shelley",
	)
	if err != nil {
		return Bundle{}, err
	}
	return ParseMainnet(byronJSON, shelleyJSON)
}

func decompressBounded(compressed []byte, maximum int64, name string) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, fmt.Errorf("open embedded %s genesis: %w", name, err)
	}
	defer reader.Close()
	body, err := io.ReadAll(io.LimitReader(reader, maximum+1))
	if err != nil {
		return nil, fmt.Errorf("decompress embedded %s genesis: %w", name, err)
	}
	if int64(len(body)) > maximum {
		return nil, fmt.Errorf("embedded %s genesis exceeds %d bytes", name, maximum)
	}
	return body, nil
}

func ParseMainnet(byronJSON, shelleyJSON []byte) (Bundle, error) {
	if model.Hash32(blake2b.Sum256(byronJSON)) != byronJSONHash {
		return Bundle{}, errors.New("official Byron genesis JSON hash mismatch")
	}
	if model.Hash32(blake2b.Sum256(shelleyJSON)) != shelleyJSONHash {
		return Bundle{}, errors.New("official Shelley genesis JSON hash mismatch")
	}
	byronGenesis, err := byron.NewByronGenesisFromReader(bytes.NewReader(byronJSON))
	if err != nil {
		return Bundle{}, fmt.Errorf("parse official Byron genesis: %w", err)
	}
	if byronGenesis.ProtocolConsts.ProtocolMagic != 764824073 ||
		byronGenesis.StartTime != 1506203091 ||
		len(byronGenesis.AvvmDistr) != int(AVVMEntries) ||
		len(byronGenesis.NonAvvmBalances) != 0 {
		return Bundle{}, errors.New("official Byron genesis semantic identity mismatch")
	}
	var shelley struct {
		NetworkID    string                     `json:"networkId"`
		NetworkMagic uint32                     `json:"networkMagic"`
		SystemStart  string                     `json:"systemStart"`
		InitialFunds map[string]json.RawMessage `json:"initialFunds"`
		Staking      struct {
			Pools map[string]json.RawMessage `json:"pools"`
			Stake map[string]json.RawMessage `json:"stake"`
		} `json:"staking"`
	}
	if err := json.Unmarshal(shelleyJSON, &shelley); err != nil {
		return Bundle{}, fmt.Errorf("parse official Shelley genesis: %w", err)
	}
	if shelley.NetworkID != "Mainnet" ||
		shelley.NetworkMagic != 764824073 ||
		shelley.SystemStart != "2017-09-23T21:44:51Z" ||
		len(shelley.InitialFunds) != 0 ||
		len(shelley.Staking.Pools) != 0 ||
		len(shelley.Staking.Stake) != 0 {
		return Bundle{}, errors.New("official Shelley genesis semantic identity mismatch")
	}

	utxos, err := byronGenesis.GenesisUtxos()
	if err != nil {
		return Bundle{}, fmt.Errorf("derive official Byron genesis UTxOs: %w", err)
	}
	if len(utxos) != int(AVVMEntries) {
		return Bundle{}, fmt.Errorf("derived %d genesis UTxOs, want %d", len(utxos), AVVMEntries)
	}
	transactions := make([]model.Transaction, 0, len(utxos))
	var supply uint64
	for index, utxo := range utxos {
		input, ok := utxo.Id.(byron.ByronTransactionInput)
		if !ok {
			return Bundle{}, fmt.Errorf("genesis UTxO %d input type is %T", index, utxo.Id)
		}
		output, ok := utxo.Output.(byron.ByronTransactionOutput)
		if !ok {
			return Bundle{}, fmt.Errorf("genesis UTxO %d output type is %T", index, utxo.Output)
		}
		if input.OutputIndex != 0 || output.OutputAmount == 0 {
			return Bundle{}, fmt.Errorf("genesis UTxO %d has invalid index/value", index)
		}
		if output.OutputAmount > math.MaxUint64-supply {
			return Bundle{}, errors.New("official genesis supply overflows UInt64")
		}
		supply += output.OutputAmount
		address, err := output.OutputAddress.Bytes()
		if err != nil {
			return Bundle{}, fmt.Errorf("encode genesis address %d: %w", index, err)
		}
		var txHash model.Hash32
		copy(txHash[:], input.TxId.Bytes())
		if model.Hash32(blake2b.Sum256(address)) != txHash {
			return Bundle{}, fmt.Errorf("genesis UTxO %d transaction hash differs from address hash", index)
		}
		transactions = append(transactions, model.Transaction{
			Hash:        txHash,
			Era:         "Byron",
			Phase2Valid: true,
			FlowKind:    "genesis",
			Outputs: []model.Output{{
				TransactionHash:       txHash,
				Kind:                  "genesis",
				Address:               address,
				PaymentCredentialKind: "none",
				Lovelace:              output.OutputAmount,
				DatumKind:             "none",
			}},
		})
	}
	if supply != InitialSupply {
		return Bundle{}, fmt.Errorf("derived genesis supply %d, want %d", supply, InitialSupply)
	}
	sort.Slice(transactions, func(left, right int) bool {
		return bytes.Compare(transactions[left].Hash[:], transactions[right].Hash[:]) < 0
	})
	for index := range transactions {
		if index > 0 && transactions[index-1].Hash == transactions[index].Hash {
			return Bundle{}, errors.New("official genesis contains a duplicate transaction reference")
		}
		transactions[index].Order = uint32(index)
		transactions[index].Outputs[0].TransactionOrder = uint32(index)
	}
	return Bundle{
		Block: model.Block{
			Hash:         byronGenesisID,
			Slot:         0,
			Number:       0,
			Era:          "Byron",
			Type:         -1,
			Synthetic:    true,
			Transactions: transactions,
			ObservedAt:   time.Unix(int64(byronGenesis.StartTime), 0).UTC(),
		},
		Source: publication.OfficialMainnetGenesisSource(),
		Proof: store.OriginGenesisProof{
			ByronJSONHash:   byronJSONHash,
			ShelleyJSONHash: shelleyJSONHash,
			AVVMEntries:     AVVMEntries,
			InitialSupply:   InitialSupply,
		},
	}, nil
}

func mustHash(encoded string) model.Hash32 {
	decoded, err := hex.DecodeString(encoded)
	if err != nil || len(decoded) != 32 {
		panic("invalid compiled mainnet genesis hash")
	}
	var hash model.Hash32
	copy(hash[:], decoded)
	return hash
}
