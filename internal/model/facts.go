package model

import "time"

type Hash32 [32]byte
type Hash28 [28]byte

type Block struct {
	Hash                   Hash32
	ParentHash             *Hash32
	Slot                   uint64
	Number                 uint64
	Era                    string
	Type                   int16
	Synthetic              bool
	BodyHashVerified       bool
	TransactionIDsVerified bool
	Transactions           []Transaction
	Datums                 []DatumBody
	ObservedAt             time.Time
}

type Transaction struct {
	Hash                Hash32
	Order               uint32
	ParentHash          *Hash32
	SubtransactionIndex *uint32
	Era                 string
	Phase2Valid         bool
	FlowKind            string
	DeclaredFee         *uint64
	EffectiveFee        *uint64
	MintApplied         bool
	Mint                []AssetDelta
	Inputs              []Input
	Outputs             []Output
	Withdrawals         []Withdrawal
	Redeemers           []Redeemer
	Metadata            *Metadata
	DatumObservations   []DatumObservation
}

type Input struct {
	TransactionHash  Hash32
	TransactionOrder uint32
	SourceHash       Hash32
	SourceIndex      uint32
	BodyOrdinal      uint32
	Role             string
	Consumed         bool
	SourceResolved   bool
}

type Output struct {
	TransactionHash         Hash32
	TransactionOrder        uint32
	Index                   uint32
	BodyOrdinal             uint32
	Kind                    string
	Address                 []byte
	Lovelace                uint64
	Assets                  []Asset
	DatumKind               string
	DatumHash               *Hash32
	ReferenceScriptHash     *Hash28
	ReferenceScriptLanguage *string
}

type Asset struct {
	PolicyID Hash28
	Name     []byte
	Quantity uint64
}

type AssetDelta struct {
	PolicyID Hash28
	Name     []byte
	Quantity int64
}

type DatumBody struct {
	Hash         Hash32
	CBOR         []byte
	Observations []DatumObservation
}

type DatumObservation struct {
	Hash             Hash32
	TransactionHash  Hash32
	TransactionOrder uint32
	SourceKind       string
	SourceOrdinal    uint32
	OutputIndex      *uint32
}

type Withdrawal struct {
	TransactionHash  Hash32
	TransactionOrder uint32
	BodyOrdinal      uint32
	RewardAccount    []byte
	Lovelace         uint64
	Applied          bool
	CredentialKind   string
	CredentialHash   Hash28
}

type Redeemer struct {
	TransactionHash     Hash32
	TransactionOrder    uint32
	RawPurposeTag       uint8
	Purpose             string
	Index               uint32
	DataCBOR            []byte
	DataHash            Hash32
	ExUnitsMemory       uint64
	ExUnitsSteps        uint64
	Applied             bool
	TargetTxHash        *Hash32
	TargetOutputIndex   *uint32
	TargetPolicyID      *Hash28
	TargetRewardAccount []byte
	TargetBodyOrdinal   *uint32
	TargetIdentity      []byte
	ResolvedScriptHash  *Hash28
}

type Metadata struct {
	TransactionHash  Hash32
	TransactionOrder uint32
	Labels           []uint64
	CBOR             []byte
	ContentHash      Hash32
}

type PeerObservation struct {
	ID                    [16]byte
	Kind                  string
	PeerHost              string
	PeerAddress           string
	Operator              string
	N2NVersion            uint16
	NetworkMagic          uint32
	TipSlot               uint64
	TipHash               Hash32
	TipBlockNumber        uint64
	CheckpointSlot        *uint64
	CheckpointHash        *Hash32
	CheckpointBlockNumber *uint64
	CheckpointIsByronEBB  *bool
	AgreementGroup        *[16]byte
	SelectedBodySource    bool
	BodyHashVerified      bool
	PointVerified         bool
	ParentVerified        bool
	Result                string
	Reason                string
	ObservedAt            time.Time
}
