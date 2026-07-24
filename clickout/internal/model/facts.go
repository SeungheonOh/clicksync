package model

type AtPoint struct {
	Tip       bool
	BlockHash *Hash32
}

type InputRole string

const (
	InputRegular    InputRole = "regular"
	InputCollateral InputRole = "collateral"
	InputReference  InputRole = "reference"
)

type OutputKind string

const (
	OutputRegular          OutputKind = "regular"
	OutputCollateralReturn OutputKind = "collateral_return"
	OutputGenesis          OutputKind = "genesis"
)

type OutputAsset struct {
	PolicyID PolicyID `json:"policy_id"`
	Name     Bytes    `json:"name"`
	Quantity uint64   `json:"quantity"`
}

type SignedAssetQuantity struct {
	PolicyID PolicyID `json:"policy_id"`
	Name     Bytes    `json:"name"`
	Quantity int64    `json:"quantity"`
}

type Output struct {
	Ref                     UTxORef       `json:"ref"`
	ProducingTx             Hash32        `json:"producing_tx"`
	BodyOrdinal             uint32        `json:"body_ordinal"`
	BlockHash               Hash32        `json:"block_hash"`
	BlockHeight             uint64        `json:"block_height"`
	Kind                    OutputKind    `json:"kind"`
	Address                 Bytes         `json:"address"`
	AddressText             string        `json:"address_text,omitempty"`
	PaymentCredentialKind   string        `json:"payment_credential_kind"`
	PaymentCredentialHash   Bytes         `json:"payment_credential_hash,omitempty"`
	Lovelace                uint64        `json:"lovelace"`
	Assets                  []OutputAsset `json:"assets"`
	DatumKind               string        `json:"datum_kind"`
	DatumHash               *Hash32       `json:"datum_hash,omitempty"`
	InlineDatumCBOR         Bytes         `json:"inline_datum_cbor,omitempty"`
	ReferenceScriptHash     *PolicyID     `json:"reference_script_hash,omitempty"`
	ReferenceScriptLanguage string        `json:"reference_script_language,omitempty"`
}

type Spend struct {
	Source               UTxORef   `json:"source"`
	ConsumingTx          Hash32    `json:"consuming_tx"`
	ConsumingBlockHash   Hash32    `json:"consuming_block_hash"`
	ConsumingBlockHeight uint64    `json:"consuming_block_height"`
	Role                 InputRole `json:"role"`
	BodyOrdinal          uint32    `json:"body_ordinal"`
	IsConsumed           bool      `json:"is_consumed"`
	SourceResolved       bool      `json:"source_resolved"`
	SourceOutput         *Output   `json:"source_output,omitempty"`
}

type OutputState struct {
	Output        Output         `json:"output"`
	Uses          []Spend        `json:"uses"`
	UsesTruncated bool           `json:"uses_truncated"`
	SpentBy       *Hash32        `json:"spent_by,omitempty"`
	Consumption   *FlowHyperedge `json:"consumption,omitempty"`
	IsCurrent     bool           `json:"is_current"`
}

type Transaction struct {
	Hash                Hash32                `json:"hash"`
	BlockHash           Hash32                `json:"block_hash"`
	BlockHeight         uint64                `json:"block_height"`
	Order               uint32                `json:"order"`
	ParentHash          *Hash32               `json:"parent_hash,omitempty"`
	SubtransactionIndex *uint32               `json:"subtransaction_index,omitempty"`
	Era                 string                `json:"era"`
	Phase2Valid         bool                  `json:"phase2_valid"`
	FlowKind            string                `json:"flow_kind"`
	DeclaredFee         *uint64               `json:"declared_fee,omitempty"`
	EffectiveFee        *uint64               `json:"effective_fee,omitempty"`
	MintApplied         bool                  `json:"mint_is_applied"`
	Mint                []SignedAssetQuantity `json:"mint"`
	Inputs              []Spend               `json:"inputs"`
	Outputs             []Output              `json:"outputs"`
}

type AddressPage struct {
	Address Bytes         `json:"address"`
	State   string        `json:"state"`
	Items   []OutputState `json:"items"`
	Cursor  string        `json:"cursor,omitempty"`
}

type DatumObservation struct {
	DatumHash  Hash32 `json:"datum_hash"`
	TxHash     Hash32 `json:"tx_hash"`
	BlockHash  Hash32 `json:"block_hash"`
	SourceKind string `json:"source_kind"`
	Active     bool   `json:"active"`
}

type Datum struct {
	Hash               Hash32             `json:"hash"`
	BodyCBOR           Bytes              `json:"body_cbor,omitempty"`
	BodyVerified       bool               `json:"body_verified"`
	ActiveObservations []DatumObservation `json:"active_observations"`
}

type Withdrawal struct {
	TxHash         Hash32 `json:"tx_hash"`
	RewardAccount  Bytes  `json:"reward_account"`
	RewardText     string `json:"reward_text,omitempty"`
	Lovelace       uint64 `json:"lovelace"`
	BodyOrdinal    uint32 `json:"body_ordinal"`
	Applied        bool   `json:"is_applied"`
	CredentialKind string `json:"credential_kind"`
	CredentialHash Bytes  `json:"credential_hash,omitempty"`
}

type ResolvedTarget struct {
	Status            string    `json:"status"`
	SourceUTxO        *UTxORef  `json:"source_utxo,omitempty"`
	PolicyID          *PolicyID `json:"policy_id,omitempty"`
	RewardAccount     Bytes     `json:"reward_account,omitempty"`
	BodyOrdinal       *uint32   `json:"body_ordinal,omitempty"`
	ProcedureIdentity Bytes     `json:"procedure_identity,omitempty"`
	ScriptHash        Bytes     `json:"script_hash,omitempty"`
	SourceOutput      *Output   `json:"source_output,omitempty"`
}

type Redeemer struct {
	TxHash     Hash32         `json:"tx_hash"`
	PurposeTag uint8          `json:"purpose_tag"`
	Purpose    string         `json:"purpose"`
	Index      uint32         `json:"index"`
	DataCBOR   Bytes          `json:"data_cbor"`
	Memory     uint64         `json:"execution_memory"`
	Steps      uint64         `json:"execution_steps"`
	Applied    bool           `json:"is_applied"`
	Target     ResolvedTarget `json:"resolved_target"`
}

type TransactionMetadata struct {
	TxHash      Hash32   `json:"tx_hash"`
	Labels      []uint64 `json:"labels"`
	MapCBOR     Bytes    `json:"map_cbor"`
	ByteLength  uint64   `json:"byte_length"`
	ContentHash Hash32   `json:"content_hash"`
}

type FeeSink struct {
	TxHash   Hash32 `json:"tx_hash"`
	Lovelace uint64 `json:"lovelace"`
}

type MintDelta struct {
	TxHash   Hash32              `json:"tx_hash"`
	Asset    SignedAssetQuantity `json:"asset"`
	IsSource bool                `json:"is_source"`
	IsSink   bool                `json:"is_sink"`
}

type FlowHyperedge struct {
	Transaction         Hash32       `json:"transaction"`
	Inputs              []Spend      `json:"inputs"`
	ConsumedInputs      []UTxORef    `json:"consumed_inputs"`
	ConsumedInputValues []Output     `json:"consumed_input_values"`
	ProducedOutputs     []Output     `json:"produced_outputs"`
	AppliedWithdrawals  []Withdrawal `json:"applied_withdrawals"`
	FeeSink             *FeeSink     `json:"fee_sink,omitempty"`
	MintDeltas          []MintDelta  `json:"mint_deltas"`
}

type Trace struct {
	Direction  string          `json:"direction"`
	Asset      AssetSelector   `json:"asset"`
	Depth      uint32          `json:"depth"`
	Visited    uint32          `json:"visited"`
	Hyperedges []FlowHyperedge `json:"hyperedges"`
}
