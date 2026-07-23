package normalize

type Block struct {
	Era              string        `json:"era"`
	BlockType        int           `json:"block_type"`
	BlockHash        string        `json:"block_hash"`
	ParentHash       string        `json:"parent_hash"`
	Slot             uint64        `json:"slot"`
	BlockNumber      uint64        `json:"block_number"`
	TransactionCount int           `json:"transaction_count"`
	Transactions     []Transaction `json:"transactions"`
	Datums           []Datum       `json:"datums"`
}

type Transaction struct {
	TxHash      string       `json:"tx_hash"`
	TxOrder     uint32       `json:"tx_order"`
	Valid       bool         `json:"valid"`
	FlowKind    string       `json:"flow_kind"`
	FeeLovelace *string      `json:"fee_lovelace"`
	FeeKnown    bool         `json:"fee_known"`
	Inputs      []Input      `json:"inputs"`
	Outputs     []Output     `json:"outputs"`
	Mint        []AssetDelta `json:"mint"`
}

type Input struct {
	SourceTxHash string `json:"source_tx_hash"`
	SourceIndex  uint32 `json:"source_index"`
	Ordinal      uint32 `json:"ordinal"`
	Kind         string `json:"kind"`
}

type Output struct {
	TxHash      string  `json:"tx_hash"`
	OutputIndex uint32  `json:"output_index"`
	Ordinal     uint32  `json:"ordinal"`
	Kind        string  `json:"kind"`
	AddressHex  string  `json:"address_hex"`
	Lovelace    string  `json:"lovelace"`
	Assets      []Asset `json:"assets"`
	DatumKind   string  `json:"datum_kind"`
	DatumHash   *string `json:"datum_hash"`
}

type Asset struct {
	PolicyID     string `json:"policy_id"`
	AssetNameHex string `json:"asset_name_hex"`
	Quantity     string `json:"quantity"`
}

type AssetDelta struct {
	PolicyID     string `json:"policy_id"`
	AssetNameHex string `json:"asset_name_hex"`
	Quantity     string `json:"quantity"`
}

type Datum struct {
	DatumHash    string             `json:"datum_hash"`
	DatumCBORHex string             `json:"datum_cbor_hex"`
	Sources      []DatumObservation `json:"sources"`
}

type DatumObservation struct {
	Kind   string `json:"kind"`
	TxHash string `json:"tx_hash"`
}
