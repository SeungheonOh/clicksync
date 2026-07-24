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

type OutputUse struct {
	ConsumingTx          Hash32    `json:"consuming_tx"`
	ConsumingBlockHash   Hash32    `json:"consuming_block_hash"`
	ConsumingBlockHeight uint64    `json:"consuming_block_height"`
	Role                 InputRole `json:"role"`
	BodyOrdinal          uint32    `json:"body_ordinal"`
	IsConsumed           bool      `json:"is_consumed"`
}

func NewOutputUse(spend Spend) OutputUse {
	return OutputUse{
		ConsumingTx:          spend.ConsumingTx,
		ConsumingBlockHash:   spend.ConsumingBlockHash,
		ConsumingBlockHeight: spend.ConsumingBlockHeight,
		Role:                 spend.Role,
		BodyOrdinal:          spend.BodyOrdinal,
		IsConsumed:           spend.IsConsumed,
	}
}

type OutputState struct {
	Output        Output         `json:"output"`
	Uses          []OutputUse    `json:"uses"`
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
	Withdrawals         []Withdrawal          `json:"withdrawals"`
	Redeemers           []Redeemer            `json:"redeemers"`
	Metadata            *TransactionMetadata  `json:"metadata,omitempty"`
	Datums              []TransactionDatum    `json:"datums"`
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
	Labels      []uint64 `json:"labels"`
	MapCBOR     Bytes    `json:"map_cbor"`
	ByteLength  uint64   `json:"byte_length"`
	ContentHash Hash32   `json:"content_hash"`
}

type TransactionDatumObservation struct {
	SourceKind    string  `json:"source_kind"`
	SourceOrdinal uint32  `json:"source_ordinal"`
	OutputIndex   *uint32 `json:"output_index,omitempty"`
}

func (observation TransactionDatumObservation) Valid() bool {
	switch observation.SourceKind {
	case "inline_output":
		return observation.OutputIndex != nil
	case "witness":
		return observation.OutputIndex == nil
	default:
		return false
	}
}

// TransactionDatum groups every transaction-local observation of one datum
// hash around its one verified immutable body. Transaction identity and block
// activity are owned by the enclosing Transaction and are not repeated here.
type TransactionDatum struct {
	Hash         Hash32                        `json:"hash"`
	BodyCBOR     Bytes                         `json:"body_cbor"`
	BodyVerified bool                          `json:"body_verified"`
	Observations []TransactionDatumObservation `json:"observations"`
}

func (datum TransactionDatum) Valid() bool {
	if !datum.BodyVerified ||
		len(datum.BodyCBOR) == 0 ||
		len(datum.Observations) == 0 {
		return false
	}
	previousKind := ""
	var previousOrdinal uint32
	for index, observation := range datum.Observations {
		if !observation.Valid() {
			return false
		}
		if index > 0 &&
			(previousKind > observation.SourceKind ||
				(previousKind == observation.SourceKind &&
					previousOrdinal >= observation.SourceOrdinal)) {
			return false
		}
		previousKind = observation.SourceKind
		previousOrdinal = observation.SourceOrdinal
	}
	return true
}

func (transaction Transaction) DatumContextValid() bool {
	previous := ""
	for _, datum := range transaction.Datums {
		current := datum.Hash.String()
		if !datum.Valid() || (previous != "" && previous >= current) {
			return false
		}
		previous = current
	}
	return true
}

type FeeSink struct {
	Lovelace uint64 `json:"lovelace"`
}

type MintDelta struct {
	Asset    SignedAssetQuantity `json:"asset"`
	IsSource bool                `json:"is_source"`
	IsSink   bool                `json:"is_sink"`
}

// FlowTransaction is the compact transaction and Plutus context carried by a
// fund-flow edge. Flow inputs, outputs, and mint deltas are owned by the
// enclosing FlowHyperedge and are intentionally not repeated here.
type FlowTransaction struct {
	Hash                Hash32               `json:"hash"`
	BlockHash           Hash32               `json:"block_hash"`
	BlockHeight         uint64               `json:"block_height"`
	Order               uint32               `json:"order"`
	ParentHash          *Hash32              `json:"parent_hash,omitempty"`
	SubtransactionIndex *uint32              `json:"subtransaction_index,omitempty"`
	Era                 string               `json:"era"`
	Phase2Valid         bool                 `json:"phase2_valid"`
	FlowKind            string               `json:"flow_kind"`
	DeclaredFee         *uint64              `json:"declared_fee,omitempty"`
	EffectiveFee        *uint64              `json:"effective_fee,omitempty"`
	AppliedWithdrawals  []Withdrawal         `json:"applied_withdrawals"`
	Redeemers           []Redeemer           `json:"redeemers"`
	Metadata            *TransactionMetadata `json:"metadata,omitempty"`
	Datums              []TransactionDatum   `json:"datums"`
}

type FlowHyperedge struct {
	Transaction FlowTransaction `json:"transaction"`
	Inputs      []Spend         `json:"inputs"`
	Outputs     []Output        `json:"outputs"`
	FeeSink     *FeeSink        `json:"fee_sink,omitempty"`
	MintDeltas  []MintDelta     `json:"mint_deltas"`
}

func consumedInputs(inputs []Spend) []Spend {
	result := make([]Spend, 0, len(inputs))
	for _, input := range inputs {
		if input.IsConsumed {
			result = append(result, input)
		}
	}
	return result
}

// ConsumedInputs derives the effective input facts. Reference inputs and
// other non-consuming roles remain visible on Inputs but are not returned.
func (transaction Transaction) ConsumedInputs() []Spend {
	return consumedInputs(transaction.Inputs)
}

func (edge FlowHyperedge) ConsumedInputs() []Spend {
	return consumedInputs(edge.Inputs)
}

func (transaction Transaction) ConsumedInputRefs() []UTxORef {
	result := make([]UTxORef, 0, len(transaction.Inputs))
	for _, input := range transaction.Inputs {
		if input.IsConsumed {
			result = append(result, input.Source)
		}
	}
	return result
}

func (edge FlowHyperedge) ConsumedInputRefs() []UTxORef {
	result := make([]UTxORef, 0, len(edge.Inputs))
	for _, input := range edge.Inputs {
		if input.IsConsumed {
			result = append(result, input.Source)
		}
	}
	return result
}

// ConsumedInputValues derives the resolved values for effective inputs.
// Unresolved partial-history inputs remain explicit on Transaction.Inputs.
func (transaction Transaction) ConsumedInputValues() []Output {
	result := make([]Output, 0, len(transaction.Inputs))
	for _, input := range transaction.Inputs {
		if input.IsConsumed && input.SourceResolved && input.SourceOutput != nil {
			result = append(result, *input.SourceOutput)
		}
	}
	return result
}

func (edge FlowHyperedge) ConsumedInputValues() []Output {
	result := make([]Output, 0, len(edge.Inputs))
	for _, input := range edge.Inputs {
		if input.IsConsumed && input.SourceResolved && input.SourceOutput != nil {
			result = append(result, *input.SourceOutput)
		}
	}
	return result
}

func (transaction Transaction) ProducedOutputs() []Output {
	return append([]Output(nil), transaction.Outputs...)
}

func (transaction Transaction) AppliedWithdrawals() []Withdrawal {
	result := make([]Withdrawal, 0, len(transaction.Withdrawals))
	for _, withdrawal := range transaction.Withdrawals {
		if withdrawal.Applied {
			result = append(result, withdrawal)
		}
	}
	return result
}

func (transaction Transaction) FlowTransaction() FlowTransaction {
	return FlowTransaction{
		Hash:                transaction.Hash,
		BlockHash:           transaction.BlockHash,
		BlockHeight:         transaction.BlockHeight,
		Order:               transaction.Order,
		ParentHash:          transaction.ParentHash,
		SubtransactionIndex: transaction.SubtransactionIndex,
		Era:                 transaction.Era,
		Phase2Valid:         transaction.Phase2Valid,
		FlowKind:            transaction.FlowKind,
		DeclaredFee:         transaction.DeclaredFee,
		EffectiveFee:        transaction.EffectiveFee,
		AppliedWithdrawals:  transaction.AppliedWithdrawals(),
		Redeemers:           append([]Redeemer(nil), transaction.Redeemers...),
		Metadata:            transaction.Metadata,
		Datums:              append([]TransactionDatum(nil), transaction.Datums...),
	}
}

func (transaction Transaction) FeeSink() *FeeSink {
	if transaction.EffectiveFee == nil || *transaction.EffectiveFee == 0 {
		return nil
	}
	return &FeeSink{Lovelace: *transaction.EffectiveFee}
}

func (transaction Transaction) MintDeltas() []MintDelta {
	result := make([]MintDelta, 0, len(transaction.Mint))
	if !transaction.MintApplied {
		return result
	}
	for _, asset := range transaction.Mint {
		if asset.Quantity == 0 {
			continue
		}
		result = append(result, MintDelta{
			Asset:    asset,
			IsSource: asset.Quantity > 0,
			IsSink:   asset.Quantity < 0,
		})
	}
	return result
}

func NewFlowHyperedge(transaction Transaction) FlowHyperedge {
	return FlowHyperedge{
		Transaction: transaction.FlowTransaction(),
		Inputs:      append([]Spend(nil), transaction.Inputs...),
		Outputs:     transaction.ProducedOutputs(),
		FeeSink:     transaction.FeeSink(),
		MintDeltas:  transaction.MintDeltas(),
	}
}

type Trace struct {
	Direction  string          `json:"direction"`
	Asset      AssetSelector   `json:"asset"`
	Depth      uint32          `json:"depth"`
	Visited    uint32          `json:"visited"`
	Hyperedges []FlowHyperedge `json:"hyperedges"`
}
