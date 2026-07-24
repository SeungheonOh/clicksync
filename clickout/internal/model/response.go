package model

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/clicksync-project/clickout/internal/metrics"
)

const (
	TrustPeerObserved = "peer_observed_structurally_verified"
	PeerDisclaimer    = "Data follows a corroborated remote-peer candidate chain; it is structurally verified, not independently consensus or ledger validated."
)

type Point struct {
	Origin      bool   `json:"origin"`
	Slot        uint64 `json:"slot"`
	Hash        Hash32 `json:"hash"`
	BlockNumber uint64 `json:"block_number"`
	IsByronEBB  bool   `json:"is_byron_ebb"`
}

func (point Point) Valid() bool {
	if point.Origin {
		return point.Slot == 0 &&
			point.Hash == (Hash32{}) &&
			point.BlockNumber == 0 &&
			!point.IsByronEBB
	}
	return point.Hash != (Hash32{})
}

type Head struct {
	EventSeq uint64 `json:"event_seq"`
	Point    Point  `json:"point"`
}

type Cutoff struct {
	AdoptionEventSeq uint64 `json:"adoption_event_seq"`
	PublicationID    uint64 `json:"publication_id"`
}

type SnapshotMode string

const (
	SnapshotAtTip   SnapshotMode = "tip"
	SnapshotAtBlock SnapshotMode = "block"
)

type SnapshotIdentity struct {
	DatasetID              DatasetID `json:"dataset_id"`
	SchemaContractHash     Hash32    `json:"schema_contract_hash"`
	NetworkMagic           uint32    `json:"network_magic"`
	NetworkName            string    `json:"network_name"`
	ByronGenesisID         Hash32    `json:"byron_genesis_id"`
	ByronGenesisJSONHash   Hash32    `json:"byron_genesis_json_hash"`
	ShelleyGenesisID       Hash32    `json:"shelley_genesis_id"`
	ShelleyGenesisJSONHash Hash32    `json:"shelley_genesis_json_hash"`
	Start                  Point     `json:"start"`
	TrustMode              string    `json:"trust_mode"`
	CreatedAt              time.Time `json:"created_at"`
	CompleteHistory        bool      `json:"complete_history"`
}

type SnapshotSelector struct {
	Mode                  SnapshotMode `json:"mode"`
	RequestedBlockHash    *Hash32      `json:"requested_block_hash,omitempty"`
	SelectedPublicationID uint64       `json:"selected_publication_id"`
	SelectedPoint         *Point       `json:"selected_point,omitempty"`
}

type SnapshotDiagnostics struct {
	Physical    Head   `json:"physical"`
	TrustStatus string `json:"trust_status"`
	TrustBasis  string `json:"trust_basis"`
	TrustReason string `json:"trust_reason,omitempty"`
}

type Snapshot struct {
	Identity             SnapshotIdentity    `json:"identity"`
	VisibilityGeneration uint64              `json:"visibility_generation"`
	AuthorityEffective   Head                `json:"authority_effective"`
	QueryHead            Head                `json:"query_head"`
	Cutoff               Cutoff              `json:"cutoff"`
	Selector             SnapshotSelector    `json:"selector"`
	Diagnostics          SnapshotDiagnostics `json:"diagnostics"`
}

func (snapshot Snapshot) Valid() bool {
	identity := snapshot.Identity
	if identity.DatasetID == (DatasetID{}) ||
		identity.SchemaContractHash == (Hash32{}) ||
		identity.NetworkMagic == 0 ||
		strings.TrimSpace(identity.NetworkName) == "" ||
		identity.ByronGenesisID == (Hash32{}) ||
		identity.ByronGenesisJSONHash == (Hash32{}) ||
		identity.ShelleyGenesisID == (Hash32{}) ||
		identity.ShelleyGenesisJSONHash == (Hash32{}) ||
		!identity.Start.Valid() ||
		identity.TrustMode != TrustPeerObserved ||
		identity.CreatedAt.IsZero() ||
		identity.CompleteHistory != identity.Start.Origin ||
		!snapshot.AuthorityEffective.Point.Valid() ||
		!snapshot.QueryHead.Point.Valid() ||
		!snapshot.Diagnostics.Physical.Point.Valid() ||
		snapshot.AuthorityEffective.EventSeq > snapshot.Diagnostics.Physical.EventSeq ||
		(snapshot.Cutoff.AdoptionEventSeq == 0) !=
			(snapshot.Cutoff.PublicationID == 0) ||
		snapshot.Cutoff.AdoptionEventSeq > snapshot.QueryHead.EventSeq {
		return false
	}
	switch snapshot.Diagnostics.TrustStatus {
	case "agreed", "unavailable", "checking", "disputed":
	default:
		return false
	}
	switch snapshot.Diagnostics.TrustBasis {
	case "official_genesis", "sampled_peer", "partial_boundary", "primary_only":
	default:
		return false
	}
	if snapshot.Diagnostics.Physical.EventSeq == 0 &&
		snapshot.Diagnostics.Physical.Point != identity.Start {
		return false
	}
	if snapshot.Diagnostics.Physical.EventSeq ==
		snapshot.AuthorityEffective.EventSeq &&
		snapshot.Diagnostics.Physical != snapshot.AuthorityEffective {
		return false
	}
	if identity.CompleteHistory &&
		snapshot.Diagnostics.Physical.EventSeq == 0 {
		return false
	}
	if snapshot.AuthorityEffective.EventSeq == 0 &&
		snapshot.AuthorityEffective.Point != identity.Start {
		return false
	}
	if snapshot.QueryHead.EventSeq == 0 &&
		snapshot.QueryHead.Point != identity.Start {
		return false
	}
	if identity.CompleteHistory &&
		(snapshot.AuthorityEffective.EventSeq == 0 ||
			snapshot.QueryHead.EventSeq == 0) {
		return false
	}
	switch snapshot.Selector.Mode {
	case SnapshotAtTip:
		return snapshot.QueryHead == snapshot.AuthorityEffective &&
			snapshot.Selector.RequestedBlockHash == nil &&
			snapshot.Selector.SelectedPublicationID == 0 &&
			snapshot.Selector.SelectedPoint == nil
	case SnapshotAtBlock:
		return snapshot.Selector.RequestedBlockHash != nil &&
			*snapshot.Selector.RequestedBlockHash != (Hash32{}) &&
			snapshot.Selector.SelectedPublicationID != 0 &&
			snapshot.Selector.SelectedPoint != nil &&
			snapshot.Selector.SelectedPoint.Valid() &&
			snapshot.QueryHead.EventSeq != 0 &&
			snapshot.QueryHead.EventSeq <= snapshot.AuthorityEffective.EventSeq &&
			snapshot.Cutoff == (Cutoff{
				AdoptionEventSeq: snapshot.QueryHead.EventSeq,
				PublicationID:    snapshot.Selector.SelectedPublicationID,
			}) &&
			*snapshot.Selector.SelectedPoint == snapshot.QueryHead.Point
	default:
		return false
	}
}

func (snapshot Snapshot) SamePin(other Snapshot) bool {
	return sameSnapshotIdentity(snapshot.Identity, other.Identity) &&
		snapshot.VisibilityGeneration == other.VisibilityGeneration &&
		snapshot.AuthorityEffective == other.AuthorityEffective &&
		snapshot.QueryHead == other.QueryHead &&
		snapshot.Cutoff == other.Cutoff &&
		sameSnapshotSelector(snapshot.Selector, other.Selector)
}

func sameSnapshotIdentity(left, right SnapshotIdentity) bool {
	return left.DatasetID == right.DatasetID &&
		left.SchemaContractHash == right.SchemaContractHash &&
		left.NetworkMagic == right.NetworkMagic &&
		left.NetworkName == right.NetworkName &&
		left.ByronGenesisID == right.ByronGenesisID &&
		left.ByronGenesisJSONHash == right.ByronGenesisJSONHash &&
		left.ShelleyGenesisID == right.ShelleyGenesisID &&
		left.ShelleyGenesisJSONHash == right.ShelleyGenesisJSONHash &&
		left.Start == right.Start &&
		left.TrustMode == right.TrustMode &&
		left.CreatedAt.Equal(right.CreatedAt) &&
		left.CompleteHistory == right.CompleteHistory
}

func sameSnapshotSelector(left, right SnapshotSelector) bool {
	if left.Mode != right.Mode ||
		left.SelectedPublicationID != right.SelectedPublicationID ||
		(left.RequestedBlockHash == nil) !=
			(right.RequestedBlockHash == nil) ||
		(left.SelectedPoint == nil) != (right.SelectedPoint == nil) {
		return false
	}
	if left.RequestedBlockHash != nil &&
		*left.RequestedBlockHash != *right.RequestedBlockHash {
		return false
	}
	return left.SelectedPoint == nil ||
		*left.SelectedPoint == *right.SelectedPoint
}

type TruncationReason string

const (
	TruncationAddressSeedLimit TruncationReason = "address_seed_limit"
	TruncationAddressPageLimit TruncationReason = "address_page_limit"
	TruncationMaxNodes         TruncationReason = "max_nodes"
	TruncationMaxEdges         TruncationReason = "max_edges"
	TruncationLayerTimeout     TruncationReason = "layer_timeout"
	TruncationMaxDepth         TruncationReason = "max_depth"
)

func (reason TruncationReason) Valid() bool {
	switch reason {
	case TruncationAddressSeedLimit,
		TruncationAddressPageLimit,
		TruncationMaxNodes,
		TruncationMaxEdges,
		TruncationLayerTimeout,
		TruncationMaxDepth:
		return true
	default:
		return false
	}
}

type Truncation struct {
	Truncated            bool             `json:"truncated"`
	Reason               TruncationReason `json:"reason,omitempty"`
	ContinuationCursor   string           `json:"continuation_cursor,omitempty"`
	ContinuationFrontier []UTxORef        `json:"continuation_frontier"`
	LosslessResume       bool             `json:"lossless_resume"`
}

func (truncation Truncation) Valid() bool {
	if !truncation.Truncated {
		return truncation.Reason == "" &&
			truncation.ContinuationCursor == "" &&
			len(truncation.ContinuationFrontier) == 0 &&
			!truncation.LosslessResume
	}
	if !truncation.Reason.Valid() {
		return false
	}
	switch truncation.Reason {
	case TruncationAddressSeedLimit:
		return truncation.ContinuationCursor != "" &&
			len(truncation.ContinuationFrontier) == 0 &&
			!truncation.LosslessResume
	case TruncationAddressPageLimit:
		return truncation.ContinuationCursor == "" &&
			len(truncation.ContinuationFrontier) == 0 &&
			truncation.LosslessResume
	case TruncationMaxNodes,
		TruncationMaxEdges,
		TruncationLayerTimeout,
		TruncationMaxDepth:
		return truncation.ContinuationCursor == "" &&
			len(truncation.ContinuationFrontier) > 0 &&
			!truncation.LosslessResume
	default:
		return false
	}
}

func (truncation Truncation) MarshalJSON() ([]byte, error) {
	if !truncation.Valid() {
		return nil, errors.New("invalid truncation reason/state")
	}
	type plain Truncation
	return json.Marshal(plain(truncation))
}

func (truncation *Truncation) UnmarshalJSON(data []byte) error {
	type plain Truncation
	var decoded plain
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if !Truncation(decoded).Valid() {
		return errors.New("invalid truncation reason/state")
	}
	*truncation = Truncation(decoded)
	return nil
}

type PartialHistoryBoundary struct {
	UTxO   UTxORef `json:"utxo"`
	Reason string  `json:"reason"`
}

type Response[T any] struct {
	Snapshot                 Snapshot                 `json:"snapshot"`
	Truncation               Truncation               `json:"truncation"`
	UnresolvedPartialHistory []PartialHistoryBoundary `json:"unresolved_partial_history"`
	PeerObservedDisclaimer   string                   `json:"peer_observed_disclaimer"`
	ExcludedNonUTxODeltas    []string                 `json:"excluded_non_utxo_deltas"`
	QueryMetrics             []metrics.Query          `json:"query_metrics"`
	Data                     T                        `json:"data"`
}

func NewResponse[T any](snapshot Snapshot, data T) Response[T] {
	return Response[T]{
		Snapshot: snapshot,
		Truncation: Truncation{
			ContinuationFrontier: make([]UTxORef, 0),
			LosslessResume:       false,
		},
		UnresolvedPartialHistory: make([]PartialHistoryBoundary, 0),
		PeerObservedDisclaimer:   PeerDisclaimer,
		ExcludedNonUTxODeltas: []string{
			"certificate deposits and refunds",
			"treasury and reserve movements",
			"reward-account balance history beyond observed applied withdrawals",
		},
		QueryMetrics: make([]metrics.Query, 0),
		Data:         data,
	}
}
