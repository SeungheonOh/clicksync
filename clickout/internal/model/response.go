package model

import "github.com/clicksync-project/clickout/internal/metrics"

const (
	TrustPeerObserved = "peer_observed_structurally_verified"
	PeerDisclaimer    = "Data follows a corroborated remote-peer candidate chain; it is structurally verified, not independently consensus or ledger validated."
)

type Snapshot struct {
	Event                uint64 `json:"event"`
	PublicationWatermark uint64 `json:"publication_watermark"`
	CompleteHistory      bool   `json:"complete_history"`
	TrustMode            string `json:"trust_mode"`
}

func (snapshot Snapshot) Valid() bool {
	if snapshot.TrustMode != TrustPeerObserved {
		return false
	}
	if snapshot.Event == 0 && snapshot.PublicationWatermark != 0 {
		return false
	}
	// Origin before the first committed adoption/rollback is event zero.
	return snapshot.Event != 0 || !snapshot.CompleteHistory
}

type Truncation struct {
	Truncated            bool      `json:"truncated"`
	Reason               string    `json:"reason,omitempty"`
	ContinuationCursor   string    `json:"continuation_cursor,omitempty"`
	ContinuationFrontier []UTxORef `json:"continuation_frontier"`
	LosslessResume       bool      `json:"lossless_resume"`
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
