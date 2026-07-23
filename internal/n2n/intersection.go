package n2n

import (
	"errors"
	"fmt"

	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// chainSyncClient is deliberately expressed in ChainSync terms. In
// gOuroboros v0.189.1 GetAvailableBlockRange first calls the private
// requestFindIntersect and returns ErrIntersectNotFound when the peer answers
// MsgIntersectNotFound. Calling it with one point therefore proves membership
// in the peer's currently selected ChainSync chain; BlockFetch availability is
// never used as an intersection test.
type chainSyncClient interface {
	GetAvailableBlockRange([]pcommon.Point) (pcommon.Point, pcommon.Point, error)
	Sync([]pcommon.Point) error
}

// ChainPoint is the only intersection-candidate representation. Every
// non-Origin point carries the decoded/stored block number that the next
// ChainSync header must extend.
type ChainPoint struct {
	Point       pcommon.Point
	BlockNumber uint64
	IsByronEBB  bool
}

func NewChainPoint(point pcommon.Point, blockNumber uint64) ChainPoint {
	return ChainPoint{
		Point:       pcommon.NewPoint(point.Slot, append([]byte(nil), point.Hash...)),
		BlockNumber: blockNumber,
	}
}

func NewByronEBBChainPoint(point pcommon.Point, blockNumber uint64) ChainPoint {
	ret := NewChainPoint(point, blockNumber)
	ret.IsByronEBB = true
	return ret
}

func NewChainPointOrigin() ChainPoint {
	return ChainPoint{Point: pcommon.NewPointOrigin()}
}

type reconcileFunc func(ChainPoint) error

func discoverIntersection(
	client chainSyncClient,
	candidates []ChainPoint,
) (ChainPoint, error) {
	ordered, err := normalizeCandidates(candidates)
	if err != nil {
		return ChainPoint{}, err
	}
	for _, candidate := range ordered {
		_, _, err := client.GetAvailableBlockRange([]pcommon.Point{candidate.Point})
		switch {
		case err == nil:
			return candidate, nil
		case errors.Is(err, chainsync.ErrIntersectNotFound):
			continue
		default:
			return ChainPoint{}, fmt.Errorf(
				"probe ChainSync intersection at slot %d: %w",
				candidate.Point.Slot,
				err,
			)
		}
	}
	return ChainPoint{}, errors.New("no supplied ChainSync intersection was accepted")
}

func reconcileAndSync(
	client chainSyncClient,
	candidates []ChainPoint,
	reconcile reconcileFunc,
) (ChainPoint, error) {
	if reconcile == nil {
		return ChainPoint{}, errors.New("nil committed-chain reconciler")
	}
	chosen, err := discoverIntersection(client, candidates)
	if err != nil {
		return ChainPoint{}, err
	}
	// Reconciliation must commit any required local rollback before Sync can
	// deliver and publish a new roll-forward.
	if err := reconcile(chosen); err != nil {
		return ChainPoint{}, fmt.Errorf("reconcile selected intersection: %w", err)
	}
	if err := client.Sync([]pcommon.Point{chosen.Point}); err != nil {
		return ChainPoint{}, fmt.Errorf("start ChainSync at selected intersection: %w", err)
	}
	return chosen, nil
}

func normalizeCandidates(candidates []ChainPoint) ([]ChainPoint, error) {
	var ret []ChainPoint
	seen := make(map[string]struct{}, len(candidates)+1)
	for index, candidate := range candidates {
		point := candidate.Point
		if isOrigin(point) {
			if index != len(candidates)-1 {
				return nil, errors.New("Origin intersection candidate must be last")
			}
			if candidate.BlockNumber != 0 {
				return nil, errors.New("Origin intersection cannot carry a block number")
			}
			if candidate.IsByronEBB {
				return nil, errors.New("Origin intersection cannot be a Byron EBB")
			}
			ret = append(ret, NewChainPointOrigin())
			continue
		}
		if len(point.Hash) != 32 {
			return nil, fmt.Errorf("intersection candidate at slot %d has %d-byte hash", point.Slot, len(point.Hash))
		}
		key := fmt.Sprintf("%d:%x", point.Slot, point.Hash)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		if candidate.IsByronEBB {
			ret = append(ret, NewByronEBBChainPoint(point, candidate.BlockNumber))
		} else {
			ret = append(ret, NewChainPoint(point, candidate.BlockNumber))
		}
	}
	// Origin is never widened into the candidate set here. The supervisor
	// supplies it explicitly only for a complete Origin-start dataset.
	return ret, nil
}

func isOrigin(point pcommon.Point) bool {
	return point.Slot == 0 && len(point.Hash) == 0
}
