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

type reconcileFunc func(pcommon.Point) error

func discoverIntersection(
	client chainSyncClient,
	candidates []pcommon.Point,
) (pcommon.Point, error) {
	ordered, err := normalizeCandidates(candidates)
	if err != nil {
		return pcommon.Point{}, err
	}
	for _, candidate := range ordered {
		_, _, err := client.GetAvailableBlockRange([]pcommon.Point{candidate})
		switch {
		case err == nil:
			return candidate, nil
		case errors.Is(err, chainsync.ErrIntersectNotFound):
			continue
		default:
			return pcommon.Point{}, fmt.Errorf(
				"probe ChainSync intersection at slot %d: %w",
				candidate.Slot,
				err,
			)
		}
	}
	return pcommon.Point{}, errors.New("Origin ChainSync intersection was not accepted")
}

func reconcileAndSync(
	client chainSyncClient,
	candidates []pcommon.Point,
	reconcile reconcileFunc,
) (pcommon.Point, error) {
	if reconcile == nil {
		return pcommon.Point{}, errors.New("nil committed-chain reconciler")
	}
	chosen, err := discoverIntersection(client, candidates)
	if err != nil {
		return pcommon.Point{}, err
	}
	// Reconciliation must commit any required local rollback before Sync can
	// deliver and publish a new roll-forward.
	if err := reconcile(chosen); err != nil {
		return pcommon.Point{}, fmt.Errorf("reconcile selected intersection: %w", err)
	}
	if err := client.Sync([]pcommon.Point{chosen}); err != nil {
		return pcommon.Point{}, fmt.Errorf("start ChainSync at selected intersection: %w", err)
	}
	return chosen, nil
}

func normalizeCandidates(candidates []pcommon.Point) ([]pcommon.Point, error) {
	var ret []pcommon.Point
	seen := make(map[string]struct{}, len(candidates)+1)
	origin := pcommon.NewPointOrigin()
	for _, point := range candidates {
		if isOrigin(point) {
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
		ret = append(ret, point)
	}
	// The caller supplies newest-to-historical candidates. Origin is always
	// forced to the final position and cannot be omitted.
	return append(ret, origin), nil
}

func isOrigin(point pcommon.Point) bool {
	return point.Slot == 0 && len(point.Hash) == 0
}
