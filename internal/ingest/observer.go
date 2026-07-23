package ingest

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/blinklabs-io/gouroboros/protocol/chainsync"

	"clicksync/internal/model"
	"clicksync/internal/n2n"
	"clicksync/internal/syncer"
)

type ObservationSink interface {
	InsertPeerObservations(context.Context, []model.PeerObservation) error
}

type Observer struct {
	sink         ObservationSink
	networkMagic uint32
}

func NewObserver(sink ObservationSink, networkMagic uint32) (*Observer, error) {
	if sink == nil {
		return nil, errors.New("peer observation sink is required")
	}
	if networkMagic != n2n.MainnetNetworkMagic {
		return nil, fmt.Errorf("observer network magic %d is not pinned mainnet", networkMagic)
	}
	return &Observer{sink: sink, networkMagic: networkMagic}, nil
}

func (observer *Observer) Observe(
	ctx context.Context,
	input syncer.Observation,
) error {
	id, err := randomID()
	if err != nil {
		return err
	}
	var tipHash model.Hash32
	if len(input.Tip.Point.Hash) != 0 {
		tipHash, err = requiredHash32(input.Tip.Point.Hash, "observed tip")
		if err != nil {
			return err
		}
	}
	if input.Peer.Host == "" || input.Peer.Operator == "" {
		return errors.New("peer observation omitted host or operator")
	}
	if input.Result == "agreed" || input.Result == "disagreed" {
		if input.Peer.Address == "" ||
			input.N2NVersion == 0 ||
			tipHash == (model.Hash32{}) {
			return errors.New("available peer observation omitted actual address/version/tip")
		}
	}
	row := model.PeerObservation{
		ID:                 id,
		Kind:               input.Kind,
		PeerHost:           input.Peer.Host,
		PeerAddress:        input.Peer.Address,
		Operator:           input.Peer.Operator,
		N2NVersion:         input.N2NVersion,
		NetworkMagic:       observer.networkMagic,
		TipSlot:            input.Tip.Point.Slot,
		TipHash:            tipHash,
		TipBlockNumber:     input.Tip.BlockNumber,
		SelectedBodySource: input.SelectedBodySource,
		PointVerified:      input.Result == "agreed",
		Result:             input.Result,
		Reason:             input.Reason,
		ObservedAt:         input.ObservedAt.UTC(),
	}
	if len(input.Checkpoint.Hash) != 0 {
		checkpointHash, err := requiredHash32(input.Checkpoint.Hash, "checkpoint")
		if err != nil {
			return err
		}
		slot := input.Checkpoint.Slot
		number := input.CheckpointBlockNumber
		row.CheckpointSlot = &slot
		row.CheckpointHash = &checkpointHash
		row.CheckpointBlockNumber = &number
		if input.CheckpointIsByronEBB == nil {
			return errors.New("non-Origin checkpoint omitted Byron EBB classification")
		}
		isByronEBB := *input.CheckpointIsByronEBB
		row.CheckpointIsByronEBB = &isByronEBB
	} else if input.CheckpointIsByronEBB != nil {
		return errors.New("Origin checkpoint unexpectedly has Byron EBB classification")
	}
	return observer.sink.InsertPeerObservations(ctx, []model.PeerObservation{row})
}

func BoundaryObservations(
	bootstrap n2n.BoundaryBootstrap,
	networkMagic uint32,
) ([]model.PeerObservation, error) {
	if networkMagic != n2n.MainnetNetworkMagic {
		return nil, fmt.Errorf("boundary observation network magic %d is not pinned mainnet", networkMagic)
	}
	checkpoint := bootstrap.ChainPoint
	checkpointHash, err := requiredHash32(checkpoint.Point.Hash, "boundary checkpoint")
	if err != nil {
		return nil, err
	}
	ret := make([]model.PeerObservation, 0, len(bootstrap.Evidence))
	for _, evidence := range bootstrap.Evidence {
		var tip chainsync.Tip
		var observedTipHash model.Hash32
		if evidence.Peer.Tip != nil {
			tip = cloneTip(*evidence.Peer.Tip)
			observedTipHash, err = tipHash(tip)
			if err != nil {
				return nil, err
			}
		} else if evidence.Status == n2n.BoundaryAccepted {
			return nil, errors.New("accepted boundary evidence omitted peer tip")
		}
		id, err := randomID()
		if err != nil {
			return nil, err
		}
		slot := checkpoint.Point.Slot
		number := checkpoint.BlockNumber
		isByronEBB := checkpoint.IsByronEBB
		result := "unavailable"
		switch evidence.Status {
		case n2n.BoundaryAccepted:
			result = "agreed"
		case n2n.BoundaryPeerData:
			result = "quarantined"
		case n2n.BoundaryRejected:
			result = "disagreed"
		case n2n.BoundaryUnavailable:
		default:
			return nil, fmt.Errorf("unknown boundary evidence status %q", evidence.Status)
		}
		ret = append(ret, model.PeerObservation{
			ID:                    id,
			Kind:                  "checkpoint",
			PeerHost:              evidence.Peer.Host,
			PeerAddress:           evidence.Peer.Address,
			Operator:              evidence.Peer.Operator,
			N2NVersion:            evidence.Peer.N2NVersion,
			NetworkMagic:          networkMagic,
			TipSlot:               tip.Point.Slot,
			TipHash:               observedTipHash,
			TipBlockNumber:        tip.BlockNumber,
			CheckpointSlot:        &slot,
			CheckpointHash:        &checkpointHash,
			CheckpointBlockNumber: &number,
			CheckpointIsByronEBB:  &isByronEBB,
			SelectedBodySource: evidence.Peer.Host == bootstrap.Source.Peer.Host &&
				evidence.Status == n2n.BoundaryAccepted,
			BodyHashVerified: evidence.Status == n2n.BoundaryAccepted,
			PointVerified:    evidence.Status == n2n.BoundaryAccepted,
			ParentVerified:   false,
			Result:           result,
			Reason:           evidence.Failure,
			ObservedAt:       timeFromTip(tip),
		})
	}
	return ret, nil
}

func requiredHash32(value []byte, label string) (model.Hash32, error) {
	var ret model.Hash32
	if len(value) != len(ret) {
		return ret, fmt.Errorf("%s hash has %d bytes, want 32", label, len(value))
	}
	copy(ret[:], value)
	if ret == (model.Hash32{}) {
		return ret, fmt.Errorf("%s hash is zero", label)
	}
	return ret, nil
}

func tipHash(tip chainsync.Tip) (model.Hash32, error) {
	return requiredHash32(tip.Point.Hash, "observed tip")
}

func randomID() ([16]byte, error) {
	var ret [16]byte
	if _, err := rand.Read(ret[:]); err != nil {
		return ret, fmt.Errorf("generate observation ID: %w", err)
	}
	return ret, nil
}

// ChainSync tips carry no wall-clock observation timestamp. Boundary rows are
// materialized immediately by the runtime, which overwrites this sentinel.
func timeFromTip(chainsync.Tip) time.Time {
	return time.Now().UTC()
}
