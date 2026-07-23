package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"

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
	if err := validateObservationCheck(input.Check, input.Checkpoint, input.CheckpointBlockNumber); err != nil {
		return err
	}
	var err error
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
		Kind:               input.Kind,
		ProofMethod:        input.ProofMethod,
		PeerHost:           input.Peer.Host,
		PeerAddress:        input.Peer.Address,
		Operator:           input.Peer.Operator,
		N2NVersion:         input.N2NVersion,
		NetworkMagic:       observer.networkMagic,
		TipSlot:            input.Tip.Point.Slot,
		TipHash:            tipHash,
		TipBlockNumber:     input.Tip.BlockNumber,
		SelectedBodySource: input.SelectedBodySource,
		BodyHashVerified: input.Result == "agreed" &&
			input.ProofMethod == syncer.ObservationProofFollowBlockFetch,
		PointVerified: input.Result == "agreed" &&
			input.ProofMethod != syncer.ObservationProofNone,
		Result:     input.Result,
		Reason:     input.Reason,
		ObservedAt: input.ObservedAt.UTC(),
	}
	applyObservationCheck(&row, input.Check)
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
	if err := finalizeObservationIdentity(&row); err != nil {
		return err
	}
	return observer.sink.InsertPeerObservations(ctx, []model.PeerObservation{row})
}

func BoundaryObservations(
	bootstrap n2n.BoundaryBootstrap,
	networkMagic uint32,
	check syncer.CheckIdentity,
) ([]model.PeerObservation, error) {
	if networkMagic != n2n.MainnetNetworkMagic {
		return nil, fmt.Errorf("boundary observation network magic %d is not pinned mainnet", networkMagic)
	}
	checkpoint := bootstrap.ChainPoint
	if err := validateObservationCheck(
		check,
		checkpoint.Point,
		checkpoint.BlockNumber,
	); err != nil {
		return nil, err
	}
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
		proofMethod := syncer.ObservationProofChainSyncSingleton
		if evidence.Status == n2n.BoundaryAccepted {
			proofMethod = syncer.ObservationProofBoundarySingletonFetch
		}
		row := model.PeerObservation{
			Kind:                  "checkpoint",
			ProofMethod:           proofMethod,
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
		}
		applyObservationCheck(&row, check)
		if err := finalizeObservationIdentity(&row); err != nil {
			return nil, err
		}
		ret = append(ret, row)
	}
	return ret, nil
}

func validateObservationCheck(
	check syncer.CheckIdentity,
	checkpoint pcommon.Point,
	blockNumber uint64,
) error {
	if check.ID == ([16]byte{}) ||
		check.AgreementGroup == ([16]byte{}) ||
		check.Attempt == 0 ||
		check.Required < 2 {
		return errors.New("peer observation omitted check/group/attempt/threshold identity")
	}
	if check.CheckedPoint.BlockNumber != blockNumber ||
		check.CheckedPoint.Point.Slot != checkpoint.Slot ||
		!bytes.Equal(check.CheckedPoint.Point.Hash, checkpoint.Hash) {
		return errors.New("peer observation checkpoint differs from exact checked event-point")
	}
	return nil
}

func applyObservationCheck(
	row *model.PeerObservation,
	check syncer.CheckIdentity,
) {
	row.CheckID = check.ID
	row.AgreementGroup = check.AgreementGroup
	row.CheckAttempt = check.Attempt
	row.CorroborationRequired = check.Required
	row.CheckedEventSeq = check.CheckedEventSeq
	point := check.CheckedPoint
	if len(point.Point.Hash) == 0 {
		row.CheckedPointOrigin = true
		return
	}
	slot := point.Point.Slot
	hash := model.Hash32{}
	copy(hash[:], point.Point.Hash)
	number := point.BlockNumber
	row.CheckedPointSlot = &slot
	row.CheckedPointHash = &hash
	row.CheckedBlockNumber = &number
	row.CheckedPointIsByronEBB = point.IsByronEBB
}

func finalizeObservationIdentity(row *model.PeerObservation) error {
	return model.FinalizePeerObservationIdentity(row)
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

// ChainSync tips carry no wall-clock observation timestamp. Boundary rows are
// materialized immediately by the runtime, which overwrites this sentinel.
func timeFromTip(chainsync.Tip) time.Time {
	return time.Now().UTC()
}
