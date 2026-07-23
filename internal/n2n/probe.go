package n2n

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type PointProbe struct {
	Accepted bool
	Peer     Peer
	Tip      chainsync.Tip
}

// ProbePeer opens a fresh N2N connection and asks ChainSync about exactly one
// point. Opening a fresh connection deliberately repeats DNS resolution and
// handshake/version negotiation for every corroboration attempt.
func ProbePeer(
	ctx context.Context,
	peer Peer,
	cfg DialConfig,
	point pcommon.Point,
	logger *slog.Logger,
) (PointProbe, error) {
	if logger == nil {
		return PointProbe{}, errors.New("nil logger")
	}
	cfg.Operator = peer.Operator
	conn, asyncErr, err := dial(
		peer.Host,
		cfg,
		func(chainsync.CallbackContext, uint, any, chainsync.Tip) error {
			return errors.New("unexpected application roll-forward during singleton probe")
		},
		func(chainsync.CallbackContext, pcommon.Point, chainsync.Tip) error {
			return errors.New("unexpected application rollback during singleton probe")
		},
		nil,
		nil,
		logger,
	)
	if err != nil {
		return PointProbe{}, err
	}
	defer conn.Close()
	probePeer := peer
	version, _ := conn.ProtocolVersion()
	probePeer.N2NVersion = version
	if remote := conn.Id().RemoteAddr; remote != nil {
		probePeer.Address = remote.String()
	}

	closed := make(chan struct{})
	defer close(closed)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-closed:
		}
	}()

	_, _, rangeErr := conn.ChainSync().Client.GetAvailableBlockRange(
		[]pcommon.Point{point},
	)
	accepted := rangeErr == nil
	if rangeErr != nil && !errors.Is(rangeErr, chainsync.ErrIntersectNotFound) {
		return PointProbe{}, fmt.Errorf("probe exact ChainSync point: %w", rangeErr)
	}
	tip, err := conn.ChainSync().Client.GetCurrentTip()
	if err != nil {
		return PointProbe{}, fmt.Errorf("read probe connection tip: %w", err)
	}
	if tip == nil {
		return PointProbe{}, errors.New("probe connection returned nil tip")
	}
	probePeer.Tip = tip
	select {
	case async, ok := <-asyncErr:
		if ok && async != nil {
			return PointProbe{}, fmt.Errorf("peer protocol during probe: %w", async)
		}
	default:
	}
	return PointProbe{
		Accepted: accepted,
		Peer:     probePeer,
		Tip:      *tip,
	}, nil
}
