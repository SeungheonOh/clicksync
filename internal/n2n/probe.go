package n2n

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type PointProbe struct {
	Accepted bool
	Peer     Peer
	Tip      chainsync.Tip
}

type RollbackPointProbe struct {
	TargetAccepted bool
	BranchAccepted bool
	Peer           Peer
	Tip            chainsync.Tip
}

type probeSession struct {
	conn     *ouroboros.Connection
	asyncErr <-chan error
	peer     Peer
	closed   chan struct{}
}

type rollbackMembershipClient interface {
	GetAvailableBlockRange(
		[]pcommon.Point,
	) (pcommon.Point, pcommon.Point, error)
	GetCurrentTip() (*chainsync.Tip, error)
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
	session, err := openProbeSession(ctx, peer, cfg, logger)
	if err != nil {
		return PointProbe{}, err
	}
	defer session.close()
	accepted, err := session.probe(point)
	if err != nil {
		return PointProbe{}, fmt.Errorf("probe exact ChainSync point: %w", err)
	}
	tip, err := session.currentTip()
	if err != nil {
		return PointProbe{}, err
	}
	if err := session.protocolError(); err != nil {
		return PointProbe{}, err
	}
	return PointProbe{
		Accepted: accepted,
		Peer:     session.peer,
		Tip:      tip,
	}, nil
}

// ProbeRollbackPeer proves two exact memberships on one freshly negotiated
// ChainSync session. This prevents DNS or load-balancing from satisfying the
// target proof on one backend and the branch-tip proof on another.
func ProbeRollbackPeer(
	ctx context.Context,
	peer Peer,
	cfg DialConfig,
	target pcommon.Point,
	branch pcommon.Point,
	logger *slog.Logger,
) (RollbackPointProbe, error) {
	session, err := openProbeSession(ctx, peer, cfg, logger)
	if err != nil {
		return RollbackPointProbe{}, err
	}
	defer session.close()
	targetAccepted, branchAccepted, tip, err := probeRollbackMemberships(
		session.conn.ChainSync().Client,
		target,
		branch,
		session.protocolError,
	)
	if err != nil {
		return RollbackPointProbe{}, err
	}
	session.peer.Tip = &tip
	return RollbackPointProbe{
		TargetAccepted: targetAccepted,
		BranchAccepted: branchAccepted,
		Peer:           session.peer,
		Tip:            tip,
	}, nil
}

func probeRollbackMemberships(
	client rollbackMembershipClient,
	target pcommon.Point,
	branch pcommon.Point,
	checkProtocol func() error,
) (bool, bool, chainsync.Tip, error) {
	if client == nil || checkProtocol == nil {
		return false, false, chainsync.Tip{}, errors.New(
			"rollback membership client and protocol check are required",
		)
	}
	targetAccepted, err := probeMembership(client, target)
	if err != nil {
		return false, false, chainsync.Tip{}, fmt.Errorf(
			"probe exact rollback target: %w",
			err,
		)
	}
	if err := checkProtocol(); err != nil {
		return false, false, chainsync.Tip{}, err
	}
	branchAccepted, err := probeMembership(client, branch)
	if err != nil {
		return false, false, chainsync.Tip{}, fmt.Errorf(
			"probe exact rollback branch tip: %w",
			err,
		)
	}
	if err := checkProtocol(); err != nil {
		return false, false, chainsync.Tip{}, err
	}
	tip, err := client.GetCurrentTip()
	if err != nil {
		return false, false, chainsync.Tip{}, fmt.Errorf(
			"read probe connection tip: %w",
			err,
		)
	}
	if tip == nil {
		return false, false, chainsync.Tip{}, errors.New(
			"probe connection returned nil tip",
		)
	}
	if err := checkProtocol(); err != nil {
		return false, false, chainsync.Tip{}, err
	}
	return targetAccepted, branchAccepted, *tip, nil
}

func openProbeSession(
	ctx context.Context,
	peer Peer,
	cfg DialConfig,
	logger *slog.Logger,
) (*probeSession, error) {
	if logger == nil {
		return nil, errors.New("nil logger")
	}
	cfg.Operator = peer.Operator
	conn, asyncErr, err := dial(
		peer.Host,
		cfg,
		func(chainsync.CallbackContext, uint, any, chainsync.Tip) error {
			return errors.New(
				"unexpected application roll-forward during singleton probe",
			)
		},
		func(chainsync.CallbackContext, pcommon.Point, chainsync.Tip) error {
			return errors.New(
				"unexpected application rollback during singleton probe",
			)
		},
		nil,
		nil,
		logger,
	)
	if err != nil {
		return nil, err
	}
	actualPeer := peer
	version, _ := conn.ProtocolVersion()
	actualPeer.N2NVersion = version
	if remote := conn.Id().RemoteAddr; remote != nil {
		actualPeer.Address = remote.String()
	}
	session := &probeSession{
		conn:     conn,
		asyncErr: asyncErr,
		peer:     actualPeer,
		closed:   make(chan struct{}),
	}
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-session.closed:
		}
	}()
	return session, nil
}

func (session *probeSession) close() {
	close(session.closed)
	_ = session.conn.Close()
}

func (session *probeSession) probe(point pcommon.Point) (bool, error) {
	return probeMembership(session.conn.ChainSync().Client, point)
}

func probeMembership(
	client rollbackMembershipClient,
	point pcommon.Point,
) (bool, error) {
	_, _, err := client.GetAvailableBlockRange([]pcommon.Point{point})
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, chainsync.ErrIntersectNotFound):
		return false, nil
	default:
		return false, err
	}
}

func (session *probeSession) currentTip() (chainsync.Tip, error) {
	tip, err := session.conn.ChainSync().Client.GetCurrentTip()
	if err != nil {
		return chainsync.Tip{}, fmt.Errorf(
			"read probe connection tip: %w",
			err,
		)
	}
	if tip == nil {
		return chainsync.Tip{}, errors.New(
			"probe connection returned nil tip",
		)
	}
	session.peer.Tip = tip
	return *tip, nil
}

func (session *probeSession) protocolError() error {
	select {
	case async, ok := <-session.asyncErr:
		if !ok {
			return &ProtocolChannelClosed{}
		}
		if async != nil {
			return fmt.Errorf("peer protocol during probe: %w", async)
		}
	default:
	}
	return nil
}
