package syncer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"syscall"

	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"

	"clicksync/internal/n2n"
)

type DirectTransport struct {
	config            n2n.DialConfig
	logger            *slog.Logger
	runPeer           runPeerFunc
	probePeer         probePeerFunc
	probeRollbackPeer probeRollbackPeerFunc
}

type runPeerFunc func(
	context.Context,
	string,
	n2n.DialConfig,
	[]n2n.ChainPoint,
	n2n.Handler,
	*slog.Logger,
) error

type probePeerFunc func(
	context.Context,
	n2n.Peer,
	n2n.DialConfig,
	pcommon.Point,
	*slog.Logger,
) (n2n.PointProbe, error)

type probeRollbackPeerFunc func(
	context.Context,
	n2n.Peer,
	n2n.DialConfig,
	pcommon.Point,
	pcommon.Point,
	*slog.Logger,
) (n2n.RollbackPointProbe, error)

func NewDirectTransport(config n2n.DialConfig, logger *slog.Logger) (*DirectTransport, error) {
	if logger == nil {
		return nil, errors.New("nil direct N2N transport logger")
	}
	return &DirectTransport{
		config:            config,
		logger:            logger,
		runPeer:           n2n.RunPeer,
		probePeer:         n2n.ProbePeer,
		probeRollbackPeer: n2n.ProbeRollbackPeer,
	}, nil
}

func (t *DirectTransport) Probe(
	ctx context.Context,
	peer n2n.Peer,
	point pcommon.Point,
) (ProbeResult, error) {
	result, err := t.probePeer(ctx, peer, t.config, point, t.logger)
	if err != nil {
		return ProbeResult{}, classifyDirectError(err)
	}
	return ProbeResult{
		Accepted:   result.Accepted,
		Tip:        cloneTip(result.Tip),
		N2NVersion: result.Peer.N2NVersion,
		Address:    result.Peer.Address,
	}, nil
}

func (t *DirectTransport) ProbeRollback(
	ctx context.Context,
	peer n2n.Peer,
	target pcommon.Point,
	branch pcommon.Point,
) (RollbackProbeResult, error) {
	result, err := t.probeRollbackPeer(
		ctx,
		peer,
		t.config,
		target,
		branch,
		t.logger,
	)
	if err != nil {
		return RollbackProbeResult{}, classifyDirectError(err)
	}
	return RollbackProbeResult{
		TargetAccepted: result.TargetAccepted,
		BranchAccepted: result.BranchAccepted,
		Tip:            cloneTip(result.Tip),
		N2NVersion:     result.Peer.N2NVersion,
		Address:        result.Peer.Address,
	}, nil
}

func (t *DirectTransport) Follow(
	ctx context.Context,
	peer n2n.Peer,
	candidates []n2n.ChainPoint,
	handler n2n.Handler,
) error {
	config := t.config
	config.Operator = peer.Operator
	err := t.runPeer(ctx, peer.Host, config, candidates, handler, t.logger)
	if err == nil ||
		ctx.Err() != nil {
		return err
	}
	if withTerminal, ok := handler.(interface{ terminalFailure() error }); ok {
		if terminal := withTerminal.terminalFailure(); terminal != nil {
			return terminal
		}
	}
	return classifyDirectError(err)
}

func (h *attemptHandler) terminalFailure() error {
	return h.terminalErr
}

func classifyDirectError(err error) error {
	var peerData *n2n.PeerDataViolation
	var rangeUnavailable *n2n.RangeUnavailable
	if errors.As(err, &peerData) || errors.As(err, &rangeUnavailable) {
		return err
	}
	var protocolClosed *n2n.ProtocolChannelClosed
	if errors.As(err, &protocolClosed) {
		return RetryableTransportError(err)
	}
	if isRetryableNetworkError(err) {
		return RetryableTransportError(err)
	}
	return err
}

func isRetryableNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, os.ErrDeadlineExceeded) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EHOSTUNREACH) ||
		errors.Is(err, syscall.ENETUNREACH) ||
		errors.Is(err, syscall.ETIMEDOUT) {
		return true
	}
	var dnsError *net.DNSError
	if errors.As(err, &dnsError) {
		return true
	}
	var networkError net.Error
	return errors.As(err, &networkError)
}
