package n2n

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"

	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// PeerDataViolation identifies structurally invalid or impossible data from a
// specific N2N source. Callers must quarantine the source operator rather than
// treating this as a generic reconnectable network failure.
type PeerDataViolation struct {
	Kind  string
	Point pcommon.Point
	Err   error
}

func (e *PeerDataViolation) Error() string {
	if e == nil {
		return "peer-data violation"
	}
	if e.Point.Slot == 0 && len(e.Point.Hash) == 0 {
		return fmt.Sprintf("peer-data violation (%s): %v", e.Kind, e.Err)
	}
	return fmt.Sprintf(
		"peer-data violation (%s) at %d:%x: %v",
		e.Kind,
		e.Point.Slot,
		e.Point.Hash,
		e.Err,
	)
}

func (e *PeerDataViolation) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// RangeUnavailable is a bounded ChainSync/BlockFetch race. The supervisor may
// reconnect and re-intersect once for the exact source/range; repetition is a
// peer-data violation and quarantines that operator.
type RangeUnavailable struct {
	Start pcommon.Point
	End   pcommon.Point
	Err   error
}

// ProtocolChannelClosed means gOuroboros ended its asynchronous error stream
// without a more specific validation or handler failure. It is a typed
// transport termination, not peer-data evidence.
type ProtocolChannelClosed struct{}

func (*ProtocolChannelClosed) Error() string {
	return "peer protocol error channel closed"
}

func (e *RangeUnavailable) Error() string {
	if e == nil {
		return "BlockFetch range unavailable"
	}
	return fmt.Sprintf(
		"BlockFetch range unavailable %d:%x..%d:%x: %v",
		e.Start.Slot,
		e.Start.Hash,
		e.End.Slot,
		e.End.Hash,
		e.Err,
	)
}

func (e *RangeUnavailable) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func peerDataViolation(
	kind string,
	point pcommon.Point,
	err error,
) error {
	return &PeerDataViolation{Kind: kind, Point: point, Err: err}
}

// isNetworkFailure uses only concrete standard-library error types and
// sentinels. It deliberately does not classify failures by message text.
func isNetworkFailure(err error) bool {
	if errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.ErrClosedPipe) ||
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
