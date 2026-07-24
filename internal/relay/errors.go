package relay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"

	"github.com/blinklabs-io/gouroboros/protocol"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

type FailureKind string

const (
	FailureCanceled     FailureKind = "canceled"
	FailureTimeout      FailureKind = "timeout"
	FailureDNS          FailureKind = "dns"
	FailureConnection   FailureKind = "connection"
	FailureIntersection FailureKind = "intersection"
	FailureProtocol     FailureKind = "protocol"
	FailureBound        FailureKind = "bound"
	FailureConfig       FailureKind = "config"
)

// Error gives the restart loop a stable classification without matching error
// strings from gOuroboros or the standard library.
type Error struct {
	Kind      FailureKind
	Operation string
	Relay     string
	Err       error
}

func (e *Error) Error() string {
	if e == nil {
		return "relay failure"
	}
	if e.Relay == "" {
		return fmt.Sprintf("relay %s: %v", e.Operation, e.Err)
	}
	return fmt.Sprintf("relay %s %s: %v", e.Relay, e.Operation, e.Err)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func FailureOf(err error) FailureKind {
	var failure *Error
	if errors.As(err, &failure) {
		return failure.Kind
	}
	return classifyFailure(err)
}

func wrapFailure(operation, relay string, err error) error {
	if err == nil {
		return nil
	}
	var failure *Error
	if errors.As(err, &failure) {
		return err
	}
	return &Error{
		Kind:      classifyFailure(err),
		Operation: operation,
		Relay:     relay,
		Err:       err,
	}
}

func protocolFailure(operation, relay string, err error) error {
	if err == nil {
		err = errors.New("invalid protocol callback order")
	}
	return &Error{
		Kind:      FailureProtocol,
		Operation: operation,
		Relay:     relay,
		Err:       err,
	}
}

func classifyFailure(err error) FailureKind {
	switch {
	case errors.Is(err, context.Canceled):
		return FailureCanceled
	case errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, os.ErrDeadlineExceeded),
		errors.Is(err, syscall.ETIMEDOUT):
		return FailureTimeout
	case errors.Is(err, chainsync.ErrIntersectNotFound):
		return FailureIntersection
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		if dnsErr.IsTimeout {
			return FailureTimeout
		}
		return FailureDNS
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return FailureTimeout
	}
	if errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, protocol.ErrProtocolShuttingDown) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EHOSTUNREACH) ||
		errors.Is(err, syscall.ENETUNREACH) {
		return FailureConnection
	}
	return FailureProtocol
}
