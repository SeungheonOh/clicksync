package n2n

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestParsePoint(t *testing.T) {
	point, err := ParsePoint("42:" + strings.Repeat("ab", 32))
	if err != nil {
		t.Fatal(err)
	}
	if point.Slot != 42 || len(point.Hash) != 32 {
		t.Fatalf("point = %#v", point)
	}
	origin, err := ParsePoint("origin")
	if err != nil || !isOrigin(origin) {
		t.Fatalf("origin = %#v, %v", origin, err)
	}
}

func TestParsePointRejectsMalformed(t *testing.T) {
	for _, value := range []string{"", "1", "x:" + strings.Repeat("00", 32), "1:00"} {
		if _, err := ParsePoint(value); err == nil {
			t.Fatalf("accepted %q", value)
		}
	}
}

func TestProtocolChannelClosurePrioritizesRacingHandlerFailure(t *testing.T) {
	runCtx, cancel := context.WithCancelCause(context.Background())
	handlerErr := errors.New("ClickHouse publication failed")
	workerErr := make(chan error, 1)
	workerErr <- handlerErr
	if err := protocolChannelClosure(runCtx, cancel, workerErr); !errors.Is(err, handlerErr) {
		t.Fatalf("error = %v, want handler failure", err)
	}
}

func TestProtocolChannelClosureIsTypedTransportTermination(t *testing.T) {
	runCtx, cancel := context.WithCancelCause(context.Background())
	workerErr := make(chan error, 1)
	go func() {
		<-runCtx.Done()
		workerErr <- context.Cause(runCtx)
	}()
	err := protocolChannelClosure(runCtx, cancel, workerErr)
	var closed *ProtocolChannelClosed
	if !errors.As(err, &closed) {
		t.Fatalf("error = %T %v, want ProtocolChannelClosed", err, err)
	}
}
