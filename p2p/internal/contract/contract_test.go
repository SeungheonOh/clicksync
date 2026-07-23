package contract

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestEmitterRequiresAckBeforeWindowAdvances(t *testing.T) {
	var out bytes.Buffer
	acksR, acksW := io.Pipe()
	emitter, err := NewEmitter(&out, "session", 764824073, "peer:3001", 15, 1)
	if err != nil {
		t.Fatal(err)
	}
	emitter.StartAckReader(acksR)
	if _, err := emitter.Emit(context.Background(), "ready", false, nil); err != nil {
		t.Fatal(err)
	}
	seq, err := emitter.Emit(context.Background(), "roll_forward", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := emitter.Emit(context.Background(), "heartbeat", true, nil)
		result <- err
	}()
	select {
	case err := <-result:
		t.Fatalf("window did not block: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	ack := Ack{SchemaVersion: SchemaVersion, Kind: "ack", SessionID: "session", SourceSeq: seq}
	if err := json.NewEncoder(acksW).Encode(ack); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("emit remained blocked after acknowledgement")
	}
}

func TestEmitterFailsClosedOnMalformedAck(t *testing.T) {
	var out bytes.Buffer
	emitter, err := NewEmitter(&out, "session", 1, "peer:3001", 15, 1)
	if err != nil {
		t.Fatal(err)
	}
	emitter.StartAckReader(strings.NewReader(`{"schema_version":1,"kind":"ack","session_id":"other","source_seq":1}` + "\n"))
	select {
	case <-emitter.Failure():
	case <-time.After(time.Second):
		t.Fatal("ack reader did not fail")
	}
	if err := emitter.Err(); err == nil || !strings.Contains(err.Error(), "session ID mismatch") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEmitterFailsClosedWhenParentPipeCloses(t *testing.T) {
	var out bytes.Buffer
	emitter, err := NewEmitter(&out, "session", 1, "peer:3001", 15, 1)
	if err != nil {
		t.Fatal(err)
	}
	emitter.StartAckReader(strings.NewReader(""))
	select {
	case <-emitter.Failure():
	case <-time.After(time.Second):
		t.Fatal("ack reader did not observe EOF")
	}
	if !errors.Is(emitter.Err(), ErrParentClosed) {
		t.Fatalf("unexpected error: %v", emitter.Err())
	}
}

func TestEmitterWritesProtocolOnlyNDJSON(t *testing.T) {
	var out bytes.Buffer
	emitter, err := NewEmitter(&out, "session", 1, "peer:3001", 15, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := emitter.Emit(context.Background(), "ready", false, nil); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("got %d lines", len(lines))
	}
	var env Envelope
	if err := json.Unmarshal([]byte(lines[0]), &env); err != nil {
		t.Fatal(err)
	}
	if env.SchemaVersion != SchemaVersion || env.SourceSeq != 1 || env.Kind != "ready" {
		t.Fatalf("unexpected envelope: %#v", env)
	}
}

func TestEmitterBlockedWaitHonorsContextCancellation(t *testing.T) {
	var out bytes.Buffer
	acksR, _ := io.Pipe()
	emitter, err := NewEmitter(&out, "session", 1, "peer:3001", 15, 1)
	if err != nil {
		t.Fatal(err)
	}
	emitter.StartAckReader(acksR)
	if _, err := emitter.Emit(context.Background(), "roll_forward", true, nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := emitter.Emit(ctx, "heartbeat", true, nil)
		result <- err
	}()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Emit did not observe context cancellation")
	}
	if err := emitter.Drain(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Drain did not observe context cancellation: %v", err)
	}
}

func TestEmitterDoesNotCommitSequenceBeforeSuccessfulEvent(t *testing.T) {
	var out bytes.Buffer
	emitter, err := NewEmitter(&out, "session", 1, "peer:3001", 15, 1)
	if err != nil {
		t.Fatal(err)
	}
	want := errors.New("populate failed")
	if _, err := emitter.Emit(context.Background(), "ready", false, func(*Envelope) error {
		return want
	}); !errors.Is(err, want) {
		t.Fatalf("unexpected error: %v", err)
	}
	seq, err := emitter.Emit(context.Background(), "ready", false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 1 {
		t.Fatalf("failed event consumed a source sequence: got %d", seq)
	}
}
