// Package contract defines the deliberately small, versioned process protocol
// between the direct-N2N source helper and the TypeScript commit coordinator.
package contract

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
)

const (
	SchemaVersion  = 1
	MaxAckLineSize = 64 * 1024
	MaxEventSize   = 16 * 1024 * 1024
)

var ErrParentClosed = errors.New("parent acknowledgement pipe closed")

type Point struct {
	Origin bool   `json:"origin"`
	Slot   uint64 `json:"slot,omitempty"`
	Hash   string `json:"hash,omitempty"`
}

type Tip struct {
	Point       Point  `json:"point"`
	BlockNumber uint64 `json:"block_number"`
}

type Verification struct {
	BodyHash        bool `json:"body_hash"`
	Point           bool `json:"point"`
	Parent          bool `json:"parent"`
	BlockNumber     bool `json:"block_number"`
	SlotProgression bool `json:"slot_progression"`
}

type Envelope struct {
	SchemaVersion   int             `json:"schema_version"`
	Kind            string          `json:"kind"`
	SessionID       string          `json:"session_id"`
	SourceSeq       uint64          `json:"source_seq"`
	NetworkMagic    uint32          `json:"network_magic"`
	Peer            string          `json:"peer"`
	N2NVersion      uint16          `json:"n2n_version"`
	Trust           string          `json:"trust"`
	CompleteHistory bool            `json:"complete_history"`
	Point           *Point          `json:"point,omitempty"`
	ParentPoint     *Point          `json:"parent_point,omitempty"`
	Tip             *Tip            `json:"tip,omitempty"`
	Verification    *Verification   `json:"verification,omitempty"`
	Payload         json.RawMessage `json:"payload,omitempty"`
	Error           *FatalError     `json:"error,omitempty"`
}

type FatalError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type Ack struct {
	SchemaVersion int    `json:"schema_version"`
	Kind          string `json:"kind"`
	SessionID     string `json:"session_id"`
	SourceSeq     uint64 `json:"source_seq"`
}

// Emitter serializes events and enforces a bounded cumulative-ack window.
// It is safe for one event producer and one acknowledgement reader.
type Emitter struct {
	out          io.Writer
	sessionID    string
	networkMagic uint32
	peer         string
	n2nVersion   uint16
	window       int

	mu         sync.Mutex
	notify     chan struct{}
	sourceSeq  uint64
	lastAck    uint64
	pending    []uint64
	readerErr  error
	readerDone chan struct{}
}

func NewEmitter(
	out io.Writer,
	sessionID string,
	networkMagic uint32,
	peer string,
	n2nVersion uint16,
	window int,
) (*Emitter, error) {
	if out == nil {
		return nil, errors.New("nil protocol output")
	}
	if sessionID == "" {
		return nil, errors.New("empty session ID")
	}
	if networkMagic == 0 {
		return nil, errors.New("network magic must be non-zero")
	}
	if peer == "" {
		return nil, errors.New("empty selected peer")
	}
	if n2nVersion < 7 || n2nVersion > 15 {
		return nil, fmt.Errorf("unsupported negotiated N2N version %d", n2nVersion)
	}
	if window < 1 || window > 8 {
		return nil, fmt.Errorf("ack window must be between 1 and 8, got %d", window)
	}
	e := &Emitter{
		out:          out,
		sessionID:    sessionID,
		networkMagic: networkMagic,
		peer:         peer,
		n2nVersion:   n2nVersion,
		window:       window,
		notify:       make(chan struct{}),
		readerDone:   make(chan struct{}),
	}
	return e, nil
}

func (e *Emitter) StartAckReader(input io.Reader) {
	go func() {
		defer close(e.readerDone)
		if input == nil {
			e.fail(errors.New("nil acknowledgement input"))
			return
		}
		scanner := bufio.NewScanner(input)
		scanner.Buffer(make([]byte, 4096), MaxAckLineSize)
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(bytes.TrimSpace(line)) == 0 {
				e.fail(errors.New("empty acknowledgement line"))
				return
			}
			var ack Ack
			dec := json.NewDecoder(bytes.NewReader(line))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&ack); err != nil {
				e.fail(fmt.Errorf("decode acknowledgement: %w", err))
				return
			}
			if err := requireJSONEOF(dec); err != nil {
				e.fail(fmt.Errorf("decode acknowledgement: %w", err))
				return
			}
			if err := e.acceptAck(ack); err != nil {
				e.fail(err)
				return
			}
		}
		if err := scanner.Err(); err != nil {
			e.fail(fmt.Errorf("read acknowledgement: %w", err))
		} else {
			e.fail(ErrParentClosed)
		}
	}()
}

func (e *Emitter) Emit(
	ctx context.Context,
	kind string,
	acked bool,
	populate func(*Envelope) error,
) (uint64, error) {
	if !validKind(kind) {
		return 0, fmt.Errorf("invalid event kind %q", kind)
	}
	for {
		e.mu.Lock()
		if err := context.Cause(ctx); err != nil {
			e.mu.Unlock()
			return 0, err
		}
		if e.readerErr != nil {
			err := e.readerErr
			e.mu.Unlock()
			return 0, err
		}
		if !acked || len(e.pending) < e.window {
			break
		}
		notify := e.notify
		e.mu.Unlock()
		select {
		case <-ctx.Done():
			return 0, context.Cause(ctx)
		case <-notify:
		}
	}
	defer e.mu.Unlock()
	nextSeq := e.sourceSeq + 1
	env := Envelope{
		SchemaVersion:   SchemaVersion,
		Kind:            kind,
		SessionID:       e.sessionID,
		SourceSeq:       nextSeq,
		NetworkMagic:    e.networkMagic,
		Peer:            e.peer,
		N2NVersion:      e.n2nVersion,
		Trust:           "peer_observed_structurally_verified",
		CompleteHistory: false,
	}
	if populate != nil {
		if err := populate(&env); err != nil {
			return 0, err
		}
	}
	data, err := json.Marshal(env)
	if err != nil {
		return 0, fmt.Errorf("encode %s envelope: %w", kind, err)
	}
	if len(data)+1 > MaxEventSize {
		return 0, fmt.Errorf("%s envelope is %d bytes, limit is %d", kind, len(data)+1, MaxEventSize)
	}
	if _, err := e.out.Write(append(data, '\n')); err != nil {
		return 0, fmt.Errorf("write %s envelope: %w", kind, err)
	}
	e.sourceSeq = nextSeq
	if acked {
		e.pending = append(e.pending, e.sourceSeq)
	}
	return e.sourceSeq, nil
}

func (e *Emitter) Drain(ctx context.Context) error {
	for {
		e.mu.Lock()
		if err := context.Cause(ctx); err != nil {
			e.mu.Unlock()
			return err
		}
		if e.readerErr != nil {
			err := e.readerErr
			e.mu.Unlock()
			return err
		}
		if len(e.pending) == 0 {
			e.mu.Unlock()
			return nil
		}
		notify := e.notify
		e.mu.Unlock()
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-notify:
		}
	}
}

func (e *Emitter) Failure() <-chan struct{} {
	return e.readerDone
}

func (e *Emitter) Err() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.readerErr
}

func (e *Emitter) acceptAck(ack Ack) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	switch {
	case ack.SchemaVersion != SchemaVersion:
		return fmt.Errorf("unsupported acknowledgement schema version %d", ack.SchemaVersion)
	case ack.Kind != "ack":
		return fmt.Errorf("unexpected parent message kind %q", ack.Kind)
	case ack.SessionID != e.sessionID:
		return errors.New("acknowledgement session ID mismatch")
	case ack.SourceSeq <= e.lastAck:
		return fmt.Errorf("regressive or duplicate acknowledgement %d", ack.SourceSeq)
	case ack.SourceSeq > e.sourceSeq:
		return fmt.Errorf("acknowledgement %d is ahead of emitted sequence %d", ack.SourceSeq, e.sourceSeq)
	}
	idx := -1
	for i, seq := range e.pending {
		if seq == ack.SourceSeq {
			idx = i
			break
		}
	}
	if idx < 0 {
		return fmt.Errorf("acknowledgement %d does not identify an ack-required event", ack.SourceSeq)
	}
	e.pending = append(e.pending[:0], e.pending[idx+1:]...)
	e.lastAck = ack.SourceSeq
	e.signalLocked()
	return nil
}

func (e *Emitter) fail(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.readerErr == nil {
		e.readerErr = err
		e.signalLocked()
	}
}

func (e *Emitter) signalLocked() {
	close(e.notify)
	e.notify = make(chan struct{})
}

func validKind(kind string) bool {
	switch kind {
	case "ready", "roll_forward", "roll_backward", "heartbeat", "fatal":
		return true
	default:
		return false
	}
}

func requireJSONEOF(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}
