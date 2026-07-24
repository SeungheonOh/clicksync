package relay

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"log/slog"
	"time"

	"cardano-clicksync/internal/model"
)

const RawBlockDomain = "cardano-clicksync/raw-block/v1"

type EventKind uint8

const (
	Forward EventKind = iota + 1
	Rollback
)

// Event is one relay-local observation. Agreement is deliberately a separate
// package: a relay session never decides whether another relay is correct.
type Event struct {
	Kind       EventKind
	Point      model.Point
	Tip        model.Point
	BlockType  uint
	RawLength  uint64
	Digest     model.Hash32
	RawCBOR    []byte
	Relay      model.RelayIdentity
	ObservedAt time.Time
}

// Reader is the minimal boundary consumed by the agreement package.
type Reader interface {
	Next(context.Context) (Event, error)
}

type Config struct {
	RelayIndex            int
	Host                  string
	Operator              string
	NetworkMagic          uint32
	BlockFetchRangeBlocks int
	BlockFetchQueueSize   int
	RelayQueueSize        int
	RawQueueBytes         int64
	DialTimeout           time.Duration
	BlockTimeout          time.Duration
}

// RawBlockDigest hashes the exact bytes supplied by BlockFetch. Point and
// event kind are intentionally compared separately by agreement.
func RawBlockDigest(blockType uint, raw []byte) model.Hash32 {
	hash := sha256.New()
	_, _ = hash.Write([]byte(RawBlockDomain))
	var frame [16]byte
	binary.BigEndian.PutUint64(frame[:8], uint64(blockType))
	binary.BigEndian.PutUint64(frame[8:], uint64(len(raw)))
	_, _ = hash.Write(frame[:])
	_, _ = hash.Write(raw)
	var ret model.Hash32
	copy(ret[:], hash.Sum(nil))
	return ret
}

func New(config Config, logger *slog.Logger) (*Session, error) {
	if err := validateConfig(config, logger); err != nil {
		return nil, err
	}
	identity := model.RelayIdentity{
		Host:         config.Host,
		Address:      config.Host,
		Operator:     config.Operator,
		NetworkMagic: config.NetworkMagic,
	}
	return &Session{
		config:      config,
		logger:      logger,
		identity:    identity,
		chainEvents: make(chan chainEvent, chainSyncMaxOutstanding),
		fetchJobs:   make(chan fetchJob, 1),
		events:      make(chan Event, config.RelayQueueSize),
		ready:       make(chan struct{}),
		done:        make(chan struct{}),
		rawWake:     make(chan struct{}, 1),
	}, nil
}
