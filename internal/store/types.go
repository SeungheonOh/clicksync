package store

import (
	"errors"
	"time"

	"github.com/google/uuid"

	"cardano-clicksync/internal/model"
)

var (
	// ErrNotCommitted means exact readback proved that the final authority row
	// is absent. Retrying is safe and must use newly allocated identifiers.
	ErrNotCommitted = errors.New("store commit is absent")

	// ErrCommitConflict means readback found partial, duplicate, unexpected, or
	// conflicting authority rows. The writer stops rather than guessing.
	ErrCommitConflict = errors.New("store commit conflicts with the expected rows")

	// ErrCommitIndeterminate means both the write result and its exact
	// readback failed, so the caller cannot safely infer visibility.
	ErrCommitIndeterminate = errors.New("store commit state is indeterminate")

	ErrInvalidRollback = errors.New("invalid rollback")
)

type Lock interface {
	AssertHeld() error
}

type Point struct {
	Origin      bool
	Slot        uint64
	Hash        model.Hash32
	BlockNumber uint64
	IsByronEBB  bool
}

func PointFromModel(point model.Point) Point {
	return Point{
		Origin:      point.Origin,
		Slot:        point.Slot,
		Hash:        point.Hash,
		BlockNumber: point.BlockNumber,
		IsByronEBB:  point.IsByronEBB,
	}
}

func (p Point) Model() model.Point {
	return model.Point{
		Origin:      p.Origin,
		Slot:        p.Slot,
		Hash:        p.Hash,
		BlockNumber: p.BlockNumber,
		IsByronEBB:  p.IsByronEBB,
	}
}

type Relay struct {
	Host       string
	Address    string
	Operator   string
	N2NVersion uint16
}

type DatasetConfig struct {
	NetworkMagic uint32
	NetworkName  string
	Start        Point
	SourceBuild  string
}

type DatasetIdentity struct {
	DatasetID    uuid.UUID
	SchemaHash   model.Hash32
	NetworkMagic uint32
	NetworkName  string
	Start        Point
	CreatedAt    time.Time
	SourceBuild  string
}

type Candidate struct {
	Block       model.Block
	ContentHash model.Hash32
	Relays      []Relay
	RawLength   uint64
}

type CanonicalBlock struct {
	PublicationID uint64
	EventSeq      uint64
	Point         Point
}

type State struct {
	Dataset       DatasetIdentity
	Snapshot      uint64
	Tip           Point
	Canonical     []CanonicalBlock
	Intersections []Point
}

type Commit struct {
	FirstPublicationID uint64
	LastPublicationID  uint64
	FirstEventSeq      uint64
	LastEventSeq       uint64
	Blocks             []CanonicalBlock
	Committed          bool
	ResolvedUncertain  bool
}

type RollbackRequest struct {
	To           Point
	Relays       []Relay
	Reason       string
	MaximumDepth uint32
}

type RollbackCommit struct {
	RollbackID        uuid.UUID
	EventSeq          uint64
	To                Point
	OldTip            Point
	OldTipEventSeq    uint64
	Descendants       []CanonicalBlock
	Committed         bool
	Noop              bool
	ResolvedUncertain bool
}

type publication struct {
	PublicationID uint64
	EventSeq      uint64
	Block         model.Block
	Counts        factCounts
	ContentHash   model.Hash32
	Relays        []Relay
	WriterID      uuid.UUID
	InsertedAt    time.Time
}

type rollback struct {
	RollbackID     uuid.UUID
	EventSeq       uint64
	To             Point
	OldTip         Point
	OldTipEventSeq uint64
	Descendants    []CanonicalBlock
	Relays         []Relay
	Reason         string
	WriterID       uuid.UUID
	RecordedAt     time.Time
}
