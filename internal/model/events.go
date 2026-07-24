package model

import "time"

// Point identifies a chain position. Origin is explicit so an all-zero point
// is never confused with a real slot/hash.
type Point struct {
	Origin      bool
	Slot        uint64
	Hash        Hash32
	BlockNumber uint64
	IsByronEBB  bool
}

// RelayIdentity is the configured and negotiated identity retained for one
// relay. Agreed events preserve relay configuration order.
type RelayIdentity struct {
	Host         string
	Address      string
	Operator     string
	N2NVersion   uint16
	NetworkMagic uint32
}

type EventKind uint8

const (
	EventForward EventKind = iota + 1
	EventRollback
)

// AgreedEvent is emitted only after every configured relay supplied the same
// ordered event. RawCBOR is present only for forward events and is the one
// retained source copy; rollback events carry only Point and relay metadata.
type AgreedEvent struct {
	Kind        EventKind
	Point       Point
	BlockType   uint
	ContentHash Hash32
	RawLength   uint64
	RawCBOR     []byte
	Relays      []RelayIdentity
	ObservedAt  time.Time
}
