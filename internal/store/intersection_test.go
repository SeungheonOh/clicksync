package store

import (
	"testing"

	"clicksync/internal/model"
	"clicksync/internal/n2n"
	"clicksync/internal/publication"

	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

func TestChainPointFromPublicationPreservesHeightAndByronEBB(t *testing.T) {
	point := publication.Point{
		Slot:        42,
		Hash:        model.Hash32{0x41},
		BlockNumber: 7,
		IsByronEBB:  true,
	}
	converted := chainPointFromPublication(point)
	if converted.Point.Slot != point.Slot ||
		string(converted.Point.Hash) != string(point.Hash[:]) ||
		converted.BlockNumber != point.BlockNumber ||
		!converted.IsByronEBB {
		t.Fatalf("converted chain point = %+v", converted)
	}
}

func TestAppendTerminalIntersectionPreservesSameSlotByronEBBFallback(t *testing.T) {
	successorHash := model.Hash32{0x61}
	boundaryHash := model.Hash32{0x62}
	successor := n2n.NewChainPoint(
		pcommon.NewPoint(100, successorHash[:]),
		10,
	)
	boundary := publication.Point{
		Slot:        100,
		Hash:        boundaryHash,
		BlockNumber: 9,
		IsByronEBB:  true,
	}
	got, err := appendTerminalIntersection([]n2n.ChainPoint{successor}, boundary)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 ||
		got[0].BlockNumber != 10 || got[0].IsByronEBB ||
		got[1].BlockNumber != 9 || !got[1].IsByronEBB {
		t.Fatalf("same-slot candidates = %+v", got)
	}

	invalid := boundary
	invalid.IsByronEBB = false
	if _, err := appendTerminalIntersection([]n2n.ChainPoint{successor}, invalid); err == nil {
		t.Fatal("equal-slot non-EBB boundary was accepted")
	}
}

func TestTerminalIntersectionPreservesBoundaryAndOrigin(t *testing.T) {
	boundary := publication.Point{
		Slot:        99,
		Hash:        model.Hash32{0x51},
		BlockNumber: 9,
		IsByronEBB:  true,
	}
	partial := terminalIntersection(boundary)
	if len(partial) != 1 ||
		partial[0].BlockNumber != boundary.BlockNumber ||
		!partial[0].IsByronEBB {
		t.Fatalf("partial terminal = %+v", partial)
	}
	origin := terminalIntersection(publication.Point{Origin: true})
	if len(origin) != 1 ||
		origin[0].Point.Slot != 0 ||
		len(origin[0].Point.Hash) != 0 ||
		origin[0].BlockNumber != 0 ||
		origin[0].IsByronEBB {
		t.Fatalf("Origin terminal = %+v", origin)
	}
}
