package n2n

import (
	"errors"
	"reflect"
	"testing"

	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type fakeChainSync struct {
	accepted   map[string]bool
	probeErr   map[string]error
	rangeStart pcommon.Point
	probes     []pcommon.Point
	syncPoint  []pcommon.Point
	actions    *[]string
}

func (f *fakeChainSync) GetAvailableBlockRange(points []pcommon.Point) (pcommon.Point, pcommon.Point, error) {
	if len(points) != 1 {
		return pcommon.Point{}, pcommon.Point{}, errors.New("probe was not singleton")
	}
	f.probes = append(f.probes, points[0])
	if err := f.probeErr[pointKey(points[0])]; err != nil {
		return pcommon.Point{}, pcommon.Point{}, err
	}
	if f.accepted[pointKey(points[0])] {
		return f.rangeStart, pcommon.Point{}, nil
	}
	return pcommon.Point{}, pcommon.Point{}, chainsync.ErrIntersectNotFound
}

func TestAcceptedTipSelectsInputAndProbePublishesNothing(t *testing.T) {
	tip := testPoint(40, 0x40)
	firstAfter := testPoint(41, 0x41)
	client := &fakeChainSync{
		accepted:   map[string]bool{pointKey(tip): true},
		rangeStart: firstAfter,
	}
	got, err := discoverIntersection(client, []pcommon.Point{tip})
	if err != nil {
		t.Fatal(err)
	}
	if pointKey(got) != pointKey(tip) {
		t.Fatalf("selected returned range start instead of input: %#v", got)
	}
	if len(client.syncPoint) != 0 {
		t.Fatalf("probe started publication sync: %#v", client.syncPoint)
	}
}

func TestIntersectionProbePropagatesNonMissError(t *testing.T) {
	point := testPoint(20, 0x20)
	want := errors.New("transport failed")
	client := &fakeChainSync{probeErr: map[string]error{pointKey(point): want}}
	if _, err := discoverIntersection(client, []pcommon.Point{point}); !errors.Is(err, want) {
		t.Fatalf("error = %v", err)
	}
}

func (f *fakeChainSync) Sync(points []pcommon.Point) error {
	if f.actions != nil {
		*f.actions = append(*f.actions, "sync")
	}
	f.syncPoint = append([]pcommon.Point(nil), points...)
	return nil
}

func TestDiscoverIntersectionNewestToHistoryThenOrigin(t *testing.T) {
	newest := testPoint(30, 0x30)
	historical := testPoint(20, 0x20)
	older := testPoint(10, 0x10)
	client := &fakeChainSync{accepted: map[string]bool{pointKey(older): true}}
	got, err := discoverIntersection(client, []pcommon.Point{
		newest,
		historical,
		older,
		pcommon.NewPointOrigin(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if pointKey(got) != pointKey(older) {
		t.Fatalf("chosen = %s", pointKey(got))
	}
	want := []pcommon.Point{newest, historical, older}
	if !reflect.DeepEqual(client.probes, want) {
		t.Fatalf("probe order = %#v, want %#v", client.probes, want)
	}
}

func TestOriginIsAlwaysLastAndCannotBeMistakenForBlockAvailability(t *testing.T) {
	branchPoint := testPoint(99, 0x99)
	origin := pcommon.NewPointOrigin()
	// The fake reports the branch point as not intersecting even though a
	// separate BlockFetch service might retain it. discoverIntersection only
	// consumes ChainSync FindIntersect semantics.
	client := &fakeChainSync{accepted: map[string]bool{pointKey(origin): true}}
	got, err := discoverIntersection(client, []pcommon.Point{origin, branchPoint})
	if err != nil {
		t.Fatal(err)
	}
	if !isOrigin(got) {
		t.Fatalf("chosen = %#v", got)
	}
	if len(client.probes) != 2 || pointKey(client.probes[0]) != pointKey(branchPoint) || !isOrigin(client.probes[1]) {
		t.Fatalf("probe order = %#v", client.probes)
	}
}

func TestReconcileCommitsBeforeExactPointIsPassedToSync(t *testing.T) {
	selected := testPoint(20, 0x20)
	var actions []string
	client := &fakeChainSync{
		accepted: map[string]bool{pointKey(selected): true},
		actions:  &actions,
	}
	reconciled := pcommon.Point{}
	got, err := reconcileAndSync(
		client,
		[]pcommon.Point{testPoint(30, 0x30), selected},
		func(point pcommon.Point) error {
			actions = append(actions, "rollback")
			reconciled = point
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actions, []string{"rollback", "sync"}) {
		t.Fatalf("actions = %v", actions)
	}
	if pointKey(reconciled) != pointKey(selected) ||
		len(client.syncPoint) != 1 ||
		pointKey(client.syncPoint[0]) != pointKey(selected) ||
		pointKey(got) != pointKey(selected) {
		t.Fatalf("reconciled=%#v sync=%#v got=%#v", reconciled, client.syncPoint, got)
	}
}

func testPoint(slot uint64, fill byte) pcommon.Point {
	hash := make([]byte, 32)
	for index := range hash {
		hash[index] = fill
	}
	return pcommon.NewPoint(slot, hash)
}

func pointKey(point pcommon.Point) string {
	if isOrigin(point) {
		return "origin"
	}
	return string(append([]byte{byte(point.Slot)}, point.Hash...))
}
