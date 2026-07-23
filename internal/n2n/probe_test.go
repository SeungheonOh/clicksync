package n2n

import (
	"errors"
	"reflect"
	"testing"

	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type rollbackProbeClientFake struct {
	accepted map[string]bool
	calls    []pcommon.Point
	tip      *chainsync.Tip
}

func (fake *rollbackProbeClientFake) GetAvailableBlockRange(
	points []pcommon.Point,
) (pcommon.Point, pcommon.Point, error) {
	if len(points) != 1 {
		return pcommon.Point{}, pcommon.Point{}, errors.New(
			"membership probe was not a singleton",
		)
	}
	point := pcommon.NewPoint(points[0].Slot, points[0].Hash)
	fake.calls = append(fake.calls, point)
	if !fake.accepted[probePointKey(point)] {
		return pcommon.Point{}, pcommon.Point{}, chainsync.ErrIntersectNotFound
	}
	return point, point, nil
}

func (fake *rollbackProbeClientFake) GetCurrentTip() (*chainsync.Tip, error) {
	return fake.tip, nil
}

func TestRollbackMembershipsUseOneClientInTargetThenBranchOrder(t *testing.T) {
	target := testPoint(8, 0x08)
	branch := testPoint(12, 0x12)
	tip := chainsync.Tip{Point: testPoint(13, 0x13), BlockNumber: 13}
	fake := &rollbackProbeClientFake{
		accepted: map[string]bool{
			probePointKey(target): true,
			probePointKey(branch): true,
		},
		tip: &tip,
	}
	protocolChecks := 0
	targetAccepted, branchAccepted, gotTip, err :=
		probeRollbackMemberships(
			fake,
			target,
			branch,
			func() error {
				protocolChecks++
				return nil
			},
		)
	if err != nil {
		t.Fatal(err)
	}
	if !targetAccepted || !branchAccepted ||
		gotTip.BlockNumber != tip.BlockNumber ||
		protocolChecks != 3 ||
		!reflect.DeepEqual(fake.calls, []pcommon.Point{target, branch}) {
		t.Fatalf(
			"target=%t branch=%t tip=%#v checks=%d calls=%#v",
			targetAccepted,
			branchAccepted,
			gotTip,
			protocolChecks,
			fake.calls,
		)
	}
}

func TestRollbackMembershipsPreserveCommonTipUnrelatedTargetResult(t *testing.T) {
	target := testPoint(8, 0x08)
	branch := testPoint(12, 0x12)
	tip := chainsync.Tip{Point: testPoint(13, 0x13), BlockNumber: 13}
	fake := &rollbackProbeClientFake{
		accepted: map[string]bool{
			probePointKey(branch): true,
		},
		tip: &tip,
	}
	targetAccepted, branchAccepted, _, err := probeRollbackMemberships(
		fake,
		target,
		branch,
		func() error { return nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	if targetAccepted || !branchAccepted ||
		!reflect.DeepEqual(fake.calls, []pcommon.Point{target, branch}) {
		t.Fatalf(
			"target=%t branch=%t calls=%#v",
			targetAccepted,
			branchAccepted,
			fake.calls,
		)
	}
}

func TestProbeSessionClosedAsyncChannelIsTypedTermination(t *testing.T) {
	asyncErr := make(chan error)
	close(asyncErr)
	session := &probeSession{asyncErr: asyncErr}
	err := session.protocolError()
	var closed *ProtocolChannelClosed
	if !errors.As(err, &closed) {
		t.Fatalf("error = %T %v, want ProtocolChannelClosed", err, err)
	}
}

func TestRollbackMembershipsDoNotReturnSuccessAfterAsyncProtocolError(t *testing.T) {
	target := testPoint(8, 0x08)
	branch := testPoint(12, 0x12)
	tip := chainsync.Tip{Point: testPoint(13, 0x13), BlockNumber: 13}
	fake := &rollbackProbeClientFake{
		accepted: map[string]bool{
			probePointKey(target): true,
			probePointKey(branch): true,
		},
		tip: &tip,
	}
	want := errors.New("asynchronous protocol failure")
	checks := 0
	_, _, _, err := probeRollbackMemberships(
		fake,
		target,
		branch,
		func() error {
			checks++
			if checks == 2 {
				return want
			}
			return nil
		},
	)
	if !errors.Is(err, want) || checks != 2 {
		t.Fatalf("checks=%d error=%v", checks, err)
	}
}

func probePointKey(point pcommon.Point) string {
	return string(point.Hash)
}
