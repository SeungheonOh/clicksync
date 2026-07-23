package n2n

import (
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
