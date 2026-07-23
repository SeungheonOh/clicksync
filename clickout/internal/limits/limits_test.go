package limits

import "testing"

func TestPlanDefaultsAndHardCaps(t *testing.T) {
	t.Parallel()
	value := DefaultTrace()
	if value.MaxDepth != 4 || value.MaxNodes != 10_000 || value.MaxEdges != 10_000 ||
		value.FrontierBatch != 10_000 || value.LayerTimeout.String() != "30s" {
		t.Fatalf("unexpected defaults: %#v", value)
	}
	if err := value.Validate(); err != nil {
		t.Fatal(err)
	}
	value.MaxDepth = 33
	if err := value.Validate(); err == nil {
		t.Fatal("depth above 32 must fail")
	}
	value = DefaultTrace()
	value.MaxNodes = 100_001
	if err := value.Validate(); err == nil {
		t.Fatal("nodes above 100000 must fail")
	}
	value = DefaultTrace()
	value.MaxEdges = 100_001
	if err := value.Validate(); err == nil {
		t.Fatal("edges above 100000 must fail")
	}
}
