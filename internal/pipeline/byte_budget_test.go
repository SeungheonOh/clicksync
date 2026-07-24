package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestByteBudgetBlocksUntilReleased(t *testing.T) {
	budget, err := NewByteBudget(10)
	if err != nil {
		t.Fatalf("new byte budget: %v", err)
	}
	if err := budget.Acquire(context.Background(), 8); err != nil {
		t.Fatalf("acquire initial bytes: %v", err)
	}
	acquired := make(chan error, 1)
	go func() {
		acquired <- budget.Acquire(context.Background(), 4)
	}()
	select {
	case err := <-acquired:
		t.Fatalf("second acquisition did not block: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	if err := budget.Release(4); err != nil {
		t.Fatalf("release bytes: %v", err)
	}
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("second acquisition: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second acquisition remained blocked")
	}
	if used, limit := budget.Usage(); used != 8 || limit != 10 {
		t.Fatalf("usage = (%d, %d), want (8, 10)", used, limit)
	}
}

func TestByteBudgetRejectsOversizeAndHonorsCancellation(t *testing.T) {
	budget, err := NewByteBudget(4)
	if err != nil {
		t.Fatalf("new byte budget: %v", err)
	}
	if err := budget.Acquire(context.Background(), 5); err == nil {
		t.Fatal("oversize acquisition unexpectedly succeeded")
	}
	if err := budget.Acquire(context.Background(), 4); err != nil {
		t.Fatalf("fill budget: %v", err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	want := errors.New("stop")
	cancel(want)
	if err := budget.Acquire(ctx, 1); !errors.Is(err, want) {
		t.Fatalf("canceled acquisition = %v, want %v", err, want)
	}
	fresh, err := NewByteBudget(4)
	if err != nil {
		t.Fatalf("new fresh budget: %v", err)
	}
	if err := fresh.Acquire(ctx, 1); !errors.Is(err, want) {
		t.Fatalf("free pre-canceled acquisition = %v, want %v", err, want)
	}
	if used, _ := fresh.Usage(); used != 0 {
		t.Fatalf("pre-canceled acquisition retained %d bytes", used)
	}
}

func TestByteBudgetRejectsOverRelease(t *testing.T) {
	budget, err := NewByteBudget(4)
	if err != nil {
		t.Fatalf("new byte budget: %v", err)
	}
	if err := budget.Acquire(context.Background(), 2); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := budget.Release(3); err == nil {
		t.Fatal("over-release unexpectedly succeeded")
	}
}
