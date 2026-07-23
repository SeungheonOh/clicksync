package writerlock

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestExclusiveAndRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "writer.lock")
	first, err := Acquire(path, "single-host-flock")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire(path, "single-host-flock"); !errors.Is(err, ErrAlreadyHeld) {
		t.Fatalf("second writer error = %v", err)
	}
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	second, err := Acquire(path, "single-host-flock")
	if err != nil {
		t.Fatalf("reacquire after release: %v", err)
	}
	t.Cleanup(func() { _ = second.Release() })
}

func TestCoordinationMustBeExplicit(t *testing.T) {
	if _, err := Acquire(filepath.Join(t.TempDir(), "writer.lock"), ""); err == nil {
		t.Fatal("acquired without explicit single-host gate")
	}
}
