package writerlock

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestExclusiveAndRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "writer.lock")
	first, err := Acquire(path)
	if err != nil {
		t.Fatalf("acquire first lock: %v", err)
	}
	if err := first.AssertHeld(); err != nil {
		t.Fatalf("assert first lock: %v", err)
	}
	if _, err := Acquire(path); !errors.Is(err, ErrAlreadyHeld) {
		t.Fatalf("second acquire error = %v, want ErrAlreadyHeld", err)
	}
	if err := first.Release(); err != nil {
		t.Fatalf("release first lock: %v", err)
	}
	second, err := Acquire(path)
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	if err := second.Release(); err != nil {
		t.Fatalf("release second lock: %v", err)
	}
}

func TestReplacementLosesOwnership(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "writer.lock")
	lock, err := Acquire(path)
	if err != nil {
		t.Fatalf("acquire lock: %v", err)
	}
	defer lock.Release()
	replacement := filepath.Join(dir, "replacement")
	if err := os.WriteFile(replacement, []byte("other"), 0o600); err != nil {
		t.Fatalf("write replacement: %v", err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatalf("replace lock path: %v", err)
	}
	if err := lock.AssertHeld(); err == nil {
		t.Fatal("replaced lock path still asserted ownership")
	}
}
