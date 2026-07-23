package writerlock

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
	if err := first.AssertHeld(); err != nil {
		t.Fatalf("assert first ownership: %v", err)
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

func TestLockPathReplacementFailsOwnershipCheck(t *testing.T) {
	path := filepath.Join(t.TempDir(), "writer.lock")
	lock, err := Acquire(path, "single-host-flock")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lock.Release() })
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := lock.AssertHeld(); err == nil {
		t.Fatal("replaced lock path was accepted")
	}
}

func TestTwoProcessesCannotBothAcquire(t *testing.T) {
	path := filepath.Join(t.TempDir(), "writer.lock")
	holder := helperCommand(t, path, "hold")
	stdout, err := holder.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := holder.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = holder.Process.Kill()
		_ = holder.Wait()
	})
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || scanner.Text() != "acquired" {
		t.Fatalf("holder did not acquire: %q (%v)", scanner.Text(), scanner.Err())
	}
	contender := helperCommand(t, path, "try")
	output, err := contender.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 23 {
		t.Fatalf("contender = %v, output %q; want exit 23", err, output)
	}
}

func TestProcessDeathReleasesWithoutStaleTakeover(t *testing.T) {
	path := filepath.Join(t.TempDir(), "writer.lock")
	holder := helperCommand(t, path, "hold")
	stdout, err := holder.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := holder.Start(); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || scanner.Text() != "acquired" {
		t.Fatalf("holder did not acquire: %q (%v)", scanner.Text(), scanner.Err())
	}
	if err := holder.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := holder.Wait(); err == nil {
		t.Fatal("killed helper unexpectedly exited cleanly")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		lock, acquireErr := Acquire(path, "single-host-flock")
		if acquireErr == nil {
			if err := lock.Release(); err != nil {
				t.Fatal(err)
			}
			break
		}
		if !errors.Is(acquireErr, ErrAlreadyHeld) || time.Now().After(deadline) {
			t.Fatalf("acquire after process death: %v", acquireErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func helperCommand(t *testing.T, path, action string) *exec.Cmd {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestWriterLockHelperProcess$")
	command.Env = append(os.Environ(),
		"CLICKSYNC_LOCK_HELPER=1",
		"CLICKSYNC_LOCK_HELPER_PATH="+path,
		"CLICKSYNC_LOCK_HELPER_ACTION="+action,
	)
	return command
}

func TestWriterLockHelperProcess(t *testing.T) {
	if os.Getenv("CLICKSYNC_LOCK_HELPER") != "1" {
		return
	}
	lock, err := Acquire(os.Getenv("CLICKSYNC_LOCK_HELPER_PATH"), "single-host-flock")
	if err != nil {
		if errors.Is(err, ErrAlreadyHeld) {
			os.Exit(23)
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(24)
	}
	defer lock.Release()
	switch strings.TrimSpace(os.Getenv("CLICKSYNC_LOCK_HELPER_ACTION")) {
	case "try":
		os.Exit(0)
	case "hold":
		fmt.Println("acquired")
		for {
			time.Sleep(time.Hour)
		}
	default:
		os.Exit(25)
	}
}
