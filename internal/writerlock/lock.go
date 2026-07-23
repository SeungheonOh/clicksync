// Package writerlock provides the explicit single-host writer gate.
package writerlock

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

var ErrAlreadyHeld = errors.New("another clicksync writer holds the single-host lock")

type Lock struct {
	file  *os.File
	path  string
	token string
}

func Acquire(path, coordination string) (*Lock, error) {
	if coordination != "single-host-flock" {
		return nil, errors.New(
			"set CLICKSYNC_WRITER_COORDINATION=single-host-flock; remote or multi-host writers are unsupported",
		)
	}
	if path == "" {
		return nil, errors.New("writer lock path is empty")
	}
	cleanPath, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("resolve writer lock path: %w", err)
	}
	path = cleanPath
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create writer lock directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open writer lock: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrAlreadyHeld
		}
		return nil, fmt.Errorf("acquire writer flock: %w", err)
	}
	if err := file.Truncate(0); err != nil {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
		return nil, fmt.Errorf("truncate writer lock audit file: %w", err)
	}
	tokenBytes := make([]byte, 16)
	if _, err := rand.Read(tokenBytes); err != nil {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
		return nil, fmt.Errorf("create writer lock audit token: %w", err)
	}
	token := hex.EncodeToString(tokenBytes)
	if _, err := fmt.Fprintf(file, "pid=%d\nowner_token=%s\n", os.Getpid(), token); err != nil {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
		return nil, fmt.Errorf("write writer lock audit file: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
		return nil, fmt.Errorf("sync writer lock audit file: %w", err)
	}
	return &Lock{file: file, path: path, token: token}, nil
}

func (l *Lock) Path() string {
	if l == nil {
		return ""
	}
	return l.path
}

// OwnerToken is an audit correlation value. It is not a fencing token and
// conveys no authority without the live flock held by this Lock.
func (l *Lock) OwnerToken() string {
	if l == nil {
		return ""
	}
	return l.token
}

// AssertHeld fails closed if the original locked file description is closed,
// replaced, or no longer lockable. Publication calls this before every commit
// boundary. It is a local-process ownership check, not a remote coordinator.
func (l *Lock) AssertHeld() error {
	if l == nil || l.file == nil {
		return errors.New("writer lock is not held")
	}
	fileInfo, err := l.file.Stat()
	if err != nil {
		return fmt.Errorf("stat held writer lock: %w", err)
	}
	pathInfo, err := os.Stat(l.path)
	if err != nil {
		return fmt.Errorf("stat writer lock path: %w", err)
	}
	if !os.SameFile(fileInfo, pathInfo) {
		return errors.New("writer lock path was replaced while held")
	}
	if err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("verify writer flock ownership: %w", err)
	}
	return nil
}

func (l *Lock) Release() error {
	if l == nil || l.file == nil {
		return nil
	}
	unlockErr := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	closeErr := l.file.Close()
	l.file = nil
	l.token = ""
	return errors.Join(unlockErr, closeErr)
}
