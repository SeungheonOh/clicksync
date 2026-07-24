package writerlock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

var ErrAlreadyHeld = errors.New("another clicksync writer holds the lock")

type Lock struct {
	file *os.File
	path string
}

func Acquire(path string) (*Lock, error) {
	if path == "" {
		return nil, errors.New("writer lock path is empty")
	}
	absolute, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("resolve writer lock path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(absolute), 0o700); err != nil {
		return nil, fmt.Errorf("create writer lock directory: %w", err)
	}
	file, err := os.OpenFile(absolute, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open writer lock: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrAlreadyHeld
		}
		return nil, fmt.Errorf("acquire writer lock: %w", err)
	}
	if err := file.Truncate(0); err != nil {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
		return nil, fmt.Errorf("truncate writer lock file: %w", err)
	}
	if _, err := fmt.Fprintf(file, "pid=%d\n", os.Getpid()); err != nil {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
		return nil, fmt.Errorf("write writer lock file: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
		return nil, fmt.Errorf("sync writer lock file: %w", err)
	}
	return &Lock{file: file, path: absolute}, nil
}

func (l *Lock) AssertHeld() error {
	if l == nil || l.file == nil {
		return errors.New("writer lock is not held")
	}
	held, err := l.file.Stat()
	if err != nil {
		return fmt.Errorf("stat held writer lock: %w", err)
	}
	path, err := os.Stat(l.path)
	if err != nil {
		return fmt.Errorf("stat writer lock path: %w", err)
	}
	if !os.SameFile(held, path) {
		return errors.New("writer lock path was replaced while held")
	}
	if err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("verify writer lock: %w", err)
	}
	return nil
}

func (l *Lock) Path() string {
	if l == nil {
		return ""
	}
	return l.path
}

func (l *Lock) Release() error {
	if l == nil || l.file == nil {
		return nil
	}
	unlockErr := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	closeErr := l.file.Close()
	l.file = nil
	return errors.Join(unlockErr, closeErr)
}
