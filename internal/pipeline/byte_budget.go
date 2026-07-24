package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ByteBudget bounds retained data across a buffered stage. Acquire transfers
// ownership into the stage; Release transfers it to the next bounded stage.
type ByteBudget struct {
	mu      sync.Mutex
	limit   int64
	used    int64
	changed chan struct{}
}

func NewByteBudget(limit int64) (*ByteBudget, error) {
	if limit <= 0 {
		return nil, errors.New("byte budget limit must be positive")
	}
	return &ByteBudget{
		limit:   limit,
		changed: make(chan struct{}),
	}, nil
}

func (b *ByteBudget) Acquire(ctx context.Context, size int64) error {
	if b == nil {
		return errors.New("byte budget is nil")
	}
	if ctx == nil {
		return errors.New("byte budget context is required")
	}
	if size < 0 {
		return errors.New("byte budget size cannot be negative")
	}
	if size > b.limit {
		return fmt.Errorf(
			"item size %d exceeds byte budget %d",
			size,
			b.limit,
		)
	}
	if size == 0 {
		if err := ctx.Err(); err != nil {
			return context.Cause(ctx)
		}
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return context.Cause(ctx)
		}
		b.mu.Lock()
		if b.used <= b.limit-size {
			b.used += size
			b.mu.Unlock()
			if err := ctx.Err(); err != nil {
				if releaseErr := b.Release(size); releaseErr != nil {
					return errors.Join(context.Cause(ctx), releaseErr)
				}
				return context.Cause(ctx)
			}
			return nil
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
}

func (b *ByteBudget) Release(size int64) error {
	if b == nil {
		return errors.New("byte budget is nil")
	}
	if size < 0 {
		return errors.New("byte budget size cannot be negative")
	}
	if size == 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if size > b.used {
		return fmt.Errorf(
			"release size %d exceeds used byte budget %d",
			size,
			b.used,
		)
	}
	b.used -= size
	close(b.changed)
	b.changed = make(chan struct{})
	return nil
}

func (b *ByteBudget) Usage() (used, limit int64) {
	if b == nil {
		return 0, 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used, b.limit
}
