package syncer

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ShutdownBudget gives finalization stages one shared absolute deadline.
type ShutdownBudget struct {
	timeout time.Duration
	once    sync.Once
	until   time.Time
}

func NewShutdownBudget(timeout time.Duration) (*ShutdownBudget, error) {
	if timeout <= 0 {
		return nil, errors.New("shutdown budget must be positive")
	}
	return &ShutdownBudget{timeout: timeout}, nil
}

func (budget *ShutdownBudget) Context(
	parent context.Context,
) (context.Context, context.CancelFunc) {
	until := budget.deadline()
	return context.WithDeadline(context.WithoutCancel(parent), until)
}

func (budget *ShutdownBudget) FinalizeContext(
	parent context.Context,
	attemptTimeout time.Duration,
) (context.Context, context.CancelFunc) {
	if parent.Err() != nil {
		return budget.Context(parent)
	}
	finalizeCtx, cancel := context.WithTimeout(
		context.WithoutCancel(parent),
		attemptTimeout,
	)
	stopShutdownWatch := context.AfterFunc(parent, func() {
		shutdownCtx, stopShutdown := budget.Context(parent)
		defer stopShutdown()
		select {
		case <-shutdownCtx.Done():
			cancel()
		case <-finalizeCtx.Done():
		}
	})
	return finalizeCtx, func() {
		stopShutdownWatch()
		cancel()
	}
}

func (budget *ShutdownBudget) deadline() time.Time {
	budget.once.Do(func() {
		budget.until = time.Now().Add(budget.timeout)
	})
	return budget.until
}
