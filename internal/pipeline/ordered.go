// Package pipeline contains the bounded concurrency needed between agreement
// and publication. It knows nothing about Cardano or ClickHouse.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type Event[I, R any] struct {
	Forward  *I
	Rollback *R
	Bytes    int64
}

type Result[O, R any] struct {
	Sequence uint64
	Forward  *O
	Rollback *R
}

type Decode[I, O any] func(context.Context, I) (O, error)

type OrderedMapper[I, O, R any] struct {
	Workers           int
	Window            int
	MaxBytes          int64
	Decode            Decode[I, O]
	ReleaseInputBytes func(int64) error
}

type decodeJob[I any] struct {
	sequence uint64
	value    I
	bytes    int64
}

type decodeResult[O any] struct {
	sequence uint64
	value    O
	bytes    int64
	err      error
}

type orderedResult[O, R any] struct {
	value Result[O, R]
	bytes int64
}

// Run decodes forward events concurrently and emits them in input order.
// Rollbacks are barriers: Run stops consuming input until all earlier decode
// jobs and the rollback itself have been delivered downstream.
func (m OrderedMapper[I, O, R]) Run(
	ctx context.Context,
	input <-chan Event[I, R],
	output chan<- Result[O, R],
) error {
	if input == nil || output == nil {
		return errors.New("ordered mapper input and output are required")
	}
	if m.Workers < 1 {
		return errors.New("ordered mapper workers must be positive")
	}
	if m.Window < m.Workers {
		return errors.New("ordered mapper window must be at least its worker count")
	}
	if m.MaxBytes <= 0 {
		return errors.New("ordered mapper byte bound must be positive")
	}
	if m.Decode == nil {
		return errors.New("ordered mapper decoder is required")
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan decodeJob[I])
	results := make(chan decodeResult[O], m.Window)
	var workers sync.WaitGroup
	workers.Add(m.Workers)
	for range m.Workers {
		go func() {
			defer workers.Done()
			for job := range jobs {
				value, err := m.Decode(runCtx, job.value)
				result := decodeResult[O]{
					sequence: job.sequence,
					value:    value,
					bytes:    job.bytes,
					err:      err,
				}
				select {
				case results <- result:
				case <-runCtx.Done():
					return
				}
				if err != nil {
					return
				}
			}
		}()
	}
	defer func() {
		close(jobs)
		cancel()
		workers.Wait()
	}()

	pending := make(map[uint64]orderedResult[O, R], m.Window)
	var (
		nextAssign       uint64
		nextEmit         uint64
		outstanding      int
		outstandingBytes int64
		inputOpen        = true
		barrier          bool
		held             *Event[I, R]
	)

	emitReady := func() error {
		for {
			item, ok := pending[nextEmit]
			if !ok {
				return nil
			}
			select {
			case output <- item.value:
			case <-ctx.Done():
				return context.Cause(ctx)
			}
			if item.value.Rollback != nil {
				barrier = false
			}
			outstanding--
			outstandingBytes -= item.bytes
			if outstanding < 0 || outstandingBytes < 0 {
				return errors.New("ordered mapper accounting underflow")
			}
			delete(pending, nextEmit)
			nextEmit++
		}
	}

	for inputOpen || held != nil || outstanding > 0 || len(pending) > 0 {
		if err := emitReady(); err != nil {
			return err
		}

		if held != nil && !barrier && outstanding < m.Window {
			event := *held
			if event.Forward != nil {
				if event.Bytes < 0 {
					return errors.New("forward event has a negative byte size")
				}
				if event.Bytes > m.MaxBytes {
					return fmt.Errorf(
						"single forward event has %d bytes, exceeds mapper bound %d",
						event.Bytes,
						m.MaxBytes,
					)
				}
				if event.Bytes <= m.MaxBytes-outstandingBytes {
					job := decodeJob[I]{
						sequence: nextAssign,
						value:    *event.Forward,
						bytes:    event.Bytes,
					}
					select {
					case jobs <- job:
						if m.ReleaseInputBytes != nil {
							if err := m.ReleaseInputBytes(event.Bytes); err != nil {
								return fmt.Errorf(
									"release accepted input bytes: %w",
									err,
								)
							}
						}
						outstanding++
						outstandingBytes += event.Bytes
						nextAssign++
						held = nil
						continue
					case result := <-results:
						if result.err != nil {
							return fmt.Errorf(
								"decode sequence %d: %w",
								result.sequence,
								result.err,
							)
						}
						value := result.value
						pending[result.sequence] = orderedResult[O, R]{
							value: Result[O, R]{
								Sequence: result.sequence,
								Forward:  &value,
							},
							bytes: result.bytes,
						}
						continue
					case <-ctx.Done():
						return context.Cause(ctx)
					}
				}
			} else {
				value := *event.Rollback
				pending[nextAssign] = orderedResult[O, R]{
					value: Result[O, R]{
						Sequence: nextAssign,
						Rollback: &value,
					},
				}
				outstanding++
				nextAssign++
				held = nil
				barrier = true
				continue
			}
		}

		canRead := inputOpen && held == nil && !barrier && outstanding < m.Window
		if !canRead && outstanding == 0 {
			if held != nil {
				return errors.New("ordered mapper cannot make progress within byte bound")
			}
			if !inputOpen && len(pending) == 0 {
				break
			}
		}

		var inputChannel <-chan Event[I, R]
		if canRead {
			inputChannel = input
		}
		select {
		case event, ok := <-inputChannel:
			if !ok {
				inputOpen = false
				continue
			}
			if (event.Forward == nil) == (event.Rollback == nil) {
				return errors.New("event must contain exactly one forward or rollback value")
			}
			if event.Rollback != nil && event.Bytes != 0 {
				return errors.New("rollback event cannot retain forward bytes")
			}
			held = &event
		case result := <-results:
			if result.err != nil {
				return fmt.Errorf(
					"decode sequence %d: %w",
					result.sequence,
					result.err,
				)
			}
			value := result.value
			pending[result.sequence] = orderedResult[O, R]{
				value: Result[O, R]{
					Sequence: result.sequence,
					Forward:  &value,
				},
				bytes: result.bytes,
			}
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	return emitReady()
}
