package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestOrderedMapperReordersAndHonorsRollbackBarrier(t *testing.T) {
	input := make(chan Event[int, string], 5)
	output := make(chan Result[int, string], 5)
	var (
		mu      sync.Mutex
		started []int
	)
	mapper := OrderedMapper[int, int, string]{
		Workers:  2,
		Window:   2,
		MaxBytes: 10,
		Decode: func(_ context.Context, value int) (int, error) {
			mu.Lock()
			started = append(started, value)
			mu.Unlock()
			if value == 1 {
				time.Sleep(20 * time.Millisecond)
			}
			return value * 10, nil
		},
	}
	first, second, third := 1, 2, 3
	rollback := "target"
	input <- Event[int, string]{Forward: &first, Bytes: 1}
	input <- Event[int, string]{Forward: &second, Bytes: 1}
	input <- Event[int, string]{Rollback: &rollback}
	input <- Event[int, string]{Forward: &third, Bytes: 1}
	close(input)

	done := make(chan error, 1)
	go func() {
		done <- mapper.Run(context.Background(), input, output)
		close(output)
	}()
	var got []Result[int, string]
	for value := range output {
		got = append(got, value)
	}
	if err := <-done; err != nil {
		t.Fatalf("run mapper: %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("result count = %d, want 4", len(got))
	}
	if got[0].Forward == nil || *got[0].Forward != 10 ||
		got[1].Forward == nil || *got[1].Forward != 20 ||
		got[2].Rollback == nil || *got[2].Rollback != rollback ||
		got[3].Forward == nil || *got[3].Forward != 30 {
		t.Fatalf("unexpected ordered results: %#v", got)
	}
	for index, value := range got {
		if value.Sequence != uint64(index) {
			t.Fatalf("result %d sequence = %d", index, value.Sequence)
		}
	}
}

func TestOrderedMapperRejectsInvalidEventsAndByteOverflow(t *testing.T) {
	for name, test := range map[string]struct {
		event Event[int, string]
		want  string
	}{
		"empty": {
			event: Event[int, string]{},
			want:  "exactly one",
		},
		"both": {
			event: func() Event[int, string] {
				value, rollback := 1, "x"
				return Event[int, string]{Forward: &value, Rollback: &rollback}
			}(),
			want: "exactly one",
		},
		"large": {
			event: func() Event[int, string] {
				value := 1
				return Event[int, string]{Forward: &value, Bytes: 11}
			}(),
			want: "exceeds mapper bound",
		},
		"rollback bytes": {
			event: func() Event[int, string] {
				rollback := "x"
				return Event[int, string]{Rollback: &rollback, Bytes: 1}
			}(),
			want: "cannot retain forward bytes",
		},
	} {
		t.Run(name, func(t *testing.T) {
			input := make(chan Event[int, string], 1)
			input <- test.event
			close(input)
			err := (OrderedMapper[int, int, string]{
				Workers:  1,
				Window:   1,
				MaxBytes: 10,
				Decode: func(_ context.Context, value int) (int, error) {
					return value, nil
				},
			}).Run(context.Background(), input, make(chan Result[int, string], 1))
			if err == nil || !contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestOrderedMapperReleasesInputBudgetOnDispatch(t *testing.T) {
	budget, err := NewByteBudget(10)
	if err != nil {
		t.Fatalf("new byte budget: %v", err)
	}
	if err := budget.Acquire(context.Background(), 6); err != nil {
		t.Fatalf("acquire input bytes: %v", err)
	}
	value := 7
	input := make(chan Event[int, string], 1)
	input <- Event[int, string]{Forward: &value, Bytes: 6}
	close(input)
	output := make(chan Result[int, string], 1)
	err = (OrderedMapper[int, int, string]{
		Workers:           1,
		Window:            1,
		MaxBytes:          10,
		ReleaseInputBytes: budget.Release,
		Decode: func(_ context.Context, value int) (int, error) {
			return value, nil
		},
	}).Run(context.Background(), input, output)
	if err != nil {
		t.Fatalf("run mapper: %v", err)
	}
	if used, _ := budget.Usage(); used != 0 {
		t.Fatalf("used budget = %d, want 0", used)
	}
}

func TestOrderedMapperCountsCompletedResultsUntilOrderedDelivery(t *testing.T) {
	for _, test := range []struct {
		name     string
		window   int
		maxBytes int64
	}{
		{name: "item window", window: 3, maxBytes: 100},
		{name: "byte window", window: 100, maxBytes: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			const values = 20
			input := make(chan Event[int, string], values)
			output := make(chan Result[int, string], values)
			for value := range values {
				item := value
				input <- Event[int, string]{Forward: &item, Bytes: 1}
			}
			close(input)

			releaseFirst := make(chan struct{})
			started := make(chan int, values)
			done := make(chan error, 1)
			go func() {
				done <- (OrderedMapper[int, int, string]{
					Workers:  2,
					Window:   test.window,
					MaxBytes: test.maxBytes,
					Decode: func(_ context.Context, value int) (int, error) {
						started <- value
						if value == 0 {
							<-releaseFirst
						}
						return value, nil
					},
				}).Run(context.Background(), input, output)
			}()

			for count := 0; count < 3; count++ {
				select {
				case <-started:
				case <-time.After(time.Second):
					t.Fatal("mapper did not fill its configured window")
				}
			}
			select {
			case value := <-started:
				t.Fatalf(
					"decoder started value %d beyond blocked ordered window",
					value,
				)
			case <-time.After(20 * time.Millisecond):
			}

			close(releaseFirst)
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("run mapper: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("mapper did not finish after releasing sequence zero")
			}
			if len(output) != values {
				t.Fatalf("output count = %d, want %d", len(output), values)
			}
		})
	}
}

func TestOrderedMapperStopsOnDecodeError(t *testing.T) {
	value := 1
	input := make(chan Event[int, string], 1)
	input <- Event[int, string]{Forward: &value, Bytes: 1}
	close(input)
	sentinel := errors.New("bad block")
	err := (OrderedMapper[int, int, string]{
		Workers:  1,
		Window:   1,
		MaxBytes: 10,
		Decode: func(context.Context, int) (int, error) {
			return 0, sentinel
		},
	}).Run(context.Background(), input, make(chan Result[int, string], 1))
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want %v", err, sentinel)
	}
}

func TestOrderedMapperPreservesCancellationCause(t *testing.T) {
	sentinel := errors.New("attempt stopped")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(sentinel)
	err := (OrderedMapper[int, int, string]{
		Workers:  1,
		Window:   1,
		MaxBytes: 10,
		Decode: func(context.Context, int) (int, error) {
			return 0, nil
		},
	}).Run(
		ctx,
		make(chan Event[int, string]),
		make(chan Result[int, string]),
	)
	if !errors.Is(err, sentinel) {
		t.Fatalf("cancellation error = %v, want %v", err, sentinel)
	}
}

func contains(value, part string) bool {
	for index := 0; index+len(part) <= len(value); index++ {
		if value[index:index+len(part)] == part {
			return true
		}
	}
	return false
}
