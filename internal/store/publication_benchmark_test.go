package store

import (
	"context"
	"errors"
	"testing"
)

type discardConnection struct{}

func (discardConnection) Exec(context.Context, string, ...any) error {
	return nil
}

func (discardConnection) PrepareBatch(context.Context, string) (batch, error) {
	return &discardBatch{}, nil
}

func (discardConnection) Query(context.Context, string, ...any) (rows, error) {
	return nil, errors.New("benchmark publication unexpectedly queried")
}

func (discardConnection) QueryRow(context.Context, string, ...any) row {
	return fakeRow{err: errors.New("benchmark publication unexpectedly queried")}
}

func (discardConnection) Ping(context.Context) error {
	return nil
}

func (discardConnection) Close() error {
	return nil
}

type discardBatch struct{}

func (*discardBatch) Append(...any) error { return nil }
func (*discardBatch) Send() error         { return nil }
func (*discardBatch) Abort() error        { return nil }

func BenchmarkPublisherRepresentativeBatch(b *testing.B) {
	const blocksPerBatch = 128
	candidates := make([]Candidate, blocksPerBatch)
	var rawBytes int64
	for index := range candidates {
		candidates[index] = richCandidate(byte(index + 1))
		rawBytes += int64(candidates[index].RawLength)
	}
	db := initializedTestDB(newFakeConnection(), 0, 0)
	db.conn = discardConnection{}
	lock := &fakeLock{}
	b.ReportAllocs()
	b.SetBytes(rawBytes)
	b.ReportMetric(blocksPerBatch, "blocks/op")
	b.ResetTimer()
	for range b.N {
		if _, err := db.Publish(context.Background(), lock, candidates); err != nil {
			b.Fatal(err)
		}
	}
}
