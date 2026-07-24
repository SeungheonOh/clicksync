package relay

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"cardano-clicksync/internal/config"
	"cardano-clicksync/internal/model"
)

// TestLiveHandshakeAndRange is opt-in because it requires a Cardano N2N
// relay and a recent point on that relay's selected chain.
func TestLiveHandshakeAndRange(t *testing.T) {
	host := os.Getenv("CLICKSYNC_LIVE_RELAY")
	pointText := os.Getenv("CLICKSYNC_LIVE_POINT")
	if host == "" || pointText == "" {
		t.Skip("set CLICKSYNC_LIVE_RELAY and CLICKSYNC_LIVE_POINT to run")
	}
	configPoint, err := config.ParsePoint(pointText)
	if err != nil {
		t.Fatalf("parse CLICKSYNC_LIVE_POINT: %v", err)
	}
	candidate := model.Point{
		Origin:      configPoint.Origin,
		Slot:        configPoint.Slot,
		Hash:        configPoint.Hash,
		BlockNumber: configPoint.BlockNumber,
		IsByronEBB:  configPoint.IsByronEBB,
	}
	session, err := New(
		Config{
			RelayIndex:        0,
			Host:              host,
			Operator:          "live-test",
			NetworkMagic:      config.MainnetMagic,
			ProtocolQueueSize: 16,
			HeaderBatchSize:   1,
			RelayQueueSize:    2,
			RawQueueBytes:     16 << 20,
			DialTimeout:       10 * time.Second,
			BlockTimeout:      30 * time.Second,
		},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	runDone := make(chan error, 1)
	go func() {
		runDone <- session.Run(ctx, []model.Point{candidate})
	}()
	event, err := session.Next(ctx)
	if err != nil {
		cancel()
		t.Fatalf("read live relay event: %v", err)
	}
	if event.Kind != Forward ||
		len(event.RawCBOR) == 0 ||
		event.RawLength != uint64(len(event.RawCBOR)) ||
		event.Digest != RawBlockDigest(event.BlockType, event.RawCBOR) {
		cancel()
		t.Fatalf("invalid live raw event: %+v", event)
	}
	if event.Relay.Address == "" || event.Relay.N2NVersion == 0 {
		cancel()
		t.Fatalf("missing negotiated metadata: %+v", event.Relay)
	}
	cancel()
	select {
	case err := <-runDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("stop live session: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("live session did not stop after cancellation")
	}
}
