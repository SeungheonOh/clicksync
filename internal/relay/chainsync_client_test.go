package relay

import (
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

func TestChainSyncAwaitReplyKeepsRequestAndSignalsBarrier(t *testing.T) {
	called := 0
	client := &chainSyncClient{
		awaitReply: func() error {
			called++
			return nil
		},
		outstanding:      chainSyncMaxOutstanding,
		responseDeadline: time.Now().Add(time.Minute),
	}

	if err := client.handleMessage(chainsync.NewMsgAwaitReply()); err != nil {
		t.Fatal(err)
	}
	if called != 1 {
		t.Fatalf("await-reply callbacks = %d, want 1", called)
	}
	if client.outstanding != chainSyncMaxOutstanding {
		t.Fatalf(
			"outstanding requests = %d, want %d",
			client.outstanding,
			chainSyncMaxOutstanding,
		)
	}
	if !client.responseSuspended || !client.responseDeadline.IsZero() {
		t.Fatal("response watchdog remained armed in MustReply")
	}
}
