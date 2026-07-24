package syncer

import (
	"testing"
	"time"
)

func TestMetricsTrackStageAveragesAndQueueHighWater(t *testing.T) {
	metrics := &Metrics{}
	metrics.observeAgreement(10 * time.Millisecond)
	metrics.observeAgreement(30 * time.Millisecond)
	metrics.observeMismatch()
	metrics.observeNormalize(8 * time.Millisecond)
	metrics.observeNormalize(12 * time.Millisecond)
	metrics.observePublish(2, 19, 20*time.Millisecond)
	metrics.observeAgreedQueue(3, 300)
	metrics.observeAgreedQueue(1, 100)

	snapshot := metrics.Snapshot()
	if snapshot.AgreementCalls != 2 ||
		snapshot.AgreementMismatches != 1 ||
		snapshot.AgreementWaitAvg != 20*time.Millisecond {
		t.Fatalf("agreement metrics = %#v", snapshot)
	}
	if snapshot.NormalizedBlocks != 2 ||
		snapshot.NormalizeAvg != 10*time.Millisecond {
		t.Fatalf("normalization metrics = %#v", snapshot)
	}
	if snapshot.PublishedBlocks != 2 ||
		snapshot.PublishedBatches != 1 ||
		snapshot.PublishedRows != 19 ||
		snapshot.PublishAvg != 20*time.Millisecond {
		t.Fatalf("publication metrics = %#v", snapshot)
	}
	if snapshot.AgreedQueueItems != 1 ||
		snapshot.AgreedQueueBytes != 100 ||
		snapshot.AgreedQueueHighItems != 3 ||
		snapshot.AgreedQueueHighBytes != 300 {
		t.Fatalf("queue metrics = %#v", snapshot)
	}
}
