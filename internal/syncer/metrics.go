package syncer

import (
	"sync"
	"sync/atomic"
	"time"
)

type MetricsSnapshot struct {
	StartedAt            time.Time
	Attempts             uint64
	Reconnects           uint64
	AgreedBlocks         uint64
	AgreedBytes          uint64
	PublishedBlocks      uint64
	PublishedBatches     uint64
	PublishedRows        uint64
	Rollbacks            uint64
	AgreementCalls       uint64
	AgreementMismatches  uint64
	NormalizedBlocks     uint64
	AgreedQueueItems     int64
	AgreedQueueBytes     int64
	AgreedQueueHighItems int64
	AgreedQueueHighBytes int64
	AgreedBlocksPerSec   float64
	PublishedPerSec      float64
	AgreementWaitAvg     time.Duration
	NormalizeAvg         time.Duration
	PublishAvg           time.Duration
}

type Metrics struct {
	startOnce sync.Once
	startedAt atomic.Int64

	attempts             atomic.Uint64
	reconnects           atomic.Uint64
	agreedBlocks         atomic.Uint64
	agreedBytes          atomic.Uint64
	publishedBlocks      atomic.Uint64
	publishedBatches     atomic.Uint64
	publishedRows        atomic.Uint64
	rollbacks            atomic.Uint64
	agreementCalls       atomic.Uint64
	agreementWaitNs      atomic.Uint64
	mismatches           atomic.Uint64
	normalizedBlocks     atomic.Uint64
	normalizeNs          atomic.Uint64
	publishNs            atomic.Uint64
	agreedQueueItems     atomic.Int64
	agreedQueueBytes     atomic.Int64
	agreedQueueHighItems atomic.Int64
	agreedQueueHighBytes atomic.Int64
}

func (m *Metrics) start() {
	if m == nil {
		return
	}
	m.startOnce.Do(func() {
		m.startedAt.Store(time.Now().UTC().UnixNano())
	})
}

func (m *Metrics) observeAttempt() {
	if m == nil {
		return
	}
	m.start()
	m.attempts.Add(1)
}

func (m *Metrics) observeReconnect() {
	if m != nil {
		m.reconnects.Add(1)
	}
}

func (m *Metrics) observeAgreed(bytes uint64) {
	if m == nil {
		return
	}
	m.start()
	m.agreedBlocks.Add(1)
	m.agreedBytes.Add(bytes)
}

func (m *Metrics) observeAgreement(wait time.Duration) {
	if m == nil {
		return
	}
	m.start()
	m.agreementCalls.Add(1)
	m.agreementWaitNs.Add(uint64(max(wait, 0)))
}

func (m *Metrics) observeMismatch() {
	if m != nil {
		m.mismatches.Add(1)
	}
}

func (m *Metrics) observeNormalize(duration time.Duration) {
	if m == nil {
		return
	}
	m.normalizedBlocks.Add(1)
	m.normalizeNs.Add(uint64(max(duration, 0)))
}

func (m *Metrics) observePublish(
	blocks int,
	rows uint64,
	duration time.Duration,
) {
	if m == nil {
		return
	}
	m.start()
	m.publishedBlocks.Add(uint64(blocks))
	m.publishedBatches.Add(1)
	m.publishedRows.Add(rows)
	m.publishNs.Add(uint64(max(duration, 0)))
}

func (m *Metrics) observeRollback() {
	if m != nil {
		m.rollbacks.Add(1)
	}
}

func (m *Metrics) observeAgreedQueue(items int, bytes int64) {
	if m == nil {
		return
	}
	m.agreedQueueItems.Store(int64(items))
	m.agreedQueueBytes.Store(bytes)
	updateMaximum(&m.agreedQueueHighItems, int64(items))
	updateMaximum(&m.agreedQueueHighBytes, bytes)
}

func (m *Metrics) Snapshot() MetricsSnapshot {
	if m == nil {
		return MetricsSnapshot{}
	}
	m.start()
	started := time.Unix(0, m.startedAt.Load()).UTC()
	elapsed := time.Since(started).Seconds()
	if elapsed <= 0 {
		elapsed = 1
	}
	snapshot := MetricsSnapshot{
		StartedAt:            started,
		Attempts:             m.attempts.Load(),
		Reconnects:           m.reconnects.Load(),
		AgreedBlocks:         m.agreedBlocks.Load(),
		AgreedBytes:          m.agreedBytes.Load(),
		PublishedBlocks:      m.publishedBlocks.Load(),
		PublishedBatches:     m.publishedBatches.Load(),
		PublishedRows:        m.publishedRows.Load(),
		Rollbacks:            m.rollbacks.Load(),
		AgreementCalls:       m.agreementCalls.Load(),
		AgreementMismatches:  m.mismatches.Load(),
		NormalizedBlocks:     m.normalizedBlocks.Load(),
		AgreedQueueItems:     m.agreedQueueItems.Load(),
		AgreedQueueBytes:     m.agreedQueueBytes.Load(),
		AgreedQueueHighItems: m.agreedQueueHighItems.Load(),
		AgreedQueueHighBytes: m.agreedQueueHighBytes.Load(),
	}
	snapshot.AgreedBlocksPerSec = float64(snapshot.AgreedBlocks) / elapsed
	snapshot.PublishedPerSec = float64(snapshot.PublishedBlocks) / elapsed
	if snapshot.AgreementCalls > 0 {
		snapshot.AgreementWaitAvg = time.Duration(
			m.agreementWaitNs.Load() / snapshot.AgreementCalls,
		)
	}
	if snapshot.NormalizedBlocks > 0 {
		snapshot.NormalizeAvg = time.Duration(
			m.normalizeNs.Load() / snapshot.NormalizedBlocks,
		)
	}
	if snapshot.PublishedBatches > 0 {
		snapshot.PublishAvg = time.Duration(
			m.publishNs.Load() / snapshot.PublishedBatches,
		)
	}
	return snapshot
}

func updateMaximum(value *atomic.Int64, candidate int64) {
	for current := value.Load(); candidate > current; current = value.Load() {
		if value.CompareAndSwap(current, candidate) {
			return
		}
	}
}
