package relay

import (
	"sync/atomic"
	"time"
)

const fetchProgressInterval = 10 * time.Second

type fetchMetrics struct {
	startedAt      atomic.Int64
	chainEvents    atomic.Uint64
	headers        atomic.Uint64
	chainWaitNs    atomic.Uint64
	chainHigh      atomic.Int64
	pendingHeaders atomic.Int64
	pendingHigh    atomic.Int64
	jobs           atomic.Uint64
	jobWaitNs      atomic.Uint64
	jobHigh        atomic.Int64
	eventHigh      atomic.Int64
	rawQueueHigh   atomic.Int64
}

type fetchTotals struct {
	ranges             uint64
	expectedBlocks     uint64
	rawBlocks          uint64
	rawBytes           uint64
	transitions        uint64
	preparedBeforeDone uint64
	headerCollect      time.Duration
	getRangeWait       time.Duration
	bodyStream         time.Duration
	active             time.Duration
	interRangeIdle     time.Duration
	rawBudgetWait      time.Duration
	eventSendWait      time.Duration
}

func (m *fetchMetrics) start(now time.Time) {
	if m == nil {
		return
	}
	m.startedAt.CompareAndSwap(0, now.UTC().UnixNano())
}

func (m *fetchMetrics) observeChainEvent(
	kind chainEventKind,
	wait time.Duration,
	depth int,
) {
	if m == nil {
		return
	}
	if kind == chainForward {
		m.headers.Add(1)
	}
	m.chainEvents.Add(1)
	m.chainWaitNs.Add(uint64(max(wait, 0)))
	raiseHighWater(&m.chainHigh, int64(depth))
}

func (m *fetchMetrics) observePending(pending int) {
	if m == nil {
		return
	}
	m.pendingHeaders.Store(int64(pending))
	raiseHighWater(&m.pendingHigh, int64(pending))
}

func (m *fetchMetrics) observeJob(wait time.Duration, depth int) {
	if m == nil {
		return
	}
	m.jobs.Add(1)
	m.jobWaitNs.Add(uint64(max(wait, 0)))
	raiseHighWater(&m.jobHigh, int64(depth))
}

func (m *fetchMetrics) observeEventDepth(depth int) {
	if m != nil {
		raiseHighWater(&m.eventHigh, int64(depth))
	}
}

func (m *fetchMetrics) observeRawDepth(bytes int64) {
	if m != nil {
		raiseHighWater(&m.rawQueueHigh, bytes)
	}
}

func (s *Session) observeCompletedRange(state *activeRange) {
	if state == nil {
		return
	}
	totals := &s.fetchTotals
	totals.ranges++
	totals.expectedBlocks += uint64(len(state.headers))
	totals.rawBlocks += uint64(state.next)
	totals.rawBytes += state.rawBytes
	totals.headerCollect += state.headerCollect
	totals.getRangeWait += state.getRangeWait
	if !state.firstRaw.IsZero() && !state.lastRaw.IsZero() {
		totals.bodyStream += state.lastRaw.Sub(state.firstRaw)
	}
	totals.active += state.completed.Sub(state.requestStarted)
	totals.rawBudgetWait += state.rawBudgetWait
	totals.eventSendWait += state.eventSendWait
	if state.hasPriorRange {
		totals.transitions++
		totals.interRangeIdle += state.interRangeIdle
		if state.preparedBefore {
			totals.preparedBeforeDone++
		}
	}
	s.logFetchProgress(state.completed)
}

func (s *Session) logFetchProgress(now time.Time) {
	startedNs := s.fetchMetrics.startedAt.Load()
	if startedNs == 0 {
		return
	}
	started := time.Unix(0, startedNs)
	if s.fetchLogAt.IsZero() {
		s.fetchLogAt = started
	}
	if now.Sub(s.fetchLogAt) < fetchProgressInterval {
		return
	}
	s.fetchLogAt = now

	elapsed := now.Sub(started).Seconds()
	if elapsed <= 0 {
		return
	}
	totals := s.fetchTotals
	headers := s.fetchMetrics.headers.Load()
	chainEvents := s.fetchMetrics.chainEvents.Load()
	jobs := s.fetchMetrics.jobs.Load()
	rawQueued, _ := s.RawQueueDepth()
	s.logger.Info(
		"relay fetch progress",
		"headers", headers,
		"ranges", totals.ranges,
		"range_blocks", totals.expectedBlocks,
		"raw_blocks", totals.rawBlocks,
		"raw_bytes", totals.rawBytes,
		"headers_per_second", float64(headers)/elapsed,
		"body_blocks_per_second", float64(totals.rawBlocks)/elapsed,
		"raw_mbit_per_second", float64(totals.rawBytes*8)/elapsed/1_000_000,
		"range_blocks_avg", averageCount(totals.expectedBlocks, totals.ranges),
		"header_collect_avg", averageDuration(totals.headerCollect, totals.ranges),
		"get_range_wait_avg", averageDuration(totals.getRangeWait, totals.ranges),
		"body_stream_avg", averageDuration(totals.bodyStream, totals.ranges),
		"inter_range_idle_avg", averageDuration(
			totals.interRangeIdle,
			totals.transitions,
		),
		"prepared_before_done_ratio", ratio(
			totals.preparedBeforeDone,
			totals.transitions,
		),
		"fetch_duty", totals.active.Seconds()/elapsed,
		"chainsync_enqueue_wait_avg", averageDuration(
			time.Duration(s.fetchMetrics.chainWaitNs.Load()),
			chainEvents,
		),
		"fetch_job_enqueue_wait_avg", averageDuration(
			time.Duration(s.fetchMetrics.jobWaitNs.Load()),
			jobs,
		),
		"raw_budget_wait_avg", averageDuration(
			totals.rawBudgetWait,
			totals.rawBlocks,
		),
		"event_send_wait_avg", averageDuration(
			totals.eventSendWait,
			totals.rawBlocks,
		),
		"chainsync_events_queued", len(s.chainEvents),
		"pending_headers", s.fetchMetrics.pendingHeaders.Load(),
		"fetch_jobs_queued", len(s.fetchJobs),
		"events_queued", len(s.events),
		"raw_bytes_queued", rawQueued,
		"chainsync_events_queued_high", s.fetchMetrics.chainHigh.Load(),
		"pending_headers_high", s.fetchMetrics.pendingHigh.Load(),
		"fetch_jobs_queued_high", s.fetchMetrics.jobHigh.Load(),
		"events_queued_high", s.fetchMetrics.eventHigh.Load(),
		"raw_bytes_queued_high", s.fetchMetrics.rawQueueHigh.Load(),
	)
}

func averageDuration(total time.Duration, count uint64) time.Duration {
	if count == 0 {
		return 0
	}
	return total / time.Duration(count)
}

func averageCount(total, count uint64) float64 {
	if count == 0 {
		return 0
	}
	return float64(total) / float64(count)
}

func ratio(numerator, denominator uint64) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func raiseHighWater(value *atomic.Int64, candidate int64) {
	for current := value.Load(); candidate > current; current = value.Load() {
		if value.CompareAndSwap(current, candidate) {
			return
		}
	}
}
