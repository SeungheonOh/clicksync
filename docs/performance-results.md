# Performance results

Date: 2026-07-24

These results separate publication capacity from relay/network variability.
They are evidence for the insertion-path goal, not a claim about public-relay
speed.

Completed coverage includes both a 100,352-block generated-fact publication
run, a fresh 100,000-consecutive-block strict two-relay real-CBOR replay, and
an optimized two-relay run using the sliding ChainSync request window. The
first isolates writer capacity; the relay runs measure variable public-relay
end-to-end behavior.

## Environment

- AMD Ryzen 9 7950X, 16 cores / 32 threads
- 29 GiB RAM; the host was not otherwise isolated and had swap pressure
- Linux 7.0.0-27-generic
- Go 1.25.8, linux/amd64
- Docker 29.1.3
- ClickHouse 26.3.12.3 in a fresh disposable local container
- native ClickHouse protocol, LZ4, 16 connections
- `async_insert=0`, `wait_for_async_insert=1`

## 100,000-block strict two-relay live replay

The fresh dataset started after configured mainnet point 10,781,330. Its first
100,000 committed adoptions were the exact Conway range 10,781,331 through
10,881,330. Both configured relays supplied every block:

- `backbone.cardano.iog.io:3001` (`3.9.192.44:3001` in the recorded sample);
- `backbone.mainnet.cardanofoundation.org:3001`
  (`195.49.96.164:3001`);
- both negotiated N2N v15.

The run intentionally included graceful process and ClickHouse restarts.
Across those restarts, the first 100,000 rows had 100,000 unique publication
IDs, 100,000 unique event sequences, and a 100,000-block inclusive number
span. There were no invalidations, rollback headers, gaps, duplicate active
adoptions, or malformed two-relay provenance rows.

| First 100,000 blocks | Rows |
|---|---:|
| Blocks | 100,000 |
| Transactions | 1,131,793 |
| Inputs | 4,513,815 |
| Outputs | 3,289,173 |
| Datum bodies | 870,899 |
| Datum observations | 970,116 |
| Withdrawals | 248,521 |
| Redeemers | 1,068,405 |
| Metadata | 359,305 |
| **Total facts** | **12,552,027** |

One uninterrupted portion had committed 81,408 blocks when measured from
database timestamps, at 104.69 blocks/second. The nearby runtime snapshot
reported:

| Measure | Result |
|---|---:|
| Agreement wait average | 9.608 ms/block |
| Normalize average | 0.617 ms/block |
| Publication average | 75.844 ms/batch |
| Agreement mismatches / reconnects | 0 / 0 |
| Agreed queue current / high-water | 0 / 256 blocks |
| Agreed byte high-water | 4,173,370 bytes |
| Sync RSS snapshot | 357.9 MiB |
| ClickHouse memory snapshot | 1,023 MiB |

At the integrity snapshot, 102,603 committed blocks occupied 1.08 GiB of
active ClickHouse parts. The service remained running after the measurement.
Because public relay throughput varies, 104.69 blocks/second is an observed
deployment result rather than a deterministic benchmark.

## Sliding-window ChainSync live run

The optimized session retained one N2N connection per relay and a 512-block
BlockFetch range. Its internal ChainSync driver kept at most 100
`RequestNext` messages outstanding and refilled 20 requests after every 20
completed callbacks. ChainSync ingress, range construction, BlockFetch, and
publication remained independent bounded stages.

After warm-up, actual two-relay intervals writing to ClickHouse reported:

| Measure | Observed result |
|---|---:|
| Header delivery per relay | about 540-576 blocks/s |
| Body delivery per relay | about 540-569 blocks/s |
| Agreed and published throughput | about 545-585 blocks/s |
| Raw ingress per relay | about 64 Mbit/s |
| Reconnects / agreement mismatches | 0 / 0 |

A six-second packet capture confirmed that each ChainSync refill mux segment
contained exactly 20 `RequestNext` messages. The two relay connections emitted
those refill writes about every 29-31 ms on average. They were distributed
throughout the response stream rather than delayed until all 100 responses
had arrived.

The final committed build later crossed a denser span at 380.17 agreed
blocks/second. Mean raw size was 23,572 bytes/block and the limiting relay
delivered 71.64 Mbit/second, which predicts 379.89 blocks/second from byte
throughput alone. Its ChainSync rate was 381.82 headers/second, BlockFetch rate
was 378.96 bodies/second, 278 headers were already pending, and BlockFetch duty
was 94.5%. For comparison, the faster span averaged 14,741 bytes/block:
64.39 Mbit/second predicts 546.00 blocks/second, matching its observed rate.
This reconciles the block-rate variation with relay body bandwidth and block
size; neither header starvation nor ClickHouse insertion explains it.

During the continued integrity run, one snapshot contained 436,358 block rows
and 436,358 unique block hashes across the inclusive block-number span
10,781,331 through 11,217,688, with no malformed two-operator provenance.
The service remained on its first attempt with no reported rollback.

This was an actual-network observation, not a controlled same-span A/B:
public-relay routing and block density changed between the baseline and
optimized runs. It nevertheless verifies that ClickHouse publication stayed
with relay intake and that the previous roughly 100-block/s scheduling ceiling
was removed.

## BlockFetch range trial

Range candidates used the same build, pinned relay IPs, fresh disposable
ClickHouse volumes, and historical mainnet point. Each measured the exact
32,768-block span 11,416,385 through 11,449,152 after a 16,384-block warm-up.
The span had the same 14.634 transactions and 137.695 selected facts per block
in every run.

| Run order | Range | Observed blocks/s | Inserted blocks/s | p95 observe-to-insert |
|---:|---:|---:|---:|---:|
| 1 | 512 | 425.48 | 426.37 | 1,098 ms |
| 2 | 1,024 | 435.48 | 439.04 | 1,143 ms |
| 3 | 2,048 | 481.07 | 488.24 | 1,047 ms |
| 4 | 512 | 519.90 | 517.38 | 1,066 ms |
| 5 | 2,048 | 410.30 | 415.50 | 1,055 ms |
| 6 | 8,000, with 16,000 prefilled | 378.10 | 375.53 | 1,066 ms |

Every sample had 32,768 unique block numbers, hashes, publication IDs, and
adoption events, with valid two-operator provenance, zero mismatches, and zero
reconnects. The large reversal between adjacent runs demonstrates that public
relay/path variation is materially larger than any measured range-size gain.
ClickHouse latency remained stable and publication kept pace.

The phase counters provide the selection signal: with a 512-block range, the
next range was often prepared before the current stream ended and the limiting
relay spent roughly 91% of wall time fetching bodies. With 2,048 blocks, no
next range was ever prepared; one relay spent only about 65% of wall time
fetching because it repeatedly waited for enough headers.

A separate actual-relay canary successfully fetched complete 8,000-block
ranges, confirming that 512 is not a protocol limit. It then exhausted the
header backlog, waited seconds for each next range, fell to roughly 184-208
blocks/second, and caused a keepalive timeout and reconnect. Larger batches
therefore reduce overlap on this single connection rather than increasing wire
throughput.

A follow-up canary explicitly waited for 16,000 headers before starting the
first 8,000-block fetch. Only the preloaded second range was ready in advance;
the following nine transitions waited for headers. Across its first 80,000
blocks, throughput was 398.31 blocks/second including prefill and 406.20 after
the reserve drained. Measuring only from the first arriving body hid the
prefill cost and produced a misleading 461.31 blocks/second. At ten completed
ranges, the slower relay's fetch duty was 54.2% with 7.19 seconds of average
inter-range idle. The exact fixed-span row above remained below both 512
controls. The production default remains 512 blocks.

## Crash and query contract

The tagged integration test passed against the disposable ClickHouse:

```text
TestClickHouseFreshInitializationAndWriteOnlyPublication   0.40s
```

It populated every one of the nine fact tables, committed adoption, recovered
state through both `State` and read-only `Inspect`, rolled back, and re-adopted
with new identifiers. Its query-log assertion observed zero `SELECT` queries
for the successful roll-forward and exactly ten inserts: nine populated fact
tables plus adoption.

## 100,352-block generated-fact publication replay

Command shape:

```sh
go test -tags=clickhouse_integration \
  -run '^$' \
  -bench '^BenchmarkClickHousePublisherRepresentativeBatch$' \
  -benchtime=98x \
  -count=1 \
  -benchmem \
  ./internal/store
```

The timed section published 98 batches of 1,024 blocks:

| Measure | Result |
|---|---:|
| Timed blocks | 100,352 |
| Fact rows per block | 9 |
| Timed fact rows | 903,168 |
| Mean 1,024-block publication | 11.716 ms |
| Publication capacity | 87,402 blocks/s |
| Fact capacity | 786,614 fact rows/s |
| Raw-length accounting throughput | 107.85 MiB/s |
| Go allocations per batch | 12,771,038 bytes / 132,698 allocs |

Benchmark output:

```text
BenchmarkClickHousePublisherRepresentativeBatch-32
  98  11716034 ns/op  107.85 MB/s  12771038 B/op  132698 allocs/op
```

Each representative block populated blocks, transactions, inputs, outputs,
datum bodies, datum observations, withdrawals, redeemers, and metadata. The
benchmark uses generated compact facts rather than a redistributed 100,000
block CBOR range, so it measures publication only. ClickHouse physical-row
counts confirmed the warm-up and timed publications were materialized.

The result exceeds the 1,000 blocks/s publication gate by roughly 87x and the
optimized strict-relay intake by roughly 150x. On this host, table insertion is
not the live bottleneck.

## Normalization

Checked-in real Conway fixture, parallel benchmark:

```text
201,887 ns/op
472,850 B/op
3,181 allocs/op
```

That is roughly 4,953 representative fixture blocks/s before publication.

A synthetic large Conway block benchmark measured:

```text
78.62 ms/op
21.88 MiB/s
98.29 MiB/op
682,635 allocs/op
```

Profiles attribute most large-block allocation to gOuroboros/fxamacker CBOR
decoding rather than fact projection. This makes decoding the first local
optimization target if real relay intake exceeds worker capacity.

## Remaining measurements

- The live range arrived from public relays; it is not a fixed local CBOR
  corpus suitable for deterministic A/B performance comparisons.
- Per-relay header/body rates, byte rates, range timings, and bounded-queue
  occupancy are session-local runtime counters; TCP RTT/cwnd/retransmission
  data still requires focused packet or host instrumentation.
- The background service had not yet reached the current mainnet tip at the
  measurement cutoff.

These limitations prevent overstating the public-network result while leaving
the insertion-headroom conclusion clear.
