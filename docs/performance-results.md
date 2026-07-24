# Performance results

Date: 2026-07-24

These results separate publication capacity from relay/network variability.
They are evidence for the insertion-path goal, not a claim about public-relay
speed.

Completed coverage includes both a 100,352-block generated-fact publication
run and a fresh 100,000-consecutive-block strict two-relay real-CBOR replay.
The former isolates writer capacity; the latter measures variable public-relay
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
measured strict-relay intake by more than 800x. On this host, table insertion
is not the live bottleneck.

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
- Per-relay byte rates were not added to the runtime metrics. Docker network
  totals and aggregate agreed bytes show the broad network bound without
  creating a larger telemetry subsystem.
- The background service had not yet reached the current mainnet tip at the
  measurement cutoff.

These limitations prevent overstating the public-network result while leaving
the insertion-headroom conclusion clear.
