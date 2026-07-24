# Cardano Clicksync

Clicksync is a non-validating Cardano-to-ClickHouse fact collector. It fetches
the same ordered blocks from every configured relay, requires exact agreement,
decodes one retained copy, and writes table-oriented ClickHouse batches.

The narrow trust claim is:

> Every configured relay returned the same event at the same ChainSync point,
> and—for roll-forward—the same exact raw block bytes.

Clicksync does not validate consensus, signatures, scripts, fees, UTxO state,
value conservation, transaction IDs, or block-body hashes. If every relay
agrees on invalid data, Clicksync records it.

## Architecture

```text
N gOuroboros relay sessions
        ↓
strict all-of-N point/type/raw-hash agreement
        ↓
bounded parallel decode and normalization
        ↓
ordered block/byte/row/time microbatches
        ↓
parallel ClickHouse fact-table inserts
        ↓
one final adoption batch
```

Facts are invisible until their adoption rows are acknowledged. Rollbacks
append descendant invalidations first and one rollback header last; headerless
invalidation remnants are inert. There is no dataset manifest, UTxO resolver,
corroboration state machine, quorum, voting, or primary-relay suffix.

If relays initially select different canonical intersection candidates, the
whole set retries from the suffix beginning at the oldest selected candidate.
Once they unanimously select an older point, Clicksync commits a rollback to
that point before forward streaming. If the same descendants arrive afterward,
they are re-adoptions of invalidated publications, not duplicate active rows
over a still-committed suffix.

The full design is in:

- [Implementation plan](docs/implementation-plan.md)
- [Legacy analysis](docs/legacy-analysis.md)
- [Architecture](docs/architecture.md)
- [Schema and crash contract](docs/schema.md)
- [Work items and tests](docs/work-items.md)
- [Performance protocol](docs/performance.md)
- [Measured performance](docs/performance-results.md)

## Run with Compose

```sh
cp .env.example .env
# Set a real CLICKHOUSE_PASSWORD in .env.
docker compose up --build
```

Compose runs the embedded migration once, then starts synchronization.
ClickHouse HTTP is published on all IPv4 interfaces; its native protocol
remains loopback-only. Protect the HTTP port with the host firewall when it
must not be reachable from an untrusted network.

Inspect the committed dataset:

```sh
docker compose run --rm clicksync status
```

Stop with `docker compose down`. Named ClickHouse volumes are retained.

## Commands

The binary implements only:

```text
clicksync migrate
clicksync sync
clicksync status
```

`migrate` uses only ClickHouse settings. `status` additionally reads the
rollback-window depth; neither command parses relay settings. `sync` requires
the complete configuration and holds a process-lifetime advisory writer lock.

Run migrations before `sync` when not using Compose.

## Required configuration

| Variable | Meaning | Default |
|---|---|---|
| `CLICKHOUSE_HOST` | Native ClickHouse host | `127.0.0.1` |
| `CLICKHOUSE_NATIVE_PORT` | Native protocol port | `9000` |
| `CLICKHOUSE_USER` | ClickHouse user | `clicksync` |
| `CLICKHOUSE_PASSWORD` | ClickHouse password | required |
| `CLICKHOUSE_DATABASE` | Fixed schema database | `clicksync` |
| `CARDANO_NETWORK_NAME` | Immutable dataset label | `mainnet` |
| `CARDANO_NETWORK_MAGIC` | Node-to-node network magic | `764824073` |
| `CARDANO_RELAYS` | Comma-separated `host:port\|operator` entries | two mainnet relays |
| `CLICKSYNC_START` | `origin` or `SLOT:HASH:BLOCK_NUMBER[:ebb]` | documented mainnet point |

At least two distinct endpoints and distinct operator labels are required.
Every configured relay participates in every agreement.

`origin` means start ChainSync at origin. Clicksync records relay-delivered
on-chain blocks; it does not fabricate a synthetic initial UTxO distribution.

## Throughput and runtime settings

| Variable | Default |
|---|---:|
| `CLICKHOUSE_OPEN_CONNS` | `16` |
| `CLICKSYNC_HEADER_BATCH_SIZE` | `512` |
| `CLICKSYNC_PROTOCOL_QUEUE_SIZE` | `512` |
| `CLICKSYNC_RELAY_QUEUE_SIZE` | `256` |
| `CLICKSYNC_AGREED_QUEUE_SIZE` | `256` |
| `CLICKSYNC_AGREED_QUEUE_BYTES` | `256 MiB` |
| `CLICKSYNC_NORMALIZE_WORKERS` | `GOMAXPROCS` |
| `CLICKSYNC_REORDER_SIZE` | `256` |
| `CLICKSYNC_REORDER_BYTES` | `256 MiB` |
| `CLICKSYNC_BATCH_BLOCKS` | `1024` |
| `CLICKSYNC_BATCH_BYTES` | `128 MiB` |
| `CLICKSYNC_BATCH_ROWS` | `2,000,000` |
| `CLICKSYNC_BATCH_AGE` | `1s` |
| `CLICKSYNC_ROLLBACK_DEPTH` | `2160` |
| `CLICKSYNC_SHUTDOWN_TIMEOUT` | `45s` |

All raw-data boundaries have item and byte bounds. ClickHouse uses native LZ4
with synchronous acknowledged inserts. Independent populated fact tables are
sent concurrently, followed by one adoption insert; ordinary roll-forward
performs no ClickHouse reads.

## Shutdown and progress

On SIGINT or SIGTERM, `sync` cancels relay intake, drains already agreed work,
flushes the final complete batch, closes ClickHouse, and releases the writer
lock. If that cannot be proven complete within `CLICKSYNC_SHUTDOWN_TIMEOUT`,
the runner returns `ErrShutdownTimeout`. Because Go cannot kill an operation
that ignores cancellation, this result is process-terminal: the CLI retains
the database handle and lock until `os.Exit` instead of releasing the writer
fence underneath possible in-flight work.

Periodic JSON progress stays intentionally compact. It reports lifetime
attempts, reconnects, agreed blocks/bytes, published blocks/batches,
published rows, rollbacks, agreement calls/mismatches, and normalized blocks.
It also reports lifetime-average agreement/publication rates, average
agreement wait, normalization and publication duration, plus current and
high-water agreed-queue items/bytes. Focused benchmarks and profiles provide
deeper detail; the runtime does not carry a per-relay throughput or
reorder/worker-utilization metrics framework.

## Measured throughput

A fresh live mainnet dataset committed 100,000 consecutive Conway blocks from
the IOG and Cardano Foundation relays, both negotiated at N2N v15. The exact
range was block 10,781,331 through 10,881,330 and produced 12,552,027 fact
rows with no duplicate identifiers, gaps, disagreement, or bad two-relay
provenance. An uninterrupted 81,408-block portion committed at 104.69
blocks/second; the agreed queue drained to zero between bursts.

The isolated ClickHouse writer published 100,352 generated-fact blocks at
87,402 blocks/second and 786,614 fact rows/second. Public-relay intake—not
insertion—was the live bottleneck. See
[Measured performance](docs/performance-results.md).

The intake trace also identified a serialized ChainSync/BlockFetch range
bubble. The approved follow-up keeps one N2N connection per relay, supports
protocol-valid ranges larger than 512, and prefetches the next header range
while the current body range streams. See the
[single-connection BlockFetch throughput plan](docs/single-connection-blockfetch-plan.md).

## Supported data

Checked-in real fixtures cover Byron main/EBB, Shelley, Allegra, Mary, Alonzo,
Babbage, Conway, and Dijkstra. The normalizer preserves phase-2 flow,
regular/collateral/reference inputs, collateral return, assets, mint/burn,
datums, withdrawals, all current redeemer purposes, and metadata.

Nested Dijkstra subtransactions are rejected because the schema has no defined
flattening semantics. Address credentials and reference-script enrichment are
nullable; raw addresses remain authoritative.

## Development

The module targets Go 1.25.8.

```sh
go test ./...
go vet ./...
go test -race ./...
go test -bench=. ./internal/normalize ./internal/store
```

Store integration tests are opt-in:

```sh
CLICKSYNC_CLICKHOUSE_INTEGRATION=1 \
CLICKHOUSE_HOST=127.0.0.1 \
CLICKHOUSE_NATIVE_PORT=19000 \
CLICKHOUSE_USER=clicksync \
CLICKHOUSE_PASSWORD=... \
go test -tags=clickhouse_integration ./internal/store
```

Use a disposable ClickHouse instance: the tagged test owns the fixed
`clicksync` database identity. The exact environment variable is also recorded
in the test skip message. Live relay testing is opt-in through
`CLICKSYNC_LIVE_RELAY` and `CLICKSYNC_LIVE_POINT`.
