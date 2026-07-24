# Clicksync

Clicksync is a single Go ingestion service for compact Cardano UTxO and
transaction-context facts. It connects outbound to public Cardano relays with
Ouroboros node-to-node ChainSync and BlockFetch and writes directly to
ClickHouse through its native protocol. It does not require a local Cardano
node and does not persist blocks, transactions, or scripts as raw CBOR.

The root binary is ingestion-only. Independent querying and bounded fund-flow
traversal live in the separately buildable `clickout` Go module.

## Validation status

The implementation is still undergoing validation. The 100 GB development
allocation is a soft planning target only: Clicksync has no storage quota,
threshold, warning, or publication pause. Normal synchronization is
storage-unbounded and continues until external shutdown or a fail-closed
correctness/non-recoverable ClickHouse error. Transient peer transport and
corroboration unavailability retry indefinitely with capped backoff. Operators
provision and monitor their ClickHouse storage externally.

Writer exclusion is explicitly single-host: every supported writer must mount
the same `clicksync-state` volume and hold its advisory lock for process
lifetime. Remote or multi-host writers are unsupported.

## Build and test

```sh
test -z "$(git status --porcelain)"
export CLICKSYNC_BUILD_ID="$(git rev-parse HEAD)"
docker build \
  --build-arg CLICKSYNC_BUILD_ID="$CLICKSYNC_BUILD_ID" \
  -t clicksync:local .
docker run --rm --network none clicksync:local peers
```

Repository tests use the digest-pinned builder:

```sh
docker run --rm --network none \
  -v "$PWD:/src:ro" -w /src \
  golang@sha256:8e02eb337d9e0ea459e041f1ee5eece41cbb61f1d83e7d883a3e2fb4862063fa \
  go test -mod=readonly ./...
```

## First run

Copy `.env.example` to `.env` and replace the password. The example starts at
the corroborated post-van-Rossem/PV11 point
`193253841:e98663bea810a45b59bf2783e40dbd2c69f79e1594b4cd0e160646a3f587eb13`.
Clicksync BlockFetches that boundary and derives its height; the resulting
dataset is correctly reported as partial history.

Build from a clean, committed tree so the image carries the exact source
identity, migrate once, then start continuous ingestion:

```sh
test -z "$(git status --porcelain)"
export CLICKSYNC_BUILD_ID="$(git rev-parse HEAD)"
docker compose build
docker compose run --rm clicksync migrate
docker compose up
```

The example publishes ClickHouse's password-authenticated native protocol on
all host interfaces at port `19000` for an independently run Clickout client.
The host port is controlled by `CLICKHOUSE_NATIVE_PORT` and defaults to `9000`
when unset; Clicksync's container-to-container connection remains
`clickhouse:9000`. Restrict the published port with the host firewall as
appropriate. A host Clickout process uses:

```sh
export CLICKOUT_CLICKHOUSE_ADDR=127.0.0.1:19000
export CLICKOUT_CLICKHOUSE_USERNAME=clicksync
export CLICKOUT_CLICKHOUSE_PASSWORD='<the configured ClickHouse password>'
export CLICKOUT_CLICKHOUSE_DATABASE=clicksync
```

Clicksync has no cumulative block-count, tip, time, or storage stop. For a
bounded validation run, an external harness may poll `clicksync status`, send
`SIGTERM` after enough committed blocks, and verify the final staged prefix was
flushed. That harness policy is not part of the Clicksync runtime.

For a complete-history backfill, set `CLICKSYNC_START=origin`, leave
`CLICKSYNC_START_POINT` empty, and run `clicksync sync`. There is no block,
tip, elapsed-time, or storage ceiling; operators provision and monitor their
ClickHouse storage externally.

The supported root commands are `migrate`, `sync`, `status`, `peers`, and
`writer`. See `docs/schema-contract.md` for the Clickout-facing
snapshot contract and `docs/direct-p2p-utxo-plan.md` for binding acceptance
requirements.
