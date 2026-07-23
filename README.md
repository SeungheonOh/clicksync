# Clicksync

Clicksync is a single Go ingestion service for compact Cardano UTxO and
transaction-context facts. It connects outbound to public Cardano relays with
Ouroboros node-to-node ChainSync and BlockFetch and writes directly to
ClickHouse through its native protocol. It does not require a local Cardano
node and does not persist blocks, transactions, or scripts as raw CBOR.

The root binary is ingestion-only. Independent querying and bounded fund-flow
traversal live in the separately buildable `clickout` Go module.

## Safety status

The implementation is still undergoing bounded validation. Do not begin an
unbounded Origin backfill. The current host has no hard filesystem quota, and
the binding capacity gate requires a conservative settled-data projection at
or below 70 GiB before full-history ingestion is authorized.

Writer exclusion is explicitly single-host: every supported writer must mount
the same `clicksync-state` volume and hold its advisory lock for process
lifetime. Remote or multi-host writers are unsupported.

## Build and test

```sh
docker build -t clicksync:local .
docker run --rm --network none clicksync:local peers
```

Repository tests use the digest-pinned builder:

```sh
docker run --rm --network none \
  -v "$PWD:/src:ro" -w /src \
  golang@sha256:8e02eb337d9e0ea459e041f1ee5eece41cbb61f1d83e7d883a3e2fb4862063fa \
  go test -mod=readonly ./...
```

## Bounded runtime

Copy `.env.example` to `.env`, replace the password, and keep a bounded stop
mode during validation:

```sh
docker compose run --rm clicksync migrate
CLICKSYNC_MAX_BLOCKS=10 docker compose up --abort-on-container-exit
```

The supported root commands are `migrate`, `sync`, `status`, `peers`,
`storage`, and `lease`. See `docs/schema-contract.md` for the Clickout-facing
snapshot contract and `docs/direct-p2p-utxo-plan.md` for binding acceptance
requirements.
