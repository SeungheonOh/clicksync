# clicksync

`clicksync` is an append-only Cardano chain indexer backed by ClickHouse. It follows Chain Sync through Ogmios, stores immutable block/transaction/UTxO facts, and models forks as canonicality events instead of issuing expensive ClickHouse mutations.

This repository currently implements the first useful vertical slice, not full `cardano-db-sync` parity. It indexes Byron through Dijkstra block JSON, including Dijkstra sub-transactions, and exposes correct rollback/current-UTxO semantics for indexed chain outputs.

## What works

- Ogmios 7 Chain Sync with intersection-based resume and configurable pipelining.
- Blocks, transaction placement, regular/collateral/reference inputs, regular/collateral-return outputs, output assets, mint/burn facts, and selected structured payloads.
- Lossless `bigint` handling for lovelace and native-asset quantities.
- Phase-2-invalid transaction semantics: collateral is consumed, regular inputs/outputs are retained as facts but do not affect the UTxO, and collateral return is produced.
- Append-only rollback audit and per-block canonicality events.
- Crash-safe publication: block facts are written before adoption; rollback members are written before their rollback commit header.
- Re-adoption of a previously rolled-back block.
- ClickHouse views for `current_chain`, `current_transactions`, global UTxO analysis, and a primary-key-pruned `current_utxos_by_address(...)` provider path.

The key limitation is that genesis distributions and ledger-derived state are not implemented yet. Until genesis seeding lands, historical genesis outputs that remain unspent are absent. Rewards, epoch stake, ADA pots, pool distributions, and enacted governance state require ledger replay and are also out of scope for this first provider. See [architecture and parity plan](docs/architecture.md).

## Architecture

```text
cardano-node socket
       │
    Ogmios 7          maintained N2C mux/version/era codecs
       │
  ChainSource         RollForward / RollBackward / intersection
       │
  normalizer          ledger-effective immutable facts
       │
 ClickHouseStore      facts first, commit event last
       │
 canonical views      active outputs − active spends
```

Ogmios is deliberate. Reimplementing Ouroboros from scratch would also mean owning mux framing, handshake/version negotiation, pipelining, hard-fork codecs, and every future node-to-client change. Forking DB Sync would retain its deep Hasql/PostgreSQL insertion coupling. The provider boundary leaves room for a direct Haskell/consensus implementation when full ledger state is needed.

## Quick start

Requirements:

- Node.js 20.6 or newer (Node 22 is used in the container).
- ClickHouse 24.8 or newer; the compose file pins ClickHouse 26.3 LTS.
- A synced `cardano-node` plus Ogmios 7.

Start ClickHouse and install/build clicksync:

```bash
cp .env.example .env
# Review .env before the first database/container start.
docker compose up -d --wait clickhouse
npm ci
npm run build
node --env-file=.env dist/src/cli.js migrate
```

If Ogmios is already running, set `OGMIOS_HOST` and `OGMIOS_PORT` in `.env`. To run the provided Ogmios sidecar against an existing node socket named `node.socket`, first edit both `CARDANO_NETWORK` and `CARDANO_NETWORK_MAGIC` in `.env` with a matching pair (`mainnet=764824073`, `preprod=1`, `preview=2`):

```bash
CARDANO_NODE_SOCKET_DIR=/absolute/path/to/socket-directory \
docker compose --profile ogmios up -d
```

Alternatively, build and run ClickHouse, Ogmios, and clicksync as one Compose
stack after creating `.env` and configuring the socket/network pair:

```bash
CARDANO_NODE_SOCKET_DIR=/absolute/path/to/socket-directory \
docker compose --profile full up -d --build
docker compose logs -f clicksync
```

Start syncing. Run status from another terminal while the foreground sync process is active:

```bash
node --env-file=.env dist/src/cli.js sync
# In another terminal:
node --env-file=.env dist/src/cli.js status
```

For development:

```bash
npm run check
npm test
npm run test:integration
```

## Useful queries

Current tip:

```sql
SELECT block_number, slot, block_hash
FROM clicksync.current_chain
WHERE network_magic = 764824073
ORDER BY canonical_event_seq DESC
LIMIT 1;
```

UTxOs at an address use the parameterized provider fast path (it prunes both
outputs and candidate spends by their primary keys):

```sql
SELECT tx_hash, output_index, lovelace
FROM clicksync.current_utxos_by_address(
  network_magic = 764824073,
  address = 'addr1...'
);
```

Native assets currently held at an address:

```sql
SELECT a.policy_id, a.asset_name, sum(a.quantity) AS quantity
FROM (SELECT * FROM clicksync.output_assets FINAL) AS a
INNER JOIN clicksync.current_utxos_by_address(
  network_magic = 764824073,
  address = 'addr1...'
) AS u
  ON a.network_magic = u.network_magic
 AND a.block_hash = u.block_hash
 AND a.tx_hash = u.tx_hash
 AND a.output_index = u.output_index
WHERE a.network_magic = 764824073
  AND a.is_produced = true
GROUP BY a.policy_id, a.asset_name;
```

Rollback audit:

```sql
SELECT observed_at, reason, rollback_to_slot, rollback_to_hash, depth
FROM clicksync.rollbacks FINAL
WHERE network_magic = 764824073
ORDER BY event_seq DESC;
```

Raw tables are intentionally append-only. Applications should normally use the `current_*` views, which apply `FINAL` where retry deduplication is required.

## Operational invariants

- Run one clicksync writer per `(ClickHouse database, network_magic)`.
- Historical blocks are published in bounded batches; tune `CLICKSYNC_BATCH_BLOCKS` for throughput and memory.
- A rollback deeper than `CLICKSYNC_MAX_ROLLBACK_DEPTH` fails closed. Investigate a stale/divergent node or deliberately raise both the guard and `CLICKSYNC_INTERSECTION_CANDIDATES` before retrying; configuration requires enough dense candidates for the entire allowed window.
- Never derive the tip with `max(slot)` from raw blocks; orphan rows remain by design.
- Do not build an incremental materialized view by joining facts to current canonical status. A later rollback insert would not retrigger old fact rows.
- Use a fresh database for a full reindex in this version. Stream generations and atomic pagination snapshots are planned but not yet exposed.
- Compose binds the unauthenticated ClickHouse and Ogmios ports to `127.0.0.1` by default. Keep them private; if you deliberately override the `*_BIND` variables, add authentication/TLS and network controls first.
- Ogmios health cannot distinguish standard mainnet magic `764824073` from a legacy/custom magic-`0` network. The supplied named-network pair is validated; custom magic-`0` deployments must verify their genesis/configuration out of band before ingestion.

See [docs/architecture.md](docs/architecture.md) for the DB Sync comparison, rollback proof, failure model, and implementation roadmap.
