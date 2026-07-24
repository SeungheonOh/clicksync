# Archived prototype bounded ClickHouse live proof

Captured 2026-07-23. The bounded demo exited `0`. It connected the Go helper
directly to two public Cardano mainnet N2N peers, passed normalized facts through
the supervised NDJSON adapter, published them append-only to ClickHouse, and
queried one exact UTxO-flow edge in both directions.

This is intentionally a one-block, partial-tail proof. It does **not** prove
continuous ChainSync, live rollback reception, Origin-to-tip completeness, or
independent Cardano consensus validation. The dataset is stored and returned as
`complete_history=false` with trust mode
`peer_observed_structurally_verified`. No `cardano-node`, Ogmios, or equivalent
long-running chain service was used or observed.

The machine-readable, secret-free result is
[v2-live-evidence.json](./v2-live-evidence.json). The retained review database is
`clicksync_utxo_v2_e2e` in the isolated `clicksync-p2p-v2-clickhouse`
container. Do not rerun the demo over that database without either supplying the
same fixed point or intentionally creating a new database/dataset identity.

## Live source and stored facts

- peers: `backbone.cardano.iog.io:3001` and
  `backbone.mainnet.cardanofoundation.org:3001`
- negotiated N2N: v15
- block: `312f8ce34de042e65d303d26667e7006af59fc23732c7799bdc68b311f7ff7cc`
- slot / block number: `193257546` / `13715607`
- rows: 1 publication, 7 transactions, 17 effective spends, 20 effective
  outputs, 7 deduplicated datum bodies, 7 datum observations, 1 adoption

The demo failed closed unless these counts matched the normalized envelope.
It also required this exact edge to appear in both traversal results:

```text
318f78d8…188cba84#2
  -> transaction 05eded9d…58d96960
  -> 05eded9d…58d96960#0
```

The forward result correctly returned all five outputs of the transaction.
The reverse result correctly returned all four inputs. It did not claim
input-to-output pair attribution.

## Query evidence

| Query | Wall time | CH read rows | CH read bytes | CH elapsed |
|---|---:|---:|---:|---:|
| point | 17.173 ms | 39 | 4,920 | 10.032 ms |
| address | 16.444 ms | 39 | 5,472 | 8.247 ms |
| forward BFS | 14.442 ms | 40 | 2,265 | 5.768 ms |
| reverse BFS | 28.115 ms | 61 | 3,187 | 14.153 ms |
| datum | 19.975 ms | 41 | 2,966 | 12.075 ms |

`EXPLAIN indexes=1` showed binary-search primary-key conditions for the forward
spend seed `(source_tx_hash, source_index)` and reverse output seed
`(tx_hash, output_index)`. Exact-address lookup showed
`PrimaryKey Condition:true`, scanning the sole proof part/granule. Address
access is therefore honestly a full scan today. No projection was added because
this one-block sample is not representative enough to justify one.

Active ClickHouse part bytes were 21,430. The exact mutable footprint of the
bind-mounted ClickHouse data plus server-log paths was 38,240,256 bytes. Docker
stdout logging was disabled to avoid a second uncounted growing log. Immutable
image, source, and dependency-cache bytes are outside that mutable measurement
and are not presented as growing database usage.

## Reproduction command shape

No real password is stored here. After following
[the operator runbook](./v2-runbook.md), the bounded command shape is:

```bash
CARDANO_NETWORK=mainnet \
CARDANO_NETWORK_MAGIC=764824073 \
CLICKHOUSE_V2_HTTP_PORT=18123 \
CLICKHOUSE_V2_USER=clicksync \
CLICKHOUSE_V2_PASSWORD='<local password>' \
CLICKHOUSE_V2_DATABASE='<fresh v2 database>' \
CLICKSYNC_V2_STORAGE_ROOT='<absolute bind root>' \
CLICKSYNC_V2_FOOTPRINT_CONTAINER=clicksync-p2p-v2-clickhouse \
CLICKSYNC_P2P_HELPER='<absolute helper path>' \
CLICKSYNC_P2P_PEERS='backbone.cardano.iog.io:3001,backbone.mainnet.cardanofoundation.org:3001' \
CLICKSYNC_P2P_CORROBORATE=2 \
CLICKSYNC_P2P_TIMEOUT=90s \
CLICKSYNC_V2_DATASET_ID='<random UUID>' \
CLICKSYNC_V2_WRITER_ID='<different random UUID>' \
npm run demo:v2
```

The full raw protocol line and datum CBOR are deliberately not committed as
evidence.
