# Archived prototype normalized UTxO evidence — 2026-07-23

Scope: one bounded, two-operator-corroborated, peer-observed mainnet block
normalized in memory. No local Cardano node, Ogmios, chain database,
ClickHouse, raw-CBOR staging file, or host network was used.

## Unit and golden gate

The labeled 213.9 MiB module cache created in the transport checkpoint was
reused read-only from the source tree:

```sh
docker run --rm \
  --name clicksync-p2p-buildcheck-20260723 \
  --network none \
  --label io.clicksync.scope=p2p-build-check \
  --cpus 2 --memory 2g --pids-limit 256 --log-driver none \
  --mount type=volume,source=clicksync-p2p-gomod,target=/go/pkg/mod \
  -v "$PWD":/src:ro -w /src \
  golang:1.25.8-alpine3.23@sha256:8e02eb337d9e0ea459e041f1ee5eece41cbb61f1d83e7d883a3e2fb4862063fa \
  go test -count=1 ./...
```

Result:

```text
ok  clicksync/p2p/cmd/clicksync-p2p       0.002s
ok  clicksync/p2p/internal/contract        0.027s
ok  clicksync/p2p/internal/normalize       0.003s
```

The 10 normalizer tests include:

- a literal, independently calculated datum vector:
  CBOR `01` hashes to
  `ee155ace9c40292074cb6aff8c9ccdd273c81648ff1149ef36bcea6ebb8a3e25`;
- a literal transaction-body vector:
  CBOR `a0` hashes to
  `d36a2619a672494604e11bb447cbcf5231e9f2ba25c2169177edc941bd50ad6c`;
- valid regular input/output/mint/fee projection;
- invalid collateral-only flow, collateral-return index, and suppressed mint;
- invalid collateral fee known and unknown cases;
- inline, witness, and hash-only datum behavior;
- witness-datum deduplication;
- empty and 32-byte asset names;
- UInt64/Int64 and asset-name bounds;
- transaction-ID mismatch; and
- explicit Dijkstra nested-transaction failure.

The JSON negative test asserts that the serialized normalized shape contains
none of block, transaction, or script CBOR, redeemers, metadata, or governance
fields. The only emitted CBOR field is explicitly named `datum_cbor_hex`.

## Artifact

```sh
docker build --pull --tag clicksync-p2p:normalized-gate \
  --iidfile /tmp/clicksync-p2p-normalized-gate.iid .
docker image inspect clicksync-p2p:normalized-gate \
  --format '{{.Id}} {{.Size}}'
```

Local result:

```text
sha256:f6f3714e92b8d5e32e73f8e978c28319794c0849d1d5650ae6be80e909114904 5641239
```

This is local build evidence, not a published immutable release identity.

## Safe live command

```sh
docker network create --driver bridge \
  --label io.clicksync.scope=p2p-normalized-proof \
  clicksync-p2p-normalized-proof-20260723

python3 scripts/ack_probe.py \
  docker run --rm -i \
  --name clicksync-p2p-normalized-proof-20260723 \
  --network clicksync-p2p-normalized-proof-20260723 \
  --label io.clicksync.scope=p2p-normalized-proof \
  --cpus 2 --memory 1g --pids-limit 128 --log-driver none \
  clicksync-p2p:normalized-gate \
  --timeout 90s --dial-timeout 15s

docker network rm clicksync-p2p-normalized-proof-20260723
```

The EMURGO default was DNS-unavailable and was rotated over. IOG and Cardano
Foundation both negotiated N2N version 15 and returned the identical block:

- era: Conway;
- slot: `193254554`;
- block number: `13715469`;
- hash:
  `4e8d2240de5da2039357f95265de405989457165c5f8c222857841573bbf6696`;
- parent hash:
  `0640a6b2eb03f29bc4dd1302f291e08f118f92572832c4a382ddd65e05edc661`.

The acknowledged roll-forward envelope contained:

- 2 recomputed transaction IDs;
- 4 effective regular spends;
- 3 effective outputs;
- 2 known fees;
- raw address bytes encoded explicitly as hex;
- zero mint deltas; and
- zero datum bodies for this particular live block.

The first real edge was:

```text
6d937074…772b5#1
  -> tx 0325f78c…61fb5
  -> 0325f78c…61fb5#0
```

The no-live-datum result is not presented as a datum wire proof. Exact inline
and witness datum CBOR/hash equivalence is proven by the literal/golden tests
above; a live datum-bearing sample remains an explicit later gate.

The envelope did not contain raw block/transaction/script CBOR, reference
inputs, redeemers, metadata, certificates, staking, or governance fields. It
was labeled `complete_history=false`, `partial_tail`, and
`peer_observed_structurally_verified`. Because this was a single fetched
candidate without its predecessor body, parent/block-number/slot-continuity
verification flags remained false rather than being overstated.

After the acknowledged event and clean exit, only the named network was
removed. The unrelated-container baseline fingerprint remained exactly
`3daf2286b34c97a9104d895b0ab4462a12694d63f3c0c0ac60e5989e501a001d`;
the proof container and network were absent.

## Explicit boundary

Top-level Dijkstra transactions with no sub-transactions are projected through
the common transaction interface. A Dijkstra transaction carrying nested
sub-transactions fails closed with
`unsupported Dijkstra nested transaction semantics`; Clicksync does not guess
their output references. This boundary must be removed only after an
independent current-era vector proves the semantics.
