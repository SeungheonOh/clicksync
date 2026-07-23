# clicksync-p2p

`clicksync-p2p` is the bounded direct Cardano node-to-node source helper. It
opens outbound TCP connections only. It does not run a Cardano node, expose a
listener, persist a chain, or write ClickHouse.

The initial `probe` command:

1. negotiates N2N with independent configured relays;
2. obtains candidate points with ChainSync;
3. selects the oldest observed tip as the least-fresh candidate;
4. fetches that exact block from each required relay with BlockFetch;
5. requires identical header and body commitments; and
6. recomputes transaction and datum identities while projecting only
   ledger-effective UTxO facts; and
7. emits protocol-only, explicitly partial/peer-observed NDJSON.

The normalization boundary retains inputs, outputs, fees, mint/burn, raw
address/asset bytes, datum hashes, and exact datum CBOR bodies. It does not
emit block/transaction/script CBOR, reference inputs, redeemers, metadata, or
governance fields. Phase-2-invalid transactions emit only their collateral
inputs and optional collateral return. A Dijkstra transaction containing
nested sub-transactions currently fails closed because its output-reference
semantics have not yet passed an independent golden vector.

The three defaults are the separately operated IOG, EMURGO, and Cardano
Foundation bootstrap names accepted in decision D-005. A duplicate configured
address is rejected and cannot count twice toward corroboration. A failed
default is rotated over; corroboration is never silently reduced.

The helper's standard input is an acknowledgement stream. The bounded probe
parent used for the standalone transport gate is `scripts/ack_probe.py`. It is
not the full production D-004 validator.

```sh
docker build --tag clicksync-p2p:probe p2p
docker network create --driver bridge \
  --label io.clicksync.scope=p2p-proof \
  clicksync-p2p-proof-20260723
python3 p2p/scripts/ack_probe.py \
  docker run --rm -i \
  --name clicksync-p2p-proof-20260723 \
  --network clicksync-p2p-proof-20260723 \
  --label io.clicksync.scope=p2p-proof \
  --cpus 2 --memory 1g --pids-limit 128 --log-driver none \
  clicksync-p2p:probe
docker network rm clicksync-p2p-proof-20260723
```

This probe is not complete history and not independent consensus validation.
Its exact trust description is:

> peer-observed, structurally verified Cardano chain data

The optional module cache is owned and labeled:

```sh
docker volume create \
  --label io.clicksync.scope=p2p-build-cache \
  clicksync-p2p-gomod
```

The audited checkpoint occupied 213.9 MiB in that volume. It contains
downloaded Go modules only and may be removed by its exact name; no global
Docker cleanup is needed.

See `evidence/live-wire-2026-07-23.md` for the first transport gate and
`evidence/live-normalized-2026-07-23.md` for the real UTxO-flow envelope.
