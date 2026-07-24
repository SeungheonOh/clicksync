# Archived prototype direct N2N live-wire evidence — 2026-07-23

Scope: bounded transport gate only. No local `cardano-node`, Ogmios, Dolos,
Oura, chain database, or ClickHouse was used. The resulting dataset label was
`complete_history=false` and `partial_tail`.

## Pinned inputs

- gOuroboros release: `v0.189.1`
- upstream commit: `9293adc2c94e390ca0545e870e5272ab9ac969fa`
- module sum: `h1:JIPi37b45LQL3e0Kvit5s/OSWjcB/V7WEI9bZ/T27Tc=`
- module-file sum: `h1:n1NCfwdivyiavSvEa+hZoNWWSUNc3m6Lqw4WH16GQbQ=`
- builder: `golang:1.25.8-alpine3.23`
- builder index digest:
  `sha256:8e02eb337d9e0ea459e041f1ee5eece41cbb61f1d83e7d883a3e2fb4862063fa`

The module identity was independently printed from the owned module cache
with:

```sh
docker run --rm \
  --name clicksync-p2p-modulecheck-20260723 \
  --network none \
  --label io.clicksync.scope=p2p-build-check \
  --cpus 1 --memory 256m --pids-limit 64 --log-driver none \
  --mount type=volume,source=clicksync-p2p-gomod,target=/go/pkg/mod \
  -v "$PWD":/src -w /src \
  golang:1.25.8-alpine3.23@sha256:8e02eb337d9e0ea459e041f1ee5eece41cbb61f1d83e7d883a3e2fb4862063fa \
  go mod download -json github.com/blinklabs-io/gouroboros@v0.189.1
```

The command returned the release, commit, and both required sums above.

## Compile and unit gate

An initially auto-created unlabeled cache was removed by its exact owned name,
then recreated with:

```sh
docker volume rm clicksync-p2p-gomod
docker volume create \
  --label io.clicksync.scope=p2p-build-cache \
  clicksync-p2p-gomod

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
ok  clicksync/p2p/cmd/clicksync-p2p  0.002s
ok  clicksync/p2p/internal/contract  0.027s
```

The labeled dependency cache occupied 213.9 MiB after the test. It is the only
persistent Docker object created by the build/test path and is far below the
project budget.

The contract tests include malformed/wrong-session acknowledgement rejection,
parent-pipe closure, context cancellation while backpressured, sequence
commit-after-write, and protocol-only NDJSON.

## Container artifact

```sh
docker build --pull --tag clicksync-p2p:wire-gate \
  --iidfile /tmp/clicksync-p2p-wire-gate.iid .
docker image inspect clicksync-p2p:wire-gate \
  --format '{{.Id}} {{.Size}}'
```

Local result:

```text
sha256:6d2da9e7cba485c95c611728a44614a740276ccfbaa2c10b16ff382b3164af77 5610155
```

This local image identifier is build evidence, not a published immutable
release identity.

## Two-operator corroboration gate

Before the run, every running container's full ID, name, and restart count was
captured and sorted. The experiment used no host network and no default bridge:

```sh
docker network create --driver bridge \
  --label io.clicksync.scope=p2p-proof \
  clicksync-p2p-proof-20260723

python3 scripts/ack_probe.py \
  docker run --rm -i \
  --name clicksync-p2p-proof-20260723 \
  --network clicksync-p2p-proof-20260723 \
  --label io.clicksync.scope=p2p-proof \
  --cpus 2 \
  --memory 1g \
  --pids-limit 128 \
  --log-driver none \
  clicksync-p2p:wire-gate \
  --timeout 90s --dial-timeout 15s

docker network rm clicksync-p2p-proof-20260723
```

Observed:

- IOG and Cardano Foundation negotiated N2N version `15`;
- both returned slot `193253841`, block `13715435`, hash
  `e98663bea810a45b59bf2783e40dbd2c69f79e1594b4cd0e160646a3f587eb13`;
- the fetched block was Conway type `7` with `9` decoded transactions;
- both body commitments matched;
- requested versus decoded slot/hash matched;
- the EMURGO default name returned DNS `NXDOMAIN` at test time;
- the unavailable name was rotated over without weakening the required
  corroboration count; and
- process exit status was zero after the bounded probe parent acknowledged source
  sequence `2`.

After removing only the named network, the running-container ID/name/restart
list exactly matched the pre-run baseline: all 29 unrelated containers
remained present and every restart count remained zero. The sorted baseline
fingerprint was:

```text
sha256:3daf2286b34c97a9104d895b0ab4462a12694d63f3c0c0ac60e5989e501a001d
```

The named proof container was absent because of `--rm`, and the named proof
network was confirmed absent after its explicit removal. No global Docker
cleanup command was used.

`scripts/ack_probe.py` is accurately described as a bounded probe parent. It
checks schema version, sequence continuity, and session consistency and sends
acks; it is not yet the complete production D-004 input/envelope validator.

## Honest boundary

This proves current live interoperability for handshake, a ChainSync point, a
single inclusive BlockFetch request, current Conway decoding, body commitment,
and cross-operator recognition. It does not prove Origin traversal, Byron EBB
handling, continuous roll-forward/rollback behavior, ledger validity, chain
selection, complete history, or UTxO normalization. Those remain separate
gates.
