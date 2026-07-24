# Clicksync Direct-P2P UTxO Indexer: Steering Plan and Decision Record

Status: binding implementation plan
Last steering revision: 2026-07-23
Steering owner: root agent
Implementation ownership: delegated manager and worker agents only

## 1. Goal

Replace the repository with one clean Go ingestion service that:

1. connects directly to remote Cardano relays using the Ouroboros node-to-node
   mini-protocols;
2. continuously follows the peer-observed chain without a local Cardano node,
   protocol bridge, or second persistent chain store;
3. decodes block bodies in bounded memory and writes compact, queryable
   historical UTxO and Plutus-transaction facts directly to ClickHouse;
4. handles restart, rollback, re-adoption, peer failover, and append-only
   ClickHouse publication correctly;
5. retains datum, withdrawals, redeemers with resolved purposes, and metadata
   needed for normal Plutus dApp/fund-flow analysis;
6. excludes full block/transaction CBOR, script bodies, and unrelated ledger
   state;
7. is storage-unbounded at runtime while measuring the current workspace
   experiment against a non-enforcing 100 GB planning target; and
8. proves fund-origin forward and reverse traversal using a completely
   independent analysis module.

The persistent goal is recorded in the agent goal system. It is complete only
after adversarial reviewers independently confirm the live direct-P2P
ingestion and fund-origin query demonstration.

## 2. User overrides and rejected prototype decisions

The following decisions supersede the earlier prototype:

- There is no legacy runtime, compatibility path, migration mode, or parallel
  generation of the application.
- The TypeScript/Node implementation is deleted.
- The old bridge-based source, broad schema, dependencies, tests, container
  profile, and documentation are deleted.
- The temporary Go-to-TypeScript NDJSON helper boundary is deleted.
- Go owns networking, normalization, publication, rollback, schema migration,
  and direct ClickHouse insertion in one service.
- Clicksync has no application storage ceiling, quota setup, warning threshold,
  high-water pause, or capacity gate. The user's 100 GB allocation is a soft
  planning target for this development run, not product behavior.
- The database has one current schema and one database name. Normal schema
  migration bookkeeping is allowed; duplicated product generations are not.
- `clicksync` contains ingestion only. Fund tracing and every other analytical
  command live in a separately buildable Go module that does not import
  Clicksync code.
- Transaction context now includes withdrawals, redeemers, and metadata.

The prior bounded mixed-runtime proof may inform private reconstruction work,
but its source, commands, database/container names, generated artifacts, and
evidence files are not retained in the final tree. Final evidence is generated
only by the replacement Go ingestion path and uses no product-generation
suffixes.

## 3. Final repository contract

The final repository contains only these product surfaces:

```text
clicksync/
  cmd/clicksync/          ingestion CLI only
  internal/               P2P, decoding, sync, publication, persistence
  migrations/             the sole ClickHouse schema
  config/                 peer/network/ClickHouse configuration
  clickout/               independent Go module and analysis CLI
  testdata/               bounded era/protocol fixtures
  docs/                   architecture, runbook, evidence, decisions
  compose.yaml            ClickHouse + Go clicksync only
  Dockerfile              Go clicksync image
  go.mod
  go.sum
```

`clickout/` has its own:

```text
clickout/go.mod
clickout/go.sum
clickout/cmd/clickout/
clickout/internal/
clickout/Dockerfile
```

The analysis module:

- has no `replace` directive pointing to the root module;
- imports no root Clicksync package;
- communicates only through ClickHouse's documented schema;
- can be copied and built outside this repository; and
- is not linked into the `clicksync` binary or image.

Final hygiene gates fail if any of the following remain outside `.git` history:

- TypeScript or JavaScript product/test files;
- `package.json`, npm lockfiles, Node container stages, or `node_modules`;
- bridge-client dependencies or source adapters;
- old schema/runtime directories;
- duplicate Compose files for old/new paths;
- database/table names carrying generation suffixes; or
- default commands that do anything except direct-P2P Go ingestion.

## 4. Product boundaries

### 4.1 Clicksync owns

- N2N handshake, KeepAlive, ChainSync, and BlockFetch client behavior;
- peer selection, health, failover, and corroboration;
- ChainSync intersection and continuous cursor management;
- bounded block-body CBOR decoding in memory;
- era-aware normalization;
- genesis UTxO seeding;
- ClickHouse migrations and direct native inserts;
- append-only adoption/rollback publication;
- writer exclusion and non-authoritative audit handling;
- status and operational metrics; and
- no cumulative block-count, tip, time, or storage stop in the shipped
  ingestion process. A bounded acceptance sample is controlled externally by
  polling committed state and sending `SIGTERM`; shutdown must flush the final
  staged prefix.

Acceptance tests and one-shot experiments stop the otherwise continuous
process from outside Clicksync. The harness polls committed state, sends
`SIGTERM`, and verifies the graceful-shutdown flush; Clicksync has no internal
test-range, block-count, tip, elapsed-time, or capacity stop condition.

### 4.2 Clicksync does not own

- fund tracing, BFS, address exploration, taint heuristics, or reports;
- dashboards or public APIs;
- transaction submission or mempool;
- block production;
- independent wallet/provider APIs;
- a local N2C socket;
- consensus/ledger-state queries;
- script execution;
- rewards, epoch stake, pools, delegation state, ADA pots, or enacted
  governance state; or
- off-chain metadata fetching.

### 4.3 Clickout owns

- UTxO, transaction, address, datum, redeemer, withdrawal, and metadata reads;
- snapshot-pinned forward/reverse traversal;
- bounded fund-origin queries;
- optional clearly named taint/allocation heuristics; and
- output rendering/decoding for analysts.

## 5. Trust model

### Architecture selection: reuse protocol machinery, not cardano-db-sync

The selected implementation is a clean Go indexer, not a fork of
`cardano-db-sync` and not a from-scratch Ouroboros implementation.

`cardano-db-sync` remains a useful semantic reference, but it is the wrong
runtime boundary for this product. Its documented architecture:

- connects to a locally running `cardano-node` through a Unix socket;
- maintains ledger state while following the chain;
- writes a highly normalized PostgreSQL database; and
- covers substantially more Cardano state than historical UTxO-flow
  analysis needs.

Replacing only its PostgreSQL layer with ClickHouse would therefore retain
the local-node and Haskell ledger/runtime dependency graph while also
invalidating storage assumptions built around relational updates,
foreign-key relationships, and transaction-oriented rollback handling. A
partial fork that removes those layers would be a larger and harder-to-audit
rewrite than this narrow service.

The parts of db-sync's model that remain applicable are treated as behavioral
requirements rather than copied runtime layers:

- era-aware transaction decoding;
- deterministic genesis UTxO creation;
- ordered chain adoption and rollback;
- preservation of datum, metadata, withdrawals, and redeemer context; and
- resolution of redeemer indices to their semantic transaction targets.

Implementing the entire Ouroboros transport and hard-fork codecs from scratch
would remove a dependency at the cost of creating a new unaudited networking
and serialization stack. That is not a sensible trade for an analytics
indexer. Clicksync instead pins gOuroboros behind a small internal adapter and
tests its behavior against real era blocks and live public relays. Clicksync
itself owns peer supervision, continuity checks, normalization, publication,
and rollback facts.

This boundary deliberately changes the assurance level. A local
`cardano-node` plus ledger validation can establish stronger chain validity;
the selected lightweight path observes and structurally verifies remote
peers. The dataset must expose that distinction rather than imply equivalent
consensus validation.

Decision matrix:

| Candidate | Local node | Runtime/storage baggage | Protocol risk | Fit |
|---|---:|---|---|---|
| Fork db-sync and swap PostgreSQL | Required by inherited architecture | Haskell ledger/db-sync stack and broad schema | Low transport risk, high invasive-fork risk | Rejected |
| Reimplement Ouroboros in Clicksync | No | Small nominal dependency graph | Unacceptably high mux/codec/era risk | Rejected |
| Go Clicksync with pinned gOuroboros adapter | No | One ingestion binary plus ClickHouse | Contained by pinning, fixtures, and live gates | Selected |

References:

- <https://github.com/IntersectMBO/cardano-db-sync>
- <https://github.com/blinklabs-io/gouroboros/releases/tag/v0.189.1>
- <https://github.com/blinklabs-io/gouroboros/blob/v0.189.1/protocol/chainsync/client.go>

### D-001: Peer-observed, not independently consensus-validated

Accepted.

N2N ChainSync reproduces a remote peer's candidate chain. It does not perform
chain selection. Block/header/body/transaction/datum verification detects
corruption and decoder mistakes, but it does not prove ledger or consensus
validity.

Every dataset and status response uses:

```text
peer_observed_structurally_verified
```

It must never claim `trustless`, `consensus_validated`, or
`ledger_validated`.

Mitigations:

- independently operated bootstrap relays;
- stable checkpoint agreement from at least two operators;
- recent-tip and unexpected/deep-rollback corroboration;
- peer/version/checkpoint provenance persisted in ClickHouse;
- known hash-pinned network/genesis configuration;
- fail-closed disagreement clamping plus malformed-peer quarantine; and
- exact completeness/trust fields available to Clickout.

Independent trustlessness would require complete Cardano consensus and ledger
validation and is deliberately outside this lightweight indexer's scope.

References:

- <https://developers.cardano.org/docs/developers/curriculum/production/network-protocol/>
- <https://ouroboros-network.cardano.intersectmbo.org/pdfs/network-spec/network-spec.pdf>

### D-002: Pinned gOuroboros networking and decoding

Accepted, subject to live compatibility gates.

Pin:

- module: `github.com/blinklabs-io/gouroboros`;
- release: `v0.189.1`;
- commit: `9293adc2c94e390ca0545e870e5272ab9ac969fa`;
- module checksum:
  `h1:JIPi37b45LQL3e0Kvit5s/OSWjcB/V7WEI9bZ/T27Tc=`;
- `go.mod` checksum:
  `h1:n1NCfwdivyiavSvEa+hZoNWWSUNc3m6Lqw4WH16GQbQ=`.

This 0.x dependency remains isolated behind internal adapter interfaces. The
build uses a digest-pinned Go toolchain image. The release's
Byron-through-current-Conway and N2N-v15 claims must be proven by
fixtures/live traffic rather than repeated from its README. A future-era
fixture proves only the documented fail-closed boundary unless Clicksync
actually implements that era's semantics.

The production BlockFetch configuration keeps
`SkipBlockValidation=false`. In the pinned dependency this causes each
decoded block to recompute and compare its body hash before the adapter can
label it structurally verified. The rule applies identically to a one-block
request and a streamed range. A corrupt-body fixture must be rejected by the
range path; merely matching the requested point or successfully decoding
CBOR is insufficient. This is structural integrity only and does not change
the peer-observed trust boundary in D-001.

The final dependency audit includes:

- `go mod graph`;
- module checksums;
- builder/runtime image digests;
- SBOM;
- license inventory;
- `govulncheck`;
- `go version -m` on the shipped binary; and
- an explanation of any validation/execution dependency pulled transitively.

No mux, handshake, or hard-fork wire codec is reimplemented from scratch.

## 6. Direct P2P synchronization

### D-003: One Go process speaks the complete required N2N bundle

Accepted.

One TCP bearer multiplexes:

- handshake;
- KeepAlive;
- ChainSync client;
- BlockFetch client; and
- optional PeerSharing discovery.

Transaction submission and inbound responder/listener behavior remain
disabled. The service makes outbound connections only.

For each selected roll-forward:

1. receive and decode the header/point;
2. request the block body by point or bounded inclusive range;
3. reject `NoBlocks`, wrong point, wrong network, body/header commitment
   mismatch, parent discontinuity, or impossible height/slot ordering;
4. decode the era-specific body in bounded memory;
5. recompute transaction IDs and relevant content hashes;
6. normalize accepted facts;
7. publish facts, then the adoption commit event;
8. update the committed tip/event metadata;
9. release all block/transaction CBOR bytes; and
10. request/acknowledge more work only when the bounded publication window has
    capacity.

Origin backfill does not issue one BlockFetch request per historical header.
The source accumulates a bounded sequential header window, requests one
inclusive BlockFetch range, and streams each returned body through the same
header/body checks and bounded publication queue. The window flushes when it
reaches 32 headers or the callback header equals the advertised tip; the
one-block case is the normal live-tip fallback.

The ChainSync callback that starts a range remains blocked until the expected
bodies and `BatchDone` arrive. Consequently a subsequent ChainSync rollback
cannot overtake an in-flight body range. Returned body count, order, points,
and heights must exactly match the buffered headers; `NoBlocks`, partial,
extra, reordered, or mismatched batches fail closed. Only a bounded number of
decoded bodies may wait behind ClickHouse backpressure.

Byron epoch-boundary blocks are part of chain continuity even when they
contain no transaction facts.

### D-004: Continuous ChainSync is required

Accepted.

The former one-tip probe is not sufficient. Production sync must:

- start at Origin for complete history;
- continuously request next headers through `Await`;
- fetch every body selected for the dataset;
- process live roll-forwards and rollbacks;
- reconnect classified transport/corroboration unavailability indefinitely
  with capped exponential backoff, peer rotation, and prompt shutdown
  cancellation—there is no arbitrary retry-count ceiling or busy loop;
- resume using ClickHouse-derived intersections;
- preserve dense recent intersection points plus geometrically spaced
  historical candidates and Origin;
- reconcile local committed tip with the peer intersection;
- handle re-adoption without duplicating visible facts; and
- run continuously until shutdown or a real dependency/validation failure;
  test orchestration bounds samples externally and verifies graceful final
  microbatch publication.

Only deterministic malformed or contradictory protocol data quarantines an
operator. An unavailable exact range, `NoBlocks`, a short response caused by
a chain race, DNS/TCP/EOF/timeout failure, or insufficient reachable
corroborators is availability evidence, not proof of bad data. Clicksync
rotates/reprobes and retries those conditions indefinitely with a capped time
backoff and prompt context cancellation; neither repeated identical responses
nor any attempt count promotes them to permanent quarantine. If a
deterministic data-integrity quarantine leaves fewer independent operators
than the configured threshold, Clicksync stops fail-closed. That integrity
failure is distinct from retryable network unavailability.

An empty recent-tail demonstration may use a two-peer-recognized point, but
the manifest and every query must show `complete_history=false`. It is never
called a complete current UTxO set.

#### Restart intersection algorithm

The pinned gOuroboros `Client.Sync` waits for `IntersectFound`, but its public
return value does not expose the peer-selected point. Passing many candidates
to `Sync` is therefore insufficient for reconciling the local committed
branch.

Clicksync uses this exact sequence:

1. load only locally committed/adopted points, newest first, using a dense
   recent tail plus geometrically spaced older points;
2. call `Client.GetAvailableBlockRange` with exactly one candidate at a time;
3. treat `nil` as proof that the input candidate is on the peer's current
   chain and `chainsync.ErrIntersectNotFound` as rejection;
4. select the first accepted input candidate, or Origin when none is accepted;
5. append and commit any required rollback from the local tip through the
   selected point before publishing new branch facts; and
6. call `Client.Sync` again with exactly the selected point to begin normal
   callbacks.

The range helper's returned `start` is deliberately ignored for intersection
identity. On a non-tip match, gOuroboros internally requests one item and
returns the first block *after* the intersection; at the exact peer tip it
returns an empty range with `nil`. During this probe the library diverts that
first roll-forward away from the configured application callback. The
subsequent singleton `Sync` performs a new `FindIntersect`, so the probed block
is fetched and published normally rather than skipped.

Required tests cover an accepted tip, a rejected point, descending candidate
selection, Origin fallback, zero publication during probing, and a live
disconnect/reconnect that resumes from the expected stored candidate.

Candidate generation itself is a bounded query path. It uses the committed
tip plus a bounded recent/event-sequence sample and geometrically older event
targets, then resolves membership only for those candidate publications. It
must not sort the hash-ordered `blocks` table by height or materialize the
global active history. A manifest cache may supply safe hints, but restart
must still recover when that cache lags a committed adoption/rollback.
`EXPLAIN`/`read_rows` evidence and a crash-with-stale-descendants fixture are
required.

Every non-Origin candidate carries its stored block number in Clicksync's
internal transport contract even though the wire `Point` contains only
slot/hash. After intersection, the first fetched header must extend that
height, whether it is a Byron epoch-boundary block (EBB), and every subsequent
header must extend its predecessor.

Candidates are probed one at a time and only the selected singleton is passed
to `FindIntersect` for following. Therefore a valid immediate Byron
successor/EBB fallback pair is retained even though both points share a slot:
newest-to-oldest it must be exactly a non-EBB at height `N + 1`, followed by
an EBB at height `N`. Every other equal-slot pair is rejected. This exception
is necessary when a partial dataset starts at the EBB: if the committed
successor is absent from a peer's selected chain, Clicksync must still be able
to select and reconcile to its exact EBB boundary rather than discard the
boundary or widen to Origin. Tests prove the pair is never transmitted as a
multi-point candidate list. Across different slots, equal-height candidates
are legal only for an EBB followed by its non-EBB predecessor; all other
equal-height candidate shapes fail closed.

The mainnet rule follows the Intersect consensus/storage specification rather
than the pinned library's more permissive generic/testnet envelope check:

- an EBB has the same block number as its predecessor and a strictly later
  slot;
- every non-EBB has block number `previous + 1`;
- slots strictly increase except that a non-EBB immediately following an EBB
  may share the EBB's slot; and
- Origin's first block is the only predecessor-free case.

The EBB identity is derived only from a structurally verified decoded block
type or its committed stored fact. It is carried durably through external
start metadata, committed tips, intersection candidates, and exact rollback
target resolution; it is never guessed from slot/height. Fixtures cover the
Origin EBB, EBB same-height, EBB-to-main equal/later slot, restart/rollback on
an EBB, same-slot successor rejection followed by EBB-boundary acceptance,
and rejection of EBB `+1`, arbitrary height gaps, regular equal height, and
all other equal-slot candidate/chain transitions. Parent hash and point/body
hash are independently checked; height is never inferred from slot.

Constructed blocks at the official epoch-zero point prove only
publication/manifest logic. Release evidence separately performs
validation-enabled N2N BlockFetch of the real mainnet epoch-zero EBB and its
immediate main-block successor (plus the epoch-one transition when available),
then pins test-only raw hashes and decoded point/height/parent metadata.
Runtime tables retain none of those block CBOR bytes.

Primary references:

- <https://ouroboros-consensus.cardano.intersectmbo.org/pdfs/report.pdf>
- <https://ouroboros-consensus.cardano.intersectmbo.org/haddocks/ouroboros-consensus/Ouroboros-Consensus-Storage-ImmutableDB-API.html>

Origin is appended only for an Origin dataset. An intersection-start
candidate list terminates at its exact verified external boundary. If every
candidate down to that boundary is rejected, ingestion stops fail-closed; it
does not quarantine peers merely for reporting valid negative membership. It
must never probe/select Origin and silently cross the declared partial-history
scope.

#### Partial-history start boundary

A Cardano ChainSync point contains a slot and hash but not its block number;
Clicksync never substitutes the slot for height. Before creating a new
intersection-start manifest, it BlockFetches that exact boundary from a
corroborated peer with structural block validation enabled, verifies the
returned slot/hash, and persists the decoded block number. Failure to fetch or
verify the boundary aborts initialization.

The shipped sample configuration uses the independently corroborated,
post-van-Rossem/PV11 point
`193253841:e98663bea810a45b59bf2783e40dbd2c69f79e1594b4cd0e160646a3f587eb13`.
It is a runnable partial-history example, not a hidden completeness claim.
The sample contains no user-entered height; Clicksync derives block
`13715435` through the verified boundary fetch. Complete history is an
explicit `CLICKSYNC_START=origin` choice with no start point.

Thereafter a rollback target must resolve either to that exact stored boundary
or to a committed block already present in the dataset. A rollback below or
outside an intersection-start dataset is quarantined and stops ingestion
instead of fabricating a height or silently widening/changing the dataset.
Origin remains the only boundary with null slot/hash/block number.

ChainSync `RollBackward` also supplies only slot/hash. Before completing a
non-Origin rollback callback or accepting another header, Clicksync performs
one validation-enabled singleton BlockFetch of that exact target on the
selected-peer connection. It requires exactly one body with the requested
slot/hash, derives the block number, and installs a height-bearing parent.
Fetching the target does not make an unknown rollback permissible: the store
must still prove that it is the exact external boundary or a locally committed
point before committing the rollback. A chain-race `NoBlocks`/short response
causes peer rotation, fresh re-intersection, and indefinite capped-backoff
retry. Repetition alone never turns availability into a peer-data violation.
A wrong body, point, order, height, or other deterministic structural mismatch
does. Origin is the only rollback target that does not require this height
resolution.

Pinned-source reference:
<https://github.com/blinklabs-io/gouroboros/blob/v0.189.1/protocol/chainsync/client.go>

### D-005: Peer corroboration and provenance are persisted

Accepted.

Initial documented mainnet seeds:

- `backbone.cardano.iog.io:3001`;
- `backbone.mainnet.cardanofoundation.org:3001`;
- the officially documented EMURGO name, currently unhealthy and therefore
  not counted until it resolves and passes handshake.

The 2026-07-23 proof observed the first two operators agreeing over N2N v15;
the documented EMURGO name returned DNS `NXDOMAIN`.

Persist narrow peer observations:

- peer hostname/resolved address/operator label;
- negotiated N2N version and network magic;
- observed tip/checkpoint point and height;
- observation time;
- agreement group/checkpoint identifier;
- selected body source;
- structural verification result; and
- disagreement/quarantine reason.

Do not write one provenance row per peer per block during a full backfill
unless measurement justifies it. Persist stable sampled checkpoints, every
live rollback, every disputed point, and negative follow/range availability
diagnostics. A successful reconnect/source selection is not duplicated as a
`peer_observations` row: each committed block already carries the actual
source hostname, resolved address, normalized operator label, negotiated N2N
version, and network magic. Thus source changes remain reconstructable from
the block stream without a redundant success-diagnostic table path.

Failure classes are explicit. DNS/TCP/EOF/timeout failures are retryable
transport failures. Local normalization, publication, storage, and invariant
failures are terminal. A body-hash failure, wrong point/order/height, extra
body, wrong network, or impossible protocol message is a peer-data violation:
persist the exact host/resolved-address/operator evidence, quarantine that
operator for the run, and continue only if the remaining independent
operators still meet the configured corroboration threshold. With the
two-operator default, one quarantine therefore stops ingestion.

Valid negative membership is not malformed data. An operator that rejects an
exact checkpoint, rollback target, or branch-tip point contributes
disagreement evidence; that can move branch selection or place the manifest
in a clamped/disputed state, but it never permanently excludes the operator.
Only an accepted response whose bytes/shape contradict the requested
protocol object, or another deterministic protocol/data violation, is
quarantined.

`NoBlocks` or a short range can result from a chain change between ChainSync
and BlockFetch. Record it, rotate/reconnect, re-intersect, and retry
indefinitely with capped time backoff. Repeated failure for the same exact
point remains availability evidence and is never an attempt-count quarantine.
An unclassifiable upstream error fails terminal rather than being silently
relabeled as a transient transport failure.

### D-006: Unknown or unsupported UTxO semantics fail closed

Accepted.

Current mainnet Conway, including the van Rossem protocol-version-11 intra-era
hard fork enacted at slot `192844800` on 2026-07-18, must decode in a live
post-boundary test. Dijkstra remains a future era as of this decision. A real
Dijkstra fixture must prove decoding reaches normalization and that nested or
otherwise unimplemented transaction semantics fail before publication.
Dijkstra feature completeness is not claimed or required for the current
mainnet goal. If the selected upstream API cannot expose any new transaction
form losslessly, Clicksync stops before publishing that block and reports an
unsupported-era error. It never drops an unknown child transaction and
continues.

Current-era references:

- <https://intersectmbo.org/news/cardano-upgrade-van-rossem-hard-fork>
- <https://docs.cardano.org/about-cardano/evolution/eras-and-phases>

## 7. Exact transaction and UTxO scope

### 7.1 Transaction identity

Retain for every transaction occurrence:

- block publication reference;
- transaction hash and transaction order;
- optional parent transaction hash and subtransaction index;
- era;
- phase-2 validity;
- regular/collateral/genesis flow kind;
- declared fee and ledger-effective fee where distinguishable;
- effective mint/burn deltas;
- presence/count summaries for inputs, outputs, withdrawals, redeemers,
  metadata, and datum observations.

Do not retain the full serialized transaction.

For a phase-2-invalid transaction the effective ADA fee/collateral sink is the
sum of resolved consumed collateral inputs minus the produced collateral
return. A positive decoded `total_collateral` value is retained and
cross-checked, but zero is not treated as proof of a zero fee: the pinned
decoder represents an absent optional Babbage/Conway field as a non-null zero.
If any consumed source output is outside a partial-history dataset, Clickout
reports the sink as unresolved/incomplete instead of inventing a value.

### 7.2 Inputs

Retain every UTxO reference in the transaction body with a role:

- regular;
- collateral; or
- reference.

Also retain:

- body ordinal;
- whether it is ledger-consumed;
- consuming transaction;
- containing publication; and
- whether its referenced output is resolved in this dataset.

Fund traversal uses only `is_consumed=true`. Reference inputs and inactive
regular/collateral references remain available for dApp context but never
become value-flow edges.

Valid transaction:

- regular inputs are consumed;
- collateral/reference inputs are not consumed.

Phase-2-invalid transaction:

- collateral inputs are consumed;
- regular/reference inputs are not consumed.

### 7.3 Outputs

Retain ledger-produced outputs only:

- UTxO transaction hash/index;
- containing publication and block height;
- regular/collateral-return/genesis kind;
- raw address bytes;
- lovelace;
- sorted native-asset policy/name/quantity arrays;
- datum kind and datum hash;
- reference-script hash and language when derivable; and
- no script body.

For a valid transaction, ordinary outputs are produced. For an invalid
transaction, only its collateral return is produced when present.

### 7.4 Mint and burn

Retain effective non-zero signed native-asset deltas on the transaction:

- policy ID: raw 28 bytes;
- asset name: raw 0-to-32 bytes;
- quantity: signed 64-bit, with explicit overflow rejection.

Invalid-transaction mint is not ledger-effective and is marked unapplied or
omitted from effective deltas. Clickout renders mint as an asset source and
burn as an asset sink.

### 7.5 Datums

Retain:

- output datum kind: none/hash/inline;
- datum hash;
- exact inline datum CBOR fragment;
- exact witness datum CBOR fragments; and
- publication-scoped observation provenance.

Store one hash-verified body in `datum_bodies` and narrow observations in
`datum_observations`. A hash-only output may remain unresolved. A verified
body first seen on an orphan branch may resolve the cryptographic content, but
Clickout must separately report whether the datum was observed on the selected
active chain.

### 7.6 Withdrawals

Retain one row per declared withdrawal:

- transaction/publication;
- raw reward-account bytes;
- lovelace amount;
- deterministic body ordinal;
- `is_applied`; and
- resolved reward credential/script hash when derivable.

Valid transaction withdrawals are applied. Phase-2-invalid transaction
withdrawals remain transaction context but are not an effective ADA source.
Clickout emits applied withdrawals as explicit external ADA sources.

No reward history, reward calculation, stake snapshots, or delegation state is
stored.

### 7.7 Redeemers and resolved purposes

Retain every redeemer, including those in phase-2-invalid transactions:

- transaction/publication;
- raw purpose tag and normalized purpose;
- redeemer index;
- exact redeemer Plutus-Data CBOR fragment;
- execution-unit memory and steps;
- `is_applied`;
- resolution status; and
- resolved target identity.

Resolved targets:

- spend: source transaction hash/output index;
- mint: policy ID;
- reward: withdrawal reward account;
- certificate: body ordinal plus a purpose-tagged digest of the exact target
  certificate and any derivable script credential;
- vote: body ordinal plus the voter constructor/credential identity;
- proposal: body ordinal plus a purpose-tagged digest of the exact target
  proposal procedure and any derivable policy script;
- future purpose: fail closed until explicitly modeled.

Store the resolved script/credential hash when it can be derived without
retaining script bytes. Full source-output/address/datum context is joined by
Clickout through the resolved UTxO reference rather than duplicated in the
redeemer row. Certificate/proposal target bytes are hashed during
normalization and then discarded; Clicksync does not persist their raw CBOR or
governance state.

The Clickout transaction/context response attaches each resolved
regular/collateral/reference source output from a batched point lookup rather
than discarding it after setting a boolean. Inline/source datum bodies are
attached through a bounded batched hash lookup when present; an unresolved
body or pre-boundary source remains explicit.

“Resolved” means mapped from the ledger's `(purpose,index)` pointer to its
actual transaction target. Merely storing the numeric pair is not accepted.
Resolution follows the ledger ordering for each purpose, not Go map iteration
or the decoder's incidental wire order. In particular, spend targets use a
transaction-ID/output-index ordered view of the input set while the separately
stored input `body_ordinal` remains unchanged. Mint policies, withdrawals,
voters, certificates, and proposals each receive ordering-oracle fixtures
whose encoded/map order differs from their ledger pointer order.

Withdrawal pointers follow `Map AccountAddress` ordering: network, then the
script-before-key `Credential` constructor order, then credential hash. Raw
reward-account byte ordering is not equivalent and must not be used.

The voter ordering oracle is the ledger implementation's derived `Ord`:
`Voter` orders committee, DRep, then stake-pool constructors, while
`Credential` orders script then key constructors. This deliberately differs
from the Conway wire tags. Primary references:

- <https://cardano-api.cardano.intersectmbo.org/cardano-api/Cardano-Api-Ledger.html>
- <https://cardano-ledger.cardano.intersectmbo.org/cardano-ledger-api/Cardano-Ledger-Api-Governance.html>

### 7.8 Transaction metadata

Retain on-chain transaction metadata, not an entire auxiliary-data envelope:

- transaction/publication;
- sorted top-level metadata labels;
- exact metadata-map CBOR fragment; and
- byte length/content hash for integrity and accounting.

Auxiliary scripts are removed before persistence. Clickout may decode the
metadata fragment for display. No off-chain URL fetch is performed.

Metadata is retained for valid and phase-2-invalid transactions because it is
part of the observed transaction, with transaction validity still explicit.

### 7.9 Compact Plutus context retained without script bodies

For normal dApp analysis also retain:

- reference input UTxO references;
- datum hashes/bodies as above;
- output reference-script hash/language;
- redeemer purpose target and execution units;
- transaction validity and fees;
- native assets/mint; and
- withdrawals/metadata.

Do not retain:

- validator/minting/native/reference script CBOR;
- full witness sets;
- signatures;
- raw block CBOR;
- raw transaction CBOR;
- full auxiliary-data CBOR containing scripts;
- certificates except the minimal target ordinal needed to resolve a
  redeemer;
- governance proposal/vote bodies;
- stake/reward/pool/epoch/treasury state; or
- Plutus execution traces.

### D-007: CBOR fragment policy

Accepted.

The only persisted CBOR is content explicitly required by the user:

- datum Plutus Data;
- redeemer Plutus Data; and
- the transaction metadata map.

These fragments are not full block/transaction/script CBOR. Each is
length-bounded and tested. Raw incoming block bytes are never written to disk,
logs, error evidence, or dead-letter files.

## 8. ClickHouse schema

### D-008: One database and one schema

Accepted.

The implementation destroys/replaces the old schema and uses the configured
database name `clicksync` by default. There is no generation suffix in
database, table, directory, CLI, Compose, environment, or type names.

Normal ordered migration files may exist, but they evolve the sole schema.
There is no compatibility migration from the deleted prototype database.
Operators re-create the experimental database.

Logical tables:

1. `dataset_manifest`
   - network/genesis identity;
   - trust/completeness/start point;
   - committed tip/event sequence;
   - writer/source build identity. Release/evidence images inject the exact
     frozen source commit or content identifier; the placeholder
     `development` value is never accepted for a live proof.
2. `blocks`
   - immutable block publication identity, point, parent, height, era,
     transaction count, source peer, synthetic flag.
3. `chain_events`
   - append-only block adoption/invalidation events.
4. `rollbacks`
   - rollback commit header, point, depth, peers, reason, writer, time.
5. `transactions`
   - compact transaction identity/effect/fee/mint/count fields.
6. `inputs`
   - regular/collateral/reference input facts and consumption status.
7. `outputs`
   - effective UTxO facts, assets, datum reference, reference-script hash.
8. `datum_bodies`
   - one exact body per datum hash.
9. `datum_observations`
   - publication/transaction/source-kind provenance.
10. `withdrawals`
    - reward accounts, amounts, applied state, credential hash.
11. `redeemers`
    - Plutus Data, execution units, applied state, resolved purpose target.
12. `transaction_metadata`
    - labels and metadata-map CBOR fragment.
13. `peer_observations`
    - checkpoint/source/corroboration/disagreement provenance.
14. `writer_audit`
    - current ingestion ownership and heartbeat metadata.

Physical principles:

- one database per network;
- raw `FixedString` hashes/policy IDs through the native Go driver;
- raw address/reward-account/asset-name/CBOR bytes as binary `String`;
- no hex doubling in storage;
- sorted parallel asset/mint arrays with equal-length constraints;
- coarse sealed block-height partitions;
- primary ordering for block, transaction, source-UTxO, producing-transaction,
  datum-hash, and transaction-metadata access;
- dual input-edge access: the base `inputs` order serves
  `(source_tx_hash, source_output_index)` spender lookup, while a measured
  lightweight projection ordered by `(tx_hash, publication_id, body_ordinal)`
  serves consuming-transaction context and reverse BFS;
- measured projections only after `EXPLAIN indexes=1`;
- ZSTD codecs on variable payloads;
- no LowCardinality on high-cardinality hashes/addresses;
- no input-by-output Cartesian edge table; and
- no global `FINAL` query as the normal point/BFS path.

## 9. Publication, rollback, and writer safety

### D-009: Facts first, adoption last

Accepted.

Logical publication remains one immutable fact bundle and one ordered event
per block. Physical ClickHouse insertion uses bounded multi-block
microbatches; emitting roughly ten independent parts per historical block is
not a viable Origin backfill.

For one microbatch:

1. normalize sequential blocks and reserve one publication/event identity for
   each under the writer lock;
2. resolve sources against the committed snapshot and all earlier staged
   outputs, using candidate-scoped publication membership rather than a
   whole-history active-set scan;
3. cap staged normalized bytes, fact rows, block count, and wall-clock age;
4. insert each fact table once for all staged publication IDs;
5. validate exact per-publication row counts/content digests;
6. insert the ordered adoption rows in one final ClickHouse insert;
7. read back the complete expected event set after any lost insert response
   and fail fatally on a partial/conflicting set;
8. update the manifest cache to the last committed event; and
9. release the normalized batch and continue ChainSync.

Initial safe caps are at most 256 blocks, 32 MiB of normalized variable
payload, and one second of batch age. A lower row/byte cap wins. The
implementation may lower these values from measured memory/server limits but
may not make any dimension unbounded.

Staging maintains row and encoded-byte counters incrementally; it does not
re-encode the entire growing batch for every incoming block. Each normalized
item is sized once, while `PublishBatch` retains one whole-batch defensive
check. Rollback-prefix retention recomputes counters only for the retained
prefix. This keeps batching work linear rather than quadratic at the 256-block
or 32-MiB boundary.

At the live tip, the age timer flushes a short batch. Backpressure stops
accepting more decoded bodies when a batch is full or being written. A clean
shutdown flushes only a fully verified pending prefix; an abrupt exit loses
only uncommitted memory and the peer replays it from the ClickHouse-derived
intersection.

If ChainSync rolls back into a pending batch, staged descendants are discarded
and the retained prefix is reconciled before new-branch blocks are accepted.
If it rolls back at or below the committed tip, all pending descendants are
discarded before the append-only committed rollback is processed. A pending
block is never exposed through the manifest or Clickout.

When the exact rollback target is already the authoritative committed tip,
there are no active descendants to invalidate. Clicksync still appends a
corroborated depth-zero rollback header, with `old_tip = rollback_to =`
the current tip, after discarding the pending batch. This records the observed
rollback instead of falsely acknowledging an unrecorded event. Empty
descendants are rejected for every non-exact, unknown, or inactive target.

A flush may make an earlier staged prefix authoritative while the current
callback remains unaccepted or a subsequent rollback-header insert fails.
The handler must return that exact committed prefix and its typed tail even
with the terminal error; the supervisor counts/corroborates it but does not
acknowledge the unaccepted callback or rollback. A successful rollback that
first publishes a retained pending prefix reports both the committed block
prefix and the separately authoritative rollback action. Fault tests cover
adoption committed plus rollback pre-commit failure as well as the fully
successful two-commit path.

Facts without an adoption event are invisible. The selected implementation
always allocates a fresh immutable publication for a retried or re-adopted
block; it does not try to prove and reactivate an older bundle. The older
publication remains inactive, so Clickout sees exactly one active copy. This
costs a small amount of append-only storage on an actual fork but removes a
complex bundle-reuse decision from the commit path.

Allocation after startup is based on the maximum identity present in all raw
fact/event tables, including uncommitted attempts, not only the manifest or
largest committed snapshot. A retry must not reuse an orphan
`publication_id`, `event_seq`, or rollback ID. This prevents a later commit
record from accidentally making rows from an interrupted attempt visible.

Content-addressed datum bodies are the exception to branch visibility, not to
integrity. The single writer verifies hash, length, and byte equality before
deduplicating a body already present. Physical duplicate rows after a crash
must be query-equivalent, and `first_publication_id`/`first_seen_at` are read
with minimum/`argMin` semantics; a `ReplacingMergeTree` version column must not
be claimed to preserve the smallest first-seen value.

### D-010: Rollback is append-only and committed by its header

Accepted.

For a rollback:

1. capture the latest committed event snapshot;
2. resolve exact active descendants server-side;
3. enforce configured depth and corroboration policy;
4. insert inactive membership events with one rollback ID;
5. insert the rollback header last;
6. update the committed tip; and
7. resume intersection/fetch.

Membership rows without the header are inert. Clicksync has no disk
high-water publication pause; rollback processing and fact publication use
the same unbounded runtime policy.

For the exact-tip depth-zero case, step 4 emits no membership rows, while the
header remains the authoritative append-only event and advances the snapshot
and manifest without changing the tip. Lost-response readback, restart
reconciliation, Origin/partial-boundary handling, and Clickout snapshot logic
must treat this as a real rollback event with no membership delta.

Re-adoption publishes a fresh fact bundle and appends its newer active event;
the previously invalidated publication stays inactive. A rolled-back spend
disappears and therefore resurrects its source UTxO.

### D-011: Enforced single-host writer gate

Accepted after proving that a MergeTree row cannot provide atomic
compare-and-swap or fencing.

The supported deployment is explicitly single-host/single-writer. Clicksync
holds an operating-system advisory `flock` for its entire process lifetime on
a small shared `clicksync-state` volume. Compose mounts the same lock path into
every supported writer and declares exactly one replica. The `writer_audit`
ClickHouse table is audit/heartbeat state only; documentation and code must
never claim it provides the lock.

The implementation must prove:

- two simultaneous processes sharing the state volume cannot both acquire;
- the loser fails before allocating a sequence or writing a fact;
- owner/build/start/heartbeat are visible in ClickHouse;
- graceful exit releases the lock;
- process death releases the lock automatically;
- restart acquires without a manual stale takeover; and
- lock-path/configuration errors fail closed.

Multi-host or separately mounted writer instances are unsupported. They must
not be enabled by a flag that pretends to be safe. A future multi-host design
requires a real external coordinator with fencing, such as a correctly
configured Keeper/etcd deployment, and a new reviewed decision.

### D-012: Snapshot semantics

Accepted.

The authoritative snapshot is the largest committed adoption/rollback event,
not the largest allocated number. Manifest state is a cache and is reconciled
against committed events after restart.

Every Clickout traversal captures one snapshot and threads it through every
frontier/page. It observes one side of a rollback only.

Two independently bounded physical access paths are required:

1. publication membership by `(publication_id, event_seq)` for a supplied
   candidate publication set; and
2. committed event order by `event_seq` for acquiring or validating a
   snapshot.

The first remains the base `chain_events` ordering. The implementation may
serve the second with a compact `event_seq`-ordered projection only if
`EXPLAIN indexes=1` and `system.query_log.read_rows` prove that ClickHouse
actually selects it. If that proof fails, publication changes to an explicit
event-sequence-ordered adoption-header table: fact and membership rows are
inserted first, the adoption header is the commit record inserted last, and
rollback headers retain the same last-write rule. A manifest-only answer is
not acceptable because it can lag or be lost after a committed insert.

Snapshot correctness must not require materializing the active state of every
historical publication for each point query. Point, transaction, and BFS
paths first use fact-table order keys to obtain a bounded set of candidate
publication IDs, then resolve committed membership only for those IDs at the
pinned snapshot. Frontier membership checks are batched. The corresponding
`chain_events` access is ordered by `(publication_id, event_seq)`.

A global `GROUP BY publication_id` over the whole chain history is permitted
for offline audit/reconciliation only, never as the normal Clickout point/BFS
or writer input-resolution path. `EXPLAIN indexes=1` and query `read_rows`
evidence must show that increasing unrelated chain history does not make a
one-UTxO lookup scan that history. The same unrelated-history scale test
applies to snapshot acquisition and pinned-event validation.

## 10. Genesis

### D-013: Complete history requires deterministic genesis outputs

Accepted.

ChainSync does not emit genesis distributions. Origin mode must:

- use bundled, hash-pinned official Byron/Shelley genesis files rather than
  fetching mutable configuration during normal sync;
- verify configured hashes;
- synthesize ledger-compatible genesis transaction/output references;
- cover Byron distribution/redemption and Shelley `initialFunds` where used;
- mark genesis facts synthetic and permanently active; and
- match trusted first-spend and total-supply fixtures.

Without this, the manifest remains `complete_history=false`.

The pinned mainnet identity and fixture invariants are:

| Item | Required value |
|---|---|
| network magic | `764824073` |
| official Byron semantic genesis ID | `5f20df933584822601f9e3f8c024eb5eb252fe8cefb24d1317dc3d432e940ebb` |
| exact downloaded Byron JSON BLAKE2b-256 | `dbbdaeab0ea4ea58225892d8b1294f178b417f4a9d1ed3bbf629c40d8f74e86b` |
| Byron AVVM entries | `14,505` |
| Byron non-AVVM entries | `0` |
| Byron seeded supply | `31,112,484,745,000,000` lovelace |
| official/raw Shelley genesis ID | `1a3be38bcbb7911969283716ad7aa550250226b76a61fc51cc9a9a35d9276d81` |
| Shelley `initialFunds` entries/supply | `0` / `0` lovelace |
| Shelley maximum supply | `45,000,000,000,000,000` lovelace |

The shipped configuration is mainnet-only. It fails closed unless the network
name/magic and all four semantic/raw genesis identities match this tuple.
Supporting another network later requires a complete, separately pinned
genesis tuple and fixtures; changing only `CARDANO_NETWORK_MAGIC` is never
enough.

The Byron ID in the official node configuration is a semantic/canonical
genesis identifier; it is not the hash of the downloaded JSON byte stream.
Clicksync records both identities and must not compare the raw-file digest to
the semantic ID. Shelley happens to have equal official and raw-file values;
the code still treats their meanings separately.

Each genesis UTxO uses the ledger-compatible reference
`BLAKE2b-256(address_bytes)#0`. Map iteration order is normalized before
publication so the synthetic fact digest is deterministic. A successful
Origin seed is idempotent: restart either proves the complete committed
synthetic bundle identical or fails closed. It never creates a second visible
genesis distribution.

Primary references:

- <https://book.world.dev.cardano.org/environments/mainnet/config.json>
- <https://book.world.dev.cardano.org/environments/mainnet/byron-genesis.json>
- <https://book.world.dev.cardano.org/environments/mainnet/shelley-genesis.json>
- <https://github.com/blinklabs-io/gouroboros/blob/v0.189.1/ledger/byron/genesis.go>
- <https://github.com/blinklabs-io/gouroboros/blob/v0.189.1/ledger/shelley/genesis.go>

## 11. Storage behavior and host safety

### D-014: The 100 GB allocation is a soft planning target

Accepted by explicit user correction on 2026-07-23.

The production contract is storage-unbounded. Clicksync contains none of the
following:

- a project-size, active-data, part-count, or free-space threshold;
- a warning/pause/emergency-reserve state machine;
- an application filesystem walker or disk-budget monitor;
- quota/LVM/filesystem setup;
- a pre- or post-publication capacity gate;
- manifest columns that pretend capacity enforcement exists; or
- a default stop condition based on estimated full-history size.

No compatibility flags or dormant schema fields retain the deleted behavior.
Clicksync keeps fetching and publishing until it is shut down or an actual
fail-closed correctness or non-recoverable ClickHouse error occurs. Transient
peer transport and corroboration unavailability retry indefinitely with capped
backoff and context cancellation. If ClickHouse reports a real write failure,
Clicksync fails without acknowledging the affected publication; it never drops
facts to save space.

The 100 GB supplied for this workspace is only an experimental resource
target. The acceptance harness polls committed state and sends `SIGTERM` after
collecting a useful sample so it does not consume the workspace gratuitously;
Clicksync itself has no cumulative block, tip, time, or storage stop. Graceful
shutdown must publish the final valid staged prefix before exit.

Capacity controls, if an operator wants them, belong outside Clicksync (for
example, a volume selected by the operator). Clicksync neither creates nor
requires such controls. Documentation may report sizing observations and
recommend provisioning; it must not describe those recommendations as
runtime enforcement.

Raw block and transaction CBOR still have zero disk retention because they are
outside the data model, not because of a capacity gate. Logs redact secrets
and payloads. Ordinary ClickHouse/log retention settings may prevent
diagnostic logs from growing needlessly, but they must not prune canonical
UTxO, datum, withdrawal, redeemer, or metadata facts.

### D-015: Full-mainnet size is observed, never gated

Accepted.

The added metadata/redeemer/reference-input scope makes the final footprint
uncertain. The acceptance work therefore ingests stratified era/activity
samples and a contiguous recent range. After merges settle, evidence reports:

- rows and bytes per table/column;
- datum/redeemer/metadata payload distributions;
- asset arrays;
- indexes/projections;
- part counts and merge behavior;
- insert calls, created parts, and blocks/transactions per microbatch;
- ChainSync headers, BlockFetch ranges/bodies, range fill ratio, and source
  throughput;
- logs/temp space; and
- observed bytes per block/transaction/input/output.

Those measurements answer the operator's system-requirement question and show
how far a particular 100 GB workspace is likely to go. They do not authorize,
pause, or prevent synchronization. If a statistically responsible
full-history projection cannot be produced from the bounded evidence, the
documentation says so rather than inventing a fit claim.

### D-016: Shared Docker host isolation

Accepted.

- unique explicit Compose project/network/volume/container labels;
- ClickHouse bound to a nonconflicting localhost port or internal-only;
- no inbound Cardano listener;
- no shipped CPU, memory, PID, storage, or runtime quota; measured resource
  recommendations are documentation, and any operator-selected deployment
  controls remain external to Clicksync;
- bounded diagnostic-log rotation, which never removes canonical analytics
  facts;
- no experimental infinite restart loop;
- no Docker prune/global cleanup/unrelated resource changes;
- pre/post unrelated-container ID/status/restart fingerprint; and
- only explicitly owned resources may be stopped or removed.

## 12. Independent Clickout analysis module

### D-017: Clicksync contains no analytical queries

Accepted.

The ingestion binary offers only operational commands such as:

```text
clicksync migrate
clicksync sync
clicksync status
clicksync peers
clicksync writer
```

It does not expose `trace`, `utxo`, `address`, analytical SQL, or BFS code.

### D-018: Clickout performs bounded graph traversal

Accepted.

Independent commands:

```text
clickout utxo TX_HASH#INDEX [--at tip|BLOCK_HASH]
clickout tx TX_HASH
clickout address ADDRESS --state current|history [--limit N] [--cursor C]
clickout datum DATUM_HASH
clickout redeemers TX_HASH
clickout metadata TX_HASH
clickout withdrawals TX_HASH
clickout trace --direction forward|reverse \
  (--utxo TX_HASH#INDEX|--tx TX_HASH|--address ADDRESS) \
  --max-depth N --max-nodes N [--asset ada|POLICY.NAME] --format jsonl
```

Exact graph:

```text
UTxO -> consuming transaction -> effective produced UTxOs
```

Reverse uses:

```text
UTxO -> producing transaction -> effective consumed UTxOs
```

It never materializes or claims exact input-to-output allocation. Multi-input/
multi-output transactions are hyperedges. Optional poison/FIFO/proportional
models require explicit heuristic names.

The node budget is request-global. Its known-UTxO set begins with every seed
and grows with every hydrated input source and produced output reference,
whether or not that reference points in the traversal direction. A hyperedge
is admitted atomically only when its entire new-node union and the edge itself
fit the remaining budgets. The repository repeats this check independently,
and the application rejects an over-budget or unrelated repository result.
An edge `a -> b` therefore fits a two-node budget and emits nothing under a
one-node budget; it is never partially rendered.

Transaction and hyperedge hydration use one exact decoder under the same
snapshot lease. It validates the persisted header counts against complete
role-local input ordinal sets, output indexes/ordinals, withdrawals,
redeemers, datum observations, and metadata presence. It also revalidates
regular versus phase-2-invalid collateral flow, consumed flags, output kinds,
fee/mint application, withdrawal/redeemer application, and resolved redeemer
targets. A transaction response includes withdrawals, resolved redeemers,
metadata-map CBOR, and datum context. A fund-flow hyperedge includes the
compact transaction/Plutus context but treats only applied withdrawals as ADA
sources.

Defaults:

- depth: 4;
- known UTxOs: 10,000;
- address seed page: 1,000;
- frontier batch: 10,000;
- per-layer time: 30 seconds.

Hard demonstration caps:

- depth: 32;
- nodes: 100,000.

Every response includes:

- snapshot event;
- dataset completeness/trust mode;
- truncation state/reason;
- continuation frontier when bounded;
- unresolved partial-history inputs; and
- peer-observed disclaimer.

Applied withdrawals are external ADA sources; fees are sinks; mint/burn are
native-asset source/sink events. Unstored deposits/refunds/treasury effects are
reported as excluded non-UTxO deltas, not invented flow.

Because Clickout exposes address history and address-seeded tracing, address
lookup is not allowed to remain a full output-history scan. A compact
hash/offset projection may narrow candidates, but raw address bytes still
perform the exact collision check. `EXPLAIN` and scaled `read_rows` must prove
the access path, and the evidence reports its settled byte cost. If the access
path is not demonstrably bounded by matching rows, the address seed/command is
removed from the release surface rather than described as fast; UTxO and
transaction seeds remain exact.

### D-019: Clickout derives every snapshot from the append-only authority

Accepted.

Clickout is an independent schema consumer, but independence does not mean
guessing authority from raw table maxima. It must not treat `max(event_seq)`,
`argMax`, an unvalidated row count, or the newest fact publication as a
committed snapshot. A raw fact or membership row may exist before its
authorizing manifest transition, and rollback invalidations deliberately
precede the rollback header and final manifest transition.

The consumer therefore owns an independent implementation of the current
schema descriptor, manifest canonical payload, row-digest verification, and
evidence-set commitment. Producer and consumer parity is proved with one
shared set of five generated golden states:

- official synthetic genesis;
- a sampled current-evidence head;
- a pending evidence reservation;
- a pending rollback; and
- a finalized rollback.

The root producer generates each state through its production canonicalization
and digest functions. Clickout decodes the same wire values through its own
types and independently recomputes the payload/digest. Mutation tests must
fail on either side. This is a conformance oracle, not a second runtime module
dependency: `clickout/go.mod` has no root import or `replace`.

For one authority load, Clickout:

1. reads the latest manifest revision and its predecessor with a bounded
   physical-replay sentinel;
2. accepts at most eight byte-equivalent physical replays of a logical row,
   rejects the ninth or any conflict, verifies the canonical row digest, and
   verifies the predecessor digest plus immutable dataset identity;
3. independently reads, groups, and hashes the exact current and last-agreed
   evidence rows, including reserved-but-not-yet-written evidence crash cuts;
4. binds the manifest trust status, check/group/attempt, checked point,
   threshold, confirmed/disagreement outcome, and evidence prefix to those
   rows;
5. validates the exact current physical adoption or rollback artifact;
6. validates any pending rollback reservation and its permitted crash cut;
7. rejects every unreserved rollback header or invalidation, including an
   artifact hidden in an allocator gap; and
8. reloads the manifest head, retrying the whole read if either revision or
   row digest changed while validation was in progress.

Only an error observed against the same stable manifest head is returned as
corruption. A concurrent valid writer transition causes a retry, not a false
failure.

Physical rollback validation follows the producer's append-only protocol but
does not copy a runtime magic number into the schema consumer:

- a rollback event is strictly greater than its old physical event; allocator
  gaps are legal, so `event = old_event + 1` is not required;
- the full persisted `UInt32` depth is accepted; there is no Clickout
  hard-coded 2,160-depth authority cap;
- parent-chain, membership, and invalidation rows are streamed with stable
  keyset pagination and fixed per-query sentinels, never one
  `depth * replay_limit` query and never `OFFSET`;
- every walked publication has exactly one logical adoption followed by at
  most one committed logical invalidation, with at most eight identical
  physical replays per logical artifact;
- nonzero depth walks the chain from the exact old active tip to the exact
  target, with distinct nonzero publication identities, an exact first old
  tip, and an exact descendant count;
- depth zero requires old tip and target to be the same and requires no
  invalidation rows;
- Origin and a configured partial-history start are terminal boundaries, not
  fabricated block facts; and
- every invalidation must match the descendant point, rollback/event/writer
  identity, and UTC-microcanonical recorded instant exactly.

A pending `reserved` rollback permits either no invalidations or the complete
exact invalidation set. The latter is the real crash cut after the atomic batch
insert but before the manifest stage marker. It never permits a partial set or
a rollback header. `invalidations_written` requires the complete exact set and
permits the exact rollback header to be either absent or present. A finalized
rollback requires the exact header and set. Pending-event adoptions, later
adoptions, same-event adoption/rollback collisions, and artifacts in
`(Physical, Pending)` or after the authority barrier fail closed. More
precisely, while a rollback is pending, every adoption after `Physical` is
invalid: the sole writer captured that raw physical snapshot before reserving
the rollback, so an intervening committed adoption would have changed the
reservation's old-physical anchor. Without a pending rollback, an adoption
after `Physical` may simply be the normal facts/adoption-before-manifest crash
cut and is ignored until the manifest authorizes it.

An accepted request pins:

- dataset ID and the exact current schema-contract hash;
- network magic/name and all pinned genesis identities;
- visibility generation;
- the selected effective event and point; and
- the exact adoption event/publication pair used as its fact watermark.

For a rollback snapshot, the fact watermark may identify an adoption that is
inactive after the rollback. That is intentional: the watermark bounds facts,
while event membership decides visibility. `--at BLOCK_HASH` may select only a
publication that is active under the authoritative effective head and whose
adoption event is no later than that head; it never selects a raw maximum.

The request lease is checked again before returning. Ordinary adoption and
manifest-revision progress is allowed, but dataset/schema/network identity and
visibility generation must be unchanged, the effective authority must not
move behind the pin, and the pinned adoption-to-watermark mapping must remain
exact. Otherwise Clickout returns a typed unavailable/stale-snapshot error
rather than mixing views.

There is one current cursor encoding only. It has no version or `v` field and
no legacy/fallback decoder. Its checksum covers the dataset ID, schema hash,
network identity, visibility generation, effective event/point, exact fact
watermark, query scope, and last physical key. A cursor from a different
dataset, schema, network, rollback generation, or query scope is rejected.

## 13. Required tests

### 13.1 Wire and peer tests

- correct/wrong magic;
- v15 negotiation and refusal;
- Origin and stored-point intersection;
- verified partial-history boundary BlockFetch, exact-boundary rollback, and
  below-boundary rejection;
- continuous `RequestNext`/`Await`;
- Byron main block and EBB;
- single/range BlockFetch, exact streamed range order/count, tip flush, and
  `NoBlocks`;
- bounded range backpressure and no rollback overtaking an in-flight range;
- timeouts, disconnect, DNS rotation, peer failover;
- primary/secondary agreement and disagreement quarantine;
- moving tip;
- live rollback and re-adoption;
- restart from ClickHouse intersection;
- malformed/oversized CBOR;
- body/header/parent/height mismatch; and
- bounded backpressure.

### 13.2 Era and normalization tests

- deterministic genesis and first spends;
- Byron, Shelley, Allegra;
- Mary assets/mint/burn;
- Alonzo valid and phase-2-invalid collateral;
- Babbage collateral return, inline/witness/hash datum, reference input, and
  reference-script hash without body;
- Conway dApp transactions, including a post-van-Rossem/PV11 live block,
  while excluding governance bodies;
- a real future Dijkstra block whose unsupported nested semantics fail closed
  before publication;
- valid/invalid withdrawals;
- spend/mint/reward/cert/vote/proposal redeemer resolution;
- exact redeemer data and execution units;
- metadata labels/types/large integers/bytes/nested maps/lists;
- auxiliary scripts stripped from metadata storage;
- empty/max-length asset names and integer boundaries; and
- unknown future purpose/era fails closed.

### 13.3 Publication/recovery tests

- crash after every fact table and before adoption;
- bounded multi-block flush by count, bytes, rows, age, and shutdown;
- one physical insert per populated table per microbatch, not per block;
- per-publication verification inside a microbatch;
- exact all-or-none adoption-batch read-back after a lost response;
- pending-prefix truncation on rollback and replay after process death;
- lost response/duplicate replay;
- incomplete attempt never reused as complete;
- rollback membership before header is inert;
- rollback header commits atomically at query semantics;
- rollback resurrects spent source;
- re-adoption;
- committed-vs-allocated snapshot;
- manifest-tip reconciliation;
- server-side bounded descendants;
- two-writer contention, audit staleness after process death, and lock loss;
- insert-failure recovery without a falsely committed manifest.

### 13.4 ClickHouse/query tests

- raw binary round trips;
- forbidden-column/schema audit;
- current vs history;
- source-spend, consuming-transaction input, and producing-transaction output
  primary/projection plans, each with scaled `read_rows` evidence;
- event-sequence-ordered snapshot acquisition and pinned-event validation,
  with unrelated fact/membership history not reflected in `read_rows`;
- candidate-scoped snapshot membership for point/forward/reverse queries,
  with unrelated event-history scale not reflected in `read_rows`;
- address point/history plan with unrelated output-history scale not
  reflected in `read_rows`, or removal of the address command/seed if that
  bounded access path cannot be proven;
- datum body vs active observation provenance;
- resolved redeemer joins;
- withdrawal/metadata reads;
- forward/reverse hypergraph symmetry;
- rollback-pinned multi-layer traversal;
- high fanout/cycles/convergence;
- deterministic bounds/abort/timeouts;
- partial-history unresolved boundary; and
- rows/bytes/timing evidence.

### 13.5 Repository separation tests

- no old runtime/toolchain files;
- no old dependencies or bridge names in executable configuration;
- one schema/database path;
- root Go binary contains no analytical commands/packages;
- Clickout module builds/tests independently with root unavailable;
- `go list -deps` for Clickout contains no Clicksync module;
- each image contains only its intended binary/runtime; and
- README/runbook default commands use direct P2P.

## 14. Adversarial review gates

At least three independent, read-only reviews occur after implementation:

1. protocol/era/normalization reviewer;
2. ClickHouse/publication/rollback/schema reviewer; and
3. repository-boundary/code-quality/host-safety goal reviewer.

Reviewers inspect code and rerun bounded evidence. They rank critical, high,
medium, and low findings with file/line evidence.

All critical/high findings are fixed and re-reviewed. The final goal reviewer
must answer explicitly:

- Is Clicksync one clean Go direct-P2P ingestion service?
- Is there any legacy/mixed runtime left?
- Is Clickout actually independent?
- Did real peer UTxO/Plutus facts reach ClickHouse?
- Do forward and reverse fund-origin queries work on those facts?
- Are withdrawal/redeemer/metadata requirements met?
- Are trust/completeness and measured sizing limitations stated honestly?
- Is the 100 GB figure only a non-enforcing experiment/planning target, with
  no Clicksync capacity gate or quota machinery?

The persistent goal is not marked complete without affirmative evidence.

## 15. Manager-led implementation phases

The root agent performs steering and decision-document maintenance only. A
manager subagent owns implementation and delegates code to worker subagents.

### Phase A: destructive cleanup and Go foundation

- preserve local Git baseline/history;
- delete Node/TypeScript/bridge/old schema/runtime files;
- move the proven N2N work into the root Go service;
- create the independent Clickout module;
- create one Compose/Docker/runtime path; and
- add repository-hygiene tests.

Exit: one Go ingestion build, one independently buildable analysis module, no
legacy files.

### Phase B: sole schema and direct Go writer

- implement migrations;
- implement native binary inserts;
- implement bounded multi-block, table-oriented publication;
- implement manifest/fact/event/rollback tables;
- implement transaction inputs/outputs/assets/datums;
- implement withdrawals/redeemers/metadata;
- implement peer observations and writer audit lifecycle; and
- omit every application storage-limit/accounting gate and related schema
  state.

Exit: era fixtures publish atomically into the sole schema and a contiguous
range produces bounded, merged parts rather than parts proportional to
fact-table-count times block-count.

### Phase C: continuous N2N source

- Origin/stored intersection;
- continuous ChainSync/BlockFetch;
- bounded sequential BlockFetch ranges for backfill;
- bounded pipelining/backpressure;
- restart/failover;
- rollback/re-adoption;
- corroboration/provenance; and
- genesis seeding/completeness.

Exit: bounded continuous test follows multiple real blocks and survives
disconnect/restart/rollback fixtures.

### Phase D: independent Clickout

- point/transaction/context reads;
- current/history/address;
- datum/redeemer/metadata/withdrawal reads;
- bounded forward/reverse BFS; and
- snapshot/completeness/trust output.

Exit: module independence tests and real-data fund-origin demonstration pass.

### Phase E: adversarial remediation

- protocol review;
- schema/publication/query-plan review;
- repository/goal/host review;
- fix critical/high findings;
- rerun all affected gates; and
- produce exact evidence.

### Phase F: non-enforcing capacity evidence

- stratified samples;
- settled ClickHouse measurement;
- best-effort projected full-history size with stated uncertainty;
- report what the current 100 GB workspace can likely hold; and
- verify that no measurement is wired to a Clicksync warning, pause, or stop.

## 16. Immediate live acceptance demonstration

The requested demonstration passes only when:

1. the only running project services are Go Clicksync and ClickHouse;
2. no local Cardano node or protocol bridge exists;
3. Clicksync directly negotiates N2N and performs ChainSync + BlockFetch;
4. two independent operators corroborate a stable point;
5. multiple real blocks are published through the sole Go writer/schema;
6. stored rows include real effective UTxO flows and, where present,
   datum/redeemer/withdrawal/metadata facts;
7. no full block/transaction/script CBOR is persisted;
8. Clickout is built/run independently;
9. a real source UTxO expands forward through its spending transaction;
10. a produced UTxO expands backward to the transaction's effective inputs;
11. query snapshot/completeness/trust fields are visible;
12. restart/resume and rollback/re-adoption are demonstrated with live or
    faithful protocol fixtures;
13. the bounded experiment reports its observed storage/log footprint without
    enforcing a product capacity gate;
14. unrelated containers are unchanged; and
15. adversarial reviewers explicitly accept the goal.

## 17. Open evidence gates

These are evidence gates, not permission to weaken scope:

- current gOuroboros continuous ChainSync behavior across public relays;
- future Dijkstra/nested semantics fail closed without partially publishing a
  block;
- exact metadata-map and redeemer-data CBOR extraction without retaining
  auxiliary scripts/full transactions;
- writer-audit activation/heartbeat/release tied to the real held `flock`,
  without treating ClickHouse audit rows as fencing;
- sustained Origin-history service from public relays;
- full-mainnet compressed size after new dApp fields;
- real filesystem-space availability while a continuous backfill runs; and
- measured address/redeemer/metadata query plans.

If a gate fails, the manager reports the blocker and safest alternative. It
does not restore deleted legacy code, recreate multiple product generations,
silently omit transaction data, trust one arbitrary peer, or introduce a
Clicksync runtime capacity limit.
