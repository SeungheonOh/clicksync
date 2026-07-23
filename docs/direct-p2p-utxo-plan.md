# Direct-P2P UTxO Clicksync: Steering Plan and Decision Log

Status: accepted for implementation, subject to the hard gates in this document  
Decision date: 2026-07-23  
Steering owner: root agent  
Implementation ownership: delegated agents only

## 1. Objective

Build Clicksync as a narrowly scoped Cardano UTxO-flow indexer that:

1. connects directly to remote Cardano relays over the node-to-node (N2N)
   Ouroboros protocols;
2. does not require a local `cardano-node`, Ogmios, Dolos, Oura, DB Sync, or a
   second persistent copy of the chain;
3. decodes block bodies in bounded memory and immediately discards block and
   transaction CBOR;
4. stores only the facts necessary to reconstruct historical, ledger-effective
   UTxO flow, native-asset sources/sinks, and datum content;
5. supports bounded forward and reverse graph traversal in ClickHouse; and
6. stays within a 100 GiB project budget or stops safely without deleting
   canonical history.

The immediate proof is deliberately smaller than a full mainnet backfill. It
must retrieve real blocks from public peers, put real UTxO facts into
ClickHouse, and demonstrate correct forward and reverse traversal. Full
mainnet ingestion is allowed only after the measured storage gate passes.

## 2. Non-goals

Clicksync will not store or serve:

- raw block CBOR;
- raw transaction CBOR;
- validator, minting-policy, native, or reference-script bytes;
- redeemers or execution units;
- transaction metadata;
- certificates, stake delegation, pools, rewards, or epoch state;
- withdrawals as an independent accounting subsystem;
- governance proposals, votes, constitutions, ratification, or enactment;
- transaction signatures and witnesses other than datum bodies;
- reference inputs, because they observe rather than consume a UTxO;
- mempool data, transaction submission, block production, or ledger queries;
- a fabricated input-to-output allocation for multi-input transactions; or
- independent Cardano consensus or ledger validation in the first version.

Governance-era and future-era blocks must still yield their UTxO effects. An
unknown UTxO-bearing era fails closed; it must never be silently skipped.

## 3. Terminology and honesty requirements

### 3.1 Peer-observed chain

N2N ChainSync reproduces a remote peer's candidate chain. ChainSync does not
perform chain selection and structural hash checks do not prove ledger or
consensus validity. Until Clicksync reuses a complete consensus and ledger
implementation, the indexed chain is always described as:

> peer-observed, structurally verified Cardano chain data

It must not be described as trustless, consensus-validated, or independently
ledger-validated.

### 3.2 Complete-history and partial-tail datasets

A dataset beginning at Origin and including deterministic genesis seeding may
be marked `complete_history=true` after all completeness checks pass.

A dataset beginning at a recent peer point must be marked
`complete_history=false`. Its old inputs may be unresolved and its apparent
current UTxO is only current relative to the indexed slice. The CLI and query
results must expose this condition; no default may conceal it.

### 3.3 Transaction hypergraph

Cardano records a set of consumed UTxOs and a set of produced UTxOs for a
transaction. It does not record which particular input funded which particular
output.

The exact graph is therefore bipartite:

```text
UTxO output -> consuming transaction -> produced UTxO outputs
```

Reverse traversal follows the same graph backward. Poison, FIFO, proportional,
haircut, or other taint-allocation models may be added later as explicitly
named heuristics. They are not ledger facts and will not be persisted as exact
edges.

## 4. Accepted decisions

### D-001: Use direct N2N ChainSync and BlockFetch

Decision: accepted.

ChainSync supplies headers, points, tips, and rollback instructions. BlockFetch
supplies the corresponding block bodies. A correct client must run handshake,
ChainSync, BlockFetch, and KeepAlive over one multiplexed TCP connection.
PeerSharing is optional and is not an identity or trust mechanism.

The client must:

- negotiate the configured network magic and an explicitly supported protocol
  version;
- intersect from stored canonical points, with Origin as the final fallback;
- fetch the body for every selected roll-forward header;
- verify the fetched body's point/header hash and body commitment;
- verify previous-hash continuity, block-number continuity, and legal slot
  progression;
- explicitly support Byron epoch-boundary blocks;
- handle inclusive block ranges, `NoBlocks`, timeouts, disconnects, and peer
  rotation;
- recompute transaction IDs and datum hashes;
- bound all message, CBOR, queue, and timeout sizes; and
- discard raw block/transaction bytes immediately after normalization.

Primary protocol references:

- <https://developers.cardano.org/docs/developers/curriculum/production/network-protocol/>
- <https://ouroboros-network.cardano.intersectmbo.org/pdfs/network-spec/network-spec.pdf>

### D-002: Isolate gOuroboros behind a small Go helper

Decision: accepted for the first live proof.

Use Blink Labs gOuroboros:

- release: `v0.189.1`;
- commit: `9293adc2c94e390ca0545e870e5272ab9ac969fa`;
- module checksum:
  `h1:JIPi37b45LQL3e0Kvit5s/OSWjcB/V7WEI9bZ/T27Tc=`;
- `go.mod` checksum:
  `h1:n1NCfwdivyiavSvEa+hZoNWWSUNc3m6Lqw4WH16GQbQ=`.

This version advertises N2N v7-v15 and Byron-through-Dijkstra transaction
decoding. Those source-code claims are not an acceptance substitute: current
wire compatibility and Dijkstra semantics must pass live and fixture gates.

The helper is built in a digest-pinned Go multi-stage container because the
host has no Go toolchain. Only the narrow networking/ledger packages required
for decoding may be imported. The implementation records its module graph,
SBOM, vulnerability scan, upstream tag, commit, checksums, builder digest, and
license.

Rejected for the first proof:

- implementing the mux, handshake, and hard-fork codecs from scratch;
- importing the Haskell node/consensus dependency graph before proving the
  index;
- running Dolos, which would retain an additional ledger/archive store;
- using Oura as another long-running pipeline layer;
- Pallas/Oura as the production decoder before current Dijkstra/v15 and
  phase-2 semantics pass the same gates; and
- a full Go rewrite, which would discard already tested ClickHouse
  publication and rollback work before N2N ingestion is proven.

Upstream reference:

- <https://github.com/blinklabs-io/gouroboros/releases/tag/v0.189.1>

### D-003: Preserve one TypeScript commit coordinator

Decision: accepted for the first proof.

The TypeScript process remains the sole owner of:

- ClickHouse schema migration;
- publication/event sequence allocation;
- facts-first/adoption-last commit ordering;
- rollback membership/rollback-header ordering;
- resume-point selection;
- disk high-water decisions; and
- BFS snapshot tokens.

The Go helper must never allocate ClickHouse sequence numbers and must never
write ClickHouse directly in this phase.

The deployed application container contains both the Node runtime and a static
`clicksync-p2p` helper. No P2P sidecar service or inbound application port is
required.

### D-004: Use a versioned, backpressured process contract

Decision: accepted.

The TypeScript parent spawns `/app/bin/clicksync-p2p`. Standard output is
protocol-only NDJSON; standard error is bounded structured logging. The parent
forwards termination signals, waits for child shutdown, and treats an
unexpected child exit as a source failure. The child exits if the parent pipe
closes.

Every envelope includes:

- `schema_version`;
- `kind`: `ready`, `roll_forward`, `roll_backward`, `heartbeat`, or `fatal`;
- `session_id`;
- monotonically increasing `source_seq`;
- network magic;
- selected peer and negotiated N2N version;
- chain point, parent point, and observed tip as applicable; and
- a normalized UTxO-only payload for roll-forward.

The adapter rejects:

- unknown schema versions or message kinds;
- the wrong network magic;
- sequence gaps;
- a conflicting duplicate key;
- malformed or oversized lines;
- invalid binary encodings; and
- facts whose computed identities disagree with the envelope.

The child has a small configurable unacknowledged window, initially one to
eight blocks. TypeScript acknowledges only after ClickHouse publication. OS
pipe flow control plus the acknowledgement window bounds memory. Restart
state is reconstructed from ClickHouse, so delivery is at least once.

NDJSON is accepted initially because Cardano production rate and ClickHouse
insertion are expected to dominate. If measurement shows serialization/IPC
above 20% of ingestion CPU, a binary framing format or direct Go writer may be
proposed in a new decision.

### D-005: Use independently operated, pinned bootstrap peers

Decision: accepted.

Initial mainnet seeds:

- `backbone.cardano.iog.io:3001`;
- `backbone.mainnet.emurgornd.com:3001`;
- `backbone.mainnet.cardanofoundation.org:3001`.

Defaults are copied from dated official topology documentation and are user
overridable. DNS answers are rotated and health-scored. Runtime PeerSharing may
provide capacity later, but never supplies identity or trust.

At least two independently operated peers must recognize stable checkpoints.
Recent tips and any unexpected/deep rollback require corroboration. Persistent
disagreement quarantines publication and reports the disagreement; it never
selects a chain silently.

This is a trust mitigation, not independent Ouroboros chain selection.

Reference:

- <https://developers.cardano.org/docs/get-started/infrastructure/node/topology/>

### D-006: Support Origin mode and an honest live-proof mode

Decision: accepted.

Full history:

1. find intersection at Origin;
2. consume headers in order;
3. fetch every selected block body in bounded ranges;
4. normalize and publish in order; and
5. resume from dense recent plus geometrically spaced historical points.

Fast live proof:

1. obtain a recent candidate point from one official peer;
2. require a second independently operated peer to recognize the point;
3. publish blocks after that point; and
4. mark the dataset incomplete.

A second proof may freeze the initially observed tip, stream headers from
Origin without fetching old bodies, and begin BlockFetch at `tip_height - N`.
This remains a partial dataset and may take hours because every historical
header is still traversed.

No external REST provider is required to obtain a working point.

### D-007: Store only ledger-effective UTxO facts

Decision: accepted.

For a valid transaction:

- store regular consumed inputs;
- store regular produced outputs;
- store effective mint/burn;
- ignore collateral inputs, collateral return, and reference inputs.

For a phase-2-invalid transaction:

- store consumed collateral inputs only;
- store collateral return only when the era/body provides it;
- ignore regular inputs and outputs;
- ignore mint/burn;
- ignore reference inputs; and
- mark the transaction flow as collateral.

The implementation must independently test any upstream `Consumed()` and
`Produced()` convenience methods rather than assuming their correctness.
Collateral-return index rules and pre-Babbage behavior require golden vectors.

### D-008: Datum CBOR is the only retained CBOR

Decision: accepted.

Retain:

- output datum kind: none, hash, or inline;
- datum hash;
- inline datum body;
- witness-map datum bodies; and
- exact datum CBOR, deduplicated by its 32-byte content hash.

The helper must extract or preserve the exact datum fragment required to
recompute the ledger hash. Re-serializing a decoded datum is accepted only
after golden vectors prove byte/hash equivalence.

A hash-only datum whose body never appears on chain remains unresolved. The
system must not claim otherwise.

Do not retain the surrounding transaction CBOR, scripts, redeemers, or other
witnesses.

References:

- <https://cips.cardano.org/cip/CIP-0032>
- <https://cips.cardano.org/cip/CIP-0031>
- <https://cips.cardano.org/cip/CIP-0040>

### D-009: Seed genesis deterministically

Decision: accepted; required before a dataset may claim complete history.

ChainSync does not emit genesis distributions. Clicksync must:

- ship or fetch hash-pinned official Byron and Shelley genesis
  configurations;
- verify their configured hashes;
- synthesize the exact ledger-compatible genesis output references;
- cover Byron redemption/distribution and Shelley `initialFunds` where used;
- mark these publications synthetic and permanently canonical; and
- compare total genesis supply and first-spend references against a trusted
  reference fixture.

A database without these rows is incomplete even if ChainSync began at Origin.

### D-010: Use a compact ClickHouse v2 database

Decision: accepted conceptually; physical settings remain subject to measured
query plans.

Create a new v2 database. Do not mutate or drop the existing v1 schema until
semantic comparison and review pass.

Use one database per Cardano network. Store network magic and completeness in
a small dataset manifest instead of repeating network magic on every fact.
Store hashes and policy IDs as binary fixed-width values, addresses and asset
names as raw bytes, and render hex/bech32 only at API boundaries.

The TypeScript HTTP insertion path will accept bounded hex/base64 fields and
decode them inside ClickHouse (for example with `unhex`) before inserting
`FixedString`/binary columns. It must not corrupt arbitrary bytes by treating
them as Unicode text.

Logical tables:

1. `dataset_manifest`
   - network magic, network name, schema version, genesis hashes;
   - `complete_history`, start point, trust mode, source version;
   - configured budget and high-water state.
2. `block_publications`
   - publication sequence, block/parent hashes, slot, block number, era,
     transaction count, synthetic flag.
3. `publication_events`
   - publication sequence, event sequence, active state, optional rollback ID.
4. `rollbacks`
   - rollback commit/audit header and peer observations.
5. `flow_transactions`
   - publication sequence, transaction hash/order, optional parent/sub-index,
     regular/collateral/genesis flow kind, effective fee, sorted mint/burn
     arrays.
6. `effective_spends`
   - source UTxO reference, consuming transaction, ordinal, regular/collateral
     kind, publication sequence.
7. `effective_outputs`
   - created UTxO reference, output kind, raw address, lovelace, datum
     kind/hash, sorted native-asset arrays, publication sequence.
8. `datums`
   - datum hash, exact datum CBOR, source kind, last-seen sequence.

No forbidden broad-data column may exist.

Hashes use `FixedString(32)`, policy IDs `FixedString(28)`, quantities use
unsigned 64-bit output values and signed 64-bit mint deltas, and asset names
remain raw 0-to-32-byte strings. Empty asset names are legal. Parallel asset
arrays must have identical lengths, deterministic `(policy,name)` ordering, no
ADA entry, and no zero quantity.

Primary access:

- outputs by `(tx_hash, output_index)`;
- spends by `(source_tx_hash, source_output_index)`;
- transactions by `tx_hash`;
- reverse spend lookup by `spending_tx_hash`;
- address history by exact raw address; and
- datums by datum hash.

ClickHouse 26.3 lightweight `_part_offset` projections may be used for
address-fingerprint and reverse-spender access only after `EXPLAIN indexes=1`
and measured scan counts prove their use. Fingerprints are prefilters; exact
binary equality is always required to eliminate collision errors.

Do not materialize input-by-output edges. Do not explode every output asset
into a separate hot-table row in v2. A separate cold asset index may be
proposed later if measured global asset search requires it.

### D-011: Preserve append-only, crash-safe publication

Decision: accepted.

Facts for one immutable publication are inserted before its active adoption
event. Without that final event the facts are invisible.

A rollback:

1. resolves active descendants at one captured sequence;
2. appends inactive membership events carrying one rollback ID; and
3. inserts the rollback header last as the commit marker.

Rollback membership without its header is inert. A later re-adoption appends a
newer active event.

An already complete publication may be reused on re-adoption. A genuinely
incomplete attempt may be retried as a new invisible publication. Orphan and
incomplete bytes are monitored and bounded; they may never be deleted by
frequent mutations or manual part removal. Sealed-partition rebuilding is an
offline, separately reviewed maintenance operation.

Every multi-query traversal captures one `snapshot_event_seq` and applies it
to every frontier layer and page. A traversal must observe either side of a
rollback, never a mixture.

### D-012: Retain mint/burn and fee boundaries, but not non-UTxO state

Decision: accepted.

Native-asset mint and burn deltas are stored as sorted parallel arrays on the
minimal transaction row, because otherwise asset traversal fabricates sources
and sinks. A separate mint table is not initially justified.

Store the effective fee because it is a compact, useful ADA sink:

- valid transaction: declared fee;
- invalid transaction: ledger-effective collateral fee.

ADA withdrawals, deposits/refunds, treasury effects, donations, and similar
non-UTxO state are intentionally outside scope. Consequently, Clicksync does
not claim total ADA conservation. If source values are locally resolvable, it
may report:

```text
sum(effective inputs) - sum(effective outputs) - fee
```

as `excluded_non_utxo_ledger_delta`. It must not label that gap unexplained
loss or infer governance/staking details.

### D-013: BFS is bounded, snapshot-pinned, and application-driven

Decision: accepted for the first version.

Commands:

```text
clicksync utxo TX_HASH#INDEX [--at tip|BLOCK_HASH]
clicksync address ADDRESS [--state current|history] [--limit N] [--cursor C]
clicksync tx TX_HASH
clicksync trace --direction forward|reverse \
  (--utxo TX_HASH#INDEX|--tx TX_HASH|--address ADDRESS) \
  --max-depth N --max-nodes N [--asset ada|POLICY.NAME] --format jsonl
```

Forward layer:

```text
frontier UTxOs -> active effective spends -> consuming transactions
               -> active effective outputs
```

Reverse layer:

```text
frontier UTxOs -> their creating transactions
               -> active effective spends consumed by those transactions
```

Initial defaults:

- maximum depth: 4;
- maximum visited nodes: 10,000;
- maximum seed outputs: 1,000;
- maximum frontier batch: 10,000;
- per-layer query time: 30 seconds.

Hard MVP caps:

- depth: 32;
- nodes: 100,000.

Maintain visited UTxO and transaction sets, stream JSONL results, and return
the snapshot token, `truncated` state, reason, and continuation frontier.
Address seeds always require explicit current/history semantics and pagination.
High-fanout/exchange addresses cannot expand without the same bounds.

ClickHouse recursive CTEs may be benchmarked later, but are not required for
correctness or the first proof.

### D-014: Treat 100 GiB as a hard operational budget

Decision: accepted.

Budget:

- 70 GiB: active ClickHouse data, including projections;
- 20 GiB: ClickHouse merge/temp/free-space reserve;
- 5 GiB: bounded logs, manifests, and zero-retention staging;
- 5 GiB: emergency headroom.

Warn at 60 GiB. Pause new publication before active data reaches 70 GiB or
total project footprint threatens the reserve. Never prune canonical flow or
datum history automatically.

The current host has approximately 226 GiB physically free but the project is
authorized to use no more than 100 GiB. The host filesystem itself does not
currently expose a project quota and noninteractive administrative mounting is
unavailable. Therefore:

- the first bounded proof uses application and ClickHouse high-water checks;
- full mainnet is a no-go until either a dedicated capped filesystem/LV is
  supplied or an equivalent independently tested hard quota exists;
- ClickHouse `system.parts`, `system.parts_columns`, `system.disks`, settled
  merge state, and `du -x` are measured;
- logs have size/retention limits;
- inserts are large enough to avoid excessive part/inode counts; and
- no per-block files are written.

The application stops cleanly at the gate. It does not treat the 226 GiB host
free space as authorization to continue.

### D-015: Full-mainnet fit is a measured gate, not a promise

Decision: accepted.

Before full backfill, ingest stratified samples:

- Byron;
- Shelley/Allegra;
- Mary and an asset/NFT-heavy range;
- Alonzo;
- Babbage;
- Conway/current era; and
- at least one contiguous, recent, high-activity range.

After merges settle, record row counts, compressed/uncompressed bytes per table
and column, projection bytes, parts, inodes, and query scan metrics. Fit bytes
per block, transaction, input, output, asset, and datum byte, then extrapolate
with at least 30% confidence/growth margin.

Full mainnet may start only if the conservative projection is at or below
70 GiB active data. If it exceeds the gate, report the measured required
capacity and renegotiate storage or scope. Do not weaken correctness or delete
history to force a pass.

### D-016: Isolate experiments from the shared Docker host

Decision: accepted.

The host already runs unrelated containers. The implementation must:

- use a unique explicit Compose project name, network, volume/bind path, port,
  and labels;
- bind ClickHouse administration to a nonconflicting localhost port or keep it
  internal;
- expose no P2P listener; all peer traffic is outbound;
- set CPU, memory, PID, and JSON-log size limits;
- avoid an experimental infinite restart loop;
- record the existing-container baseline and verify it remains unchanged;
- never run Docker prune, global cleanup, or delete unrelated volumes/images;
  and
- stop only its own explicitly named project resources.

### D-017: Establish an audit boundary before code changes

Decision: accepted.

The workspace currently has no `.git` directory. Before implementation, the
delegated lead will:

1. inspect `.gitignore` for secrets and runtime data;
2. initialize a local-only Git repository;
3. record the untouched baseline in a baseline commit; and
4. make staged implementation commits or otherwise preserve reviewable diffs.

No remote push or publication is authorized.

## 5. Required validation matrix

### Protocol and peer behavior

- correct and incorrect network magic;
- accepted/refused N2N versions, including live v15 when available;
- Origin intersection;
- Byron main headers and EBBs;
- BlockFetch single and bounded inclusive ranges;
- `NoBlocks`, timeout, disconnect, DNS rotation, and failover;
- slow-peer backpressure;
- conflicting peers and quarantine;
- moving tip;
- rollback, deep rollback, and rollback-to-Origin;
- malformed/oversized CBOR; and
- body/header/parent mismatch.

### Era and ledger-effective normalization

- deterministic genesis and first spends;
- Byron output/spend;
- Shelley and Allegra ADA flows;
- Mary assets plus mint and burn;
- Alonzo valid and invalid phase-2 transactions;
- Babbage inline datum, witness datum, hash-only datum, ignored reference input,
  omitted reference script, collateral, and collateral return;
- Conway transaction containing governance data while storing only its UTxO
  effects;
- current Dijkstra top-level/nested transaction behavior;
- empty and 32-byte asset names;
- maximum unsigned quantities and signed mint boundaries; and
- unknown future era fails closed.

### Publication and recovery

- failure after each fact insert and before adoption;
- failure during rollback membership and before rollback header;
- duplicate block after lost acknowledgement;
- restart intersection;
- block re-adoption;
- output creation, spend, rollback resurrection, and re-adoption;
- orphan-byte monitoring; and
- snapshot-pinned reads during rollback.

### Graph behavior

- forward and reverse symmetry;
- UTxO, transaction, and paginated address seeds;
- multi-input/multi-output fan-in/fan-out without pair attribution;
- converging paths and cycles;
- high fanout;
- mint source, burn sink, and fee sink;
- asset-filtered traversal;
- partial-tail unresolved boundary inputs;
- deterministic node/depth/time truncation; and
- continuation under one snapshot token.

### Storage and performance

- no forbidden tables or columns;
- binary round trips for hashes, addresses, asset names, and datum CBOR;
- exact-equality checks after fingerprints;
- merged bytes per table/column/projection;
- warning/pause thresholds;
- merge-space exhaustion simulation;
- bounded log retention;
- ClickHouse `EXPLAIN indexes=1` for both traversal directions and address
  lookup;
- part/inode growth; and
- no impact to unrelated containers.

Initial warm performance gates on representative data:

- one UTxO or transaction point lookup: below 100 ms;
- one address page of at most 1,000 rows: below 250 ms;
- one 10,000-node traversal layer: below 2 seconds.

These are acceptance targets, not public performance claims. Cold behavior and
bytes/rows scanned are recorded separately.

## 6. Staged execution plan

### Phase 0: audit safety

- establish local Git baseline;
- record host/container/disk baseline;
- pin toolchain and dependency artifacts;
- create v2 schema alongside untouched v1; and
- add the decision document to the implementation review checklist.

Exit: reviewable baseline with no runtime impact.

### Phase 1: protocol spike

- build the minimal Go helper in a container;
- handshake with all three official peers;
- record version, magic, and tips;
- ChainSync from Origin far enough to observe real Byron/EBB behavior;
- BlockFetch and decode historical blocks;
- fetch the same stable point from two independent peers;
- fetch and decode live current-era blocks; and
- emit normalized envelopes without ClickHouse.

Exit: live, non-mock wire proof and current-era proof.

### Phase 2: compact schema and publication

- implement v2 DDL;
- implement safe binary insertion;
- add publication/event/rollback barriers;
- add datum deduplication and hash checks;
- enforce forbidden-payload tests; and
- implement high-water checks.

Exit: golden fixtures publish atomically and contain only accepted facts.

### Phase 3: end-to-end bounded ingestion

- connect the Go source adapter to TypeScript;
- publish an Origin historical slice;
- publish a peer-recognized live tail slice into a separately marked dataset;
- exercise disconnect/restart and rollback/re-adoption; and
- prove raw CBOR is not persisted.

Exit: real peer data exists in ClickHouse with explicit dataset completeness.

### Phase 4: query and BFS proof

- implement point, transaction, address, forward, and reverse primitives;
- pin every traversal to one event snapshot;
- enforce all breadth/depth/time bounds;
- locate a real in-slice spend;
- demonstrate forward and reverse traversal; and
- record `EXPLAIN` and timings.

Exit: the requested ClickHouse UTxO-flow demonstration works on real chain
facts.

### Phase 5: adversarial review and remediation

Independent reviewers cover:

1. N2N/protocol/current-era correctness and trust labels;
2. UTxO/collateral/datum/genesis/rollback semantics;
3. ClickHouse storage/query plans and the 100 GiB gate; and
4. code quality, dependency supply chain, process supervision, and host safety.

All critical/high findings are fixed or explicitly block completion. Reviewers
rerun the affected gates after remediation.

### Phase 6: measured capacity decision

- run stratified samples;
- settle merges;
- calculate conservative full-history projection;
- document required disk and expected query behavior; and
- either authorize full Origin backfill or stop at a measured no-go.

Exit: evidence-backed full-history capacity decision.

## 7. Definition of the immediate requested proof

The immediate proof is complete only when all of the following are present:

1. no local `cardano-node`, Ogmios, Dolos, or Oura process;
2. direct outbound N2N handshake, ChainSync, and BlockFetch against official
   remote peers;
3. the same stable block recognized by two independent operators;
4. real decoded UTxO facts, including at least one real spend, stored in the v2
   ClickHouse schema;
5. no persisted block/transaction/script CBOR or forbidden broad data;
6. datum body/hash handling demonstrated when the sample contains one, plus
   golden datum tests regardless;
7. a forward traversal from source UTxO through its consuming transaction to
   outputs;
8. the matching reverse traversal;
9. rollback/re-adoption and crash visibility tests;
10. explicit complete/partial dataset labeling;
11. measured ClickHouse bytes, scanned rows/bytes, and query timings;
12. the project remains below its gates and unrelated containers are
    unchanged; and
13. independent adversarial reviewers accept the result.

## 8. Open gates, not undecided scope

The following are intentionally evidence gates:

- whether gOuroboros v0.189.1 interoperates with the current live v15/Dijkstra
  network exactly as advertised;
- whether public bootstrap relays permit sustained Origin backfill;
- whether the exact datum CBOR fragment can be recovered and hash-verified
  through the selected decoder API;
- whether ClickHouse's chosen projections are actually used;
- whether NDJSON IPC stays below the 20% CPU reconsideration threshold;
- whether a host-admin-provided hard 100 GiB filesystem can be supplied for
  full backfill; and
- whether full mainnet plus the required indexes projects to at most 70 GiB.

A failed gate changes the implementation path or stops full backfill. It does
not authorize silently broadening storage, weakening semantics, trusting one
arbitrary peer, or consuming the host's ungranted free space.

