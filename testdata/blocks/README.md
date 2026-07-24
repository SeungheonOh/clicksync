# Real block fixtures

These are test-only, gzip-compressed raw block bytes copied from the exact
`github.com/blinklabs-io/gouroboros` `v0.189.1` module pinned by `go.mod`.
They are not loaded or retained by the Clicksync runtime.

The seven Byron-through-Conway fixtures are real mainnet blocks published by
the upstream project in
[`internal/testdata`](https://github.com/blinklabs-io/gouroboros/tree/v0.189.1/internal/testdata).
The Dijkstra fixture was captured by that project over node-to-node from the
Musashi prototype testnet (network magic 164, slot 566037, block 28091) and is
published in
[`ledger/dijkstra/testdata`](https://github.com/blinklabs-io/gouroboros/tree/v0.189.1/ledger/dijkstra/testdata).
The real Byron epoch-boundary block is published in
[`protocol/chainsync/testdata`](https://github.com/blinklabs-io/gouroboros/tree/v0.189.1/protocol/chainsync/testdata).

Each file is the whitespace-stripped upstream hex decoded to its original
bytes, then compressed with deterministic `gzip -9 -n`. Tests verify the raw
SHA-256 before decoding and the ledger block hash after decoding.

| File | Raw-byte SHA-256 | Expected ledger block hash |
|---|---|---|
| `byron-main.cbor.gz` | `bd58984bd312930fea49b715af6ddc3b4cfa7da64143fa602edd3af314e7ae63` | `1451a0dbf16cfeddf4991a838961df1b08a68f43a19c0eb3b36cc4029c77a2d8` |
| `byron-ebb-testnet.cbor.gz` | `a4b0f5247bd7fa3be5c8f3b8d328423a3e886c81ff78fba8d92b14812bc7fef9` | `8f8602837f7c6f8b8867dd1cbc1842cf51a27eaed2c70ef48325d00f8efb320f` |
| `shelley.cbor.gz` | `6a7b7393e68bdde768f6903dbb9319e4c77e461a5748f8b34f2381f79b638b03` | `2308cdd4c0bf8b8bf92523bdd1dd31640c0f42ff079d985fcc07c36cbf915c2b` |
| `allegra.cbor.gz` | `56c41ce9a67239cd5e2209aae7a61d292f4aeb4249ad6a927bb821ae185523fb` | `8115134ab013f6a5fd88fd2a10825177a2eedcde31cb2f1f35e492df469cf9a8` |
| `mary.cbor.gz` | `07b446bea4f5bd2e44a90548bfdf7219ed55a0d7141e0154e72e1ad73876b543` | `d36ab36f451e9fcbd4247daef45ce5be9a4b918fce5ee97a63b8aeac606fca03` |
| `alonzo.cbor.gz` | `8f3a4fdc760f34506c21c114cb3d6e253e1e6637576c0a6b139eb49170ca7bcc` | `1d7974cb01cc9e3fbe9dd7594795a36b21cb1deb2f1b70a0625332c91bd7e5a7` |
| `babbage.cbor.gz` | `9384202e72b6e9c460f38d6836059c9d3769e50b59f3fbac43fed3490765e57a` | `db19fcfaba30607e363113b0a13616e6a9da5aa48b86ec2c033786f0a2e13f7d` |
| `conway.cbor.gz` | `154393dc5c75318fb67148f59677de5afa74aa4e64ee96980043b2f14111eb32` | `27807a70215e3e018eec9be8c619c692e06a78ebcb63daf90d7abe823f3bbf47` |
| `dijkstra-musashi.cbor.gz` | `465b18ecd55cef0218304861bddcf1dd42d16449c042b9b952898d3993619e56` | `3df256c7ebfd46d2de897dd8bd7cd7c4c5a958176380dbc607c0b929e5227f1a` |

The copied upstream fixtures are distributed under the Apache License 2.0;
the upstream license is retained as `LICENSE.gouroboros`.
