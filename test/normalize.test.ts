import type { BlockBFT, BlockEBB, BlockPraos, Transaction } from '@cardano-ogmios/schema'
import { describe, expect, it } from 'vitest'

import { normalizeBlock } from '../src/cardano/normalize.js'

const networkMagic = 42

describe('normalizeBlock', () => {
  it('normalizes an epoch-boundary block without inventing a slot or point', () => {
    const block: BlockEBB = {
      type: 'ebb',
      era: 'byron',
      id: 'ebb-hash',
      ancestor: 'ebb-parent',
      height: 10,
    }

    const normalized = normalizeBlock(block, { networkMagic, eventSeq: 7n })

    expect(normalized.point).toBeNull()
    expect(normalized.block).toEqual({
      network_magic: networkMagic,
      block_hash: 'ebb-hash',
      previous_block_hash: 'ebb-parent',
      slot: null,
      block_number: 10,
      era: 'byron',
      block_type: 'ebb',
      size_bytes: null,
      tx_count: 0,
      issuer_vkey: null,
      ingest_seq: '7',
    })
    expect(normalized.transactions).toEqual([])
  })

  it('normalizes a Byron top-level transaction', () => {
    const transaction: Transaction = {
      id: 'byron-tx',
      spends: 'inputs',
      inputs: [reference('byron-source', 2)],
      outputs: [output('DdzFFzByron', 123n)],
      signatories: [],
    }
    const block: BlockBFT = {
      type: 'bft',
      era: 'byron',
      id: 'byron-block',
      ancestor: 'byron-parent',
      height: 11,
      slot: 12,
      size: { bytes: 512 },
      transactions: [transaction],
      protocol: {
        id: 0,
        version: { major: 1, minor: 0 },
        software: { appName: 'cardano-sl', number: 1 },
      },
      issuer: { verificationKey: 'issuer-extended-vkey' },
      delegate: { verificationKey: 'delegate-extended-vkey' },
    }

    const normalized = normalizeBlock(block, { networkMagic, eventSeq: 8n })

    expect(normalized.point).toEqual({ id: 'byron-block', slot: 12 })
    expect(normalized.transactions[0]).toMatchObject({
      tx_hash: 'byron-tx',
      tx_index: 0,
      parent_tx_hash: null,
      subtx_index: null,
      is_valid: true,
      is_applied: true,
    })
    expect(normalized.inputs[0]).toMatchObject({
      input_kind: 'spend',
      source_tx_hash: 'byron-source',
      source_output_index: 2,
      is_consumed: true,
    })
    expect(normalized.outputs[0]).toMatchObject({ lovelace: '123', is_produced: true })
  })

  it('keeps invalid regular facts but applies only collateral facts', () => {
    const huge = 9007199254740993123n
    const metadataHuge = 900719925474099312345678901234567890n
    const transaction: Transaction = {
      id: 'invalid-tx',
      spends: 'collaterals',
      inputs: [reference('regular-source', 0)],
      references: [reference('reference-source', 1)],
      collaterals: [reference('collateral-source', 2)],
      outputs: [
        {
          address: 'addr_regular',
          value: {
            ada: { lovelace: huge },
            policyA: { '': huge, deadbeef: 2n },
          },
          datumHash: 'datum-hash',
          datum: 'inline-datum-cbor',
          script: {
            language: 'native',
            json: { clause: 'some', atLeast: metadataHuge, from: [] },
          },
        },
      ],
      collateralReturn: {
        address: 'addr_collateral_return',
        value: {
          ada: { lovelace: 5n },
          policyB: { cafe: 7n },
        },
      },
      fee: { ada: { lovelace: huge } },
      mint: { policyMint: { token: -huge } },
      metadata: {
        hash: 'metadata-hash',
        labels: { '1': { json: { int: metadataHuge } } },
      },
      signatories: [],
    }
    const normalized = normalizeBlock(praosBlock('babbage', [transaction]), {
      networkMagic,
      eventSeq: 18446744073709551615n,
    })

    expect(normalized.transactions[0]).toMatchObject({
      tx_hash: 'invalid-tx',
      is_valid: false,
      is_applied: false,
      fee_lovelace: huge.toString(),
      ingest_seq: '18446744073709551615',
    })
    expect(JSON.parse(normalized.transactions[0]?.metadata_json ?? '')).toEqual({
      hash: 'metadata-hash',
      labels: { '1': { json: { int: metadataHuge.toString() } } },
    })

    expect(normalized.inputs.map(({ input_kind, is_consumed }) => ({ input_kind, is_consumed })))
      .toEqual([
        { input_kind: 'spend', is_consumed: false },
        { input_kind: 'collateral', is_consumed: true },
        { input_kind: 'reference', is_consumed: false },
      ])
    expect(normalized.outputs.map(({ output_index, output_kind, is_produced }) => ({
      output_index,
      output_kind,
      is_produced,
    }))).toEqual([
      { output_index: 0, output_kind: 'regular', is_produced: false },
      { output_index: 1, output_kind: 'collateral_return', is_produced: true },
    ])
    expect(normalized.outputs[0]).toMatchObject({
      lovelace: huge.toString(),
      datum_hash: 'datum-hash',
      inline_datum: 'inline-datum-cbor',
    })
    expect(JSON.parse(normalized.outputs[0]?.reference_script_json ?? '')).toMatchObject({
      json: { atLeast: metadataHuge.toString() },
    })
    expect(normalized.outputAssets).toEqual(expect.arrayContaining([
      expect.objectContaining({
        policy_id: 'policyA',
        asset_name: '',
        quantity: huge.toString(),
        is_produced: false,
      }),
      expect.objectContaining({
        policy_id: 'policyB',
        asset_name: 'cafe',
        quantity: '7',
        is_produced: true,
      }),
    ]))
    expect(normalized.mintAssets[0]).toMatchObject({
      policy_id: 'policyMint',
      asset_name: 'token',
      quantity: (-huge).toString(),
      is_applied: false,
    })
  })

  it('normalizes Dijkstra sub-transactions and inherits host validity', () => {
    const host: Transaction = {
      id: 'host-tx',
      spends: 'inputs',
      inputs: [reference('host-source', 0)],
      references: [reference('host-reference', 1)],
      collaterals: [reference('unused-collateral', 2)],
      outputs: [output('addr_host', 10n)],
      collateralReturn: output('addr_unused_return', 3n),
      subTransactions: [
        {
          id: 'sub-tx',
          inputs: [reference('sub-source', 3)],
          references: [reference('sub-reference', 4)],
          outputs: [{
            address: 'addr_sub',
            value: {
              ada: { lovelace: 20n },
              subPolicy: { subAsset: 9007199254740993120n },
            },
          }],
          mint: { subMintPolicy: { subMintAsset: -9007199254740993121n } },
          validityInterval: { invalidBefore: 100, invalidAfter: 200 },
          metadata: {
            hash: 'sub-metadata',
            labels: { '2': { json: 900719925474099312347n } },
          },
          signatories: [],
        },
      ],
      signatories: [],
    }
    const normalized = normalizeBlock(praosBlock('dijkstra', [host]), {
      networkMagic,
      eventSeq: 9n,
    })

    expect(normalized.block.tx_count).toBe(1)
    expect(normalized.transactions).toHaveLength(2)
    expect(normalized.transactions[1]).toMatchObject({
      tx_hash: 'sub-tx',
      tx_index: 0,
      parent_tx_hash: 'host-tx',
      subtx_index: 0,
      is_valid: true,
      is_applied: true,
      fee_lovelace: null,
      invalid_before: 100,
      invalid_after: 200,
    })
    expect(JSON.parse(normalized.transactions[1]?.metadata_json ?? '')).toMatchObject({
      labels: { '2': { json: '900719925474099312347' } },
    })

    expect(normalized.inputs).toEqual(expect.arrayContaining([
      expect.objectContaining({ tx_hash: 'sub-tx', input_kind: 'spend', is_consumed: true }),
      expect.objectContaining({ tx_hash: 'sub-tx', input_kind: 'reference', is_consumed: false }),
      expect.objectContaining({ tx_hash: 'host-tx', input_kind: 'collateral', is_consumed: false }),
    ]))
    expect(normalized.outputs).toEqual(expect.arrayContaining([
      expect.objectContaining({ tx_hash: 'sub-tx', output_kind: 'regular', is_produced: true }),
      expect.objectContaining({
        tx_hash: 'host-tx',
        output_kind: 'collateral_return',
        is_produced: false,
      }),
    ]))
    expect(normalized.outputAssets).toContainEqual(expect.objectContaining({
      tx_hash: 'sub-tx',
      policy_id: 'subPolicy',
      asset_name: 'subAsset',
      quantity: '9007199254740993120',
      is_produced: true,
    }))
    expect(normalized.mintAssets).toContainEqual(expect.objectContaining({
      tx_hash: 'sub-tx',
      quantity: '-9007199254740993121',
      is_applied: true,
    }))
  })

  it('rejects quantities that cannot be stored without truncation', () => {
    const tooLarge = 1n << 64n
    const transaction: Transaction = {
      id: 'overflow-tx',
      spends: 'inputs',
      inputs: [],
      outputs: [output('addr_overflow', tooLarge)],
      signatories: [],
    }

    expect(() => normalizeBlock(praosBlock('babbage', [transaction]), {
      networkMagic,
      eventSeq: 10n,
    })).toThrow(/outside ClickHouse UInt64/)
  })
})

function reference(transactionId: string, index: number) {
  return { transaction: { id: transactionId }, index }
}

function output(address: string, lovelace: bigint) {
  return { address, value: { ada: { lovelace } } }
}

function praosBlock(era: BlockPraos['era'], transactions: Transaction[]): BlockPraos {
  return {
    type: 'praos',
    era,
    id: `${era}-block`,
    ancestor: 'genesis',
    height: 100,
    size: { bytes: 1024 },
    slot: 999,
    transactions,
    protocol: { version: { major: 10, minor: 0 } },
    issuer: {
      verificationKey: 'issuer-vkey',
      vrfVerificationKey: 'vrf-vkey',
      operationalCertificate: {
        count: 1,
        kes: { period: 2, verificationKey: 'kes-vkey' },
      },
      leaderValue: {},
    },
  }
}
