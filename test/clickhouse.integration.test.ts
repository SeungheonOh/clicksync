import { createClient } from '@clickhouse/client'
import { randomUUID } from 'node:crypto'
import type { BlockPraos, Transaction } from '@cardano-ogmios/schema'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { loadConfig } from '../src/config.js'
import { Ingestor } from '../src/ingest.js'
import { ClickHouseStore } from '../src/storage/clickhouse.js'

const enabled = process.env.CLICKHOUSE_INTEGRATION === '1'

describe.skipIf(!enabled)('ClickHouse canonical UTxO integration', () => {
  const config = loadConfig()
  const networkMagic = 4_000_000_000 + Math.floor(Math.random() * 100_000_000)
  const store = new ClickHouseStore(config.clickhouse, config.schemaDirectory)
  const client = createClient({
    url: config.clickhouse.url,
    database: config.clickhouse.database,
    username: config.clickhouse.username,
    password: config.clickhouse.password,
  })
  const ingestor = new Ingestor(store, networkMagic, {
    writerId: '00000000-0000-4000-8000-000000000001',
    batchBlocks: 2,
  })

  beforeAll(async () => {
    await store.migrate()
    await ingestor.initialize()
  })

  afterAll(async () => {
    await Promise.all([store.close(), client.close()])
  })

  it('removes orphan outputs and resurrects spends on rollback', async () => {
    const create: Transaction = {
      id: 'tx-create',
      spends: 'inputs',
      inputs: [],
      outputs: [output('addr_one', 100n)],
      signatories: [],
    }
    const blockOne = block('block-one', 'genesis', 1, 10, [create])

    const spend: Transaction = {
      id: 'tx-spend',
      spends: 'inputs',
      inputs: [{ transaction: { id: 'tx-create' }, index: 0 }],
      outputs: [output('addr_two', 90n)],
      signatories: [],
    }
    const blockTwo = block('block-two', 'block-one', 2, 20, [spend])

    const spendAgain: Transaction = {
      id: 'tx-spend-again',
      spends: 'inputs',
      inputs: [{ transaction: { id: 'tx-spend' }, index: 0 }],
      outputs: [output('addr_three', 80n)],
      signatories: [],
    }
    const blockThree = block('block-three', 'block-two', 3, 30, [spendAgain])

    const initialNodeTip = { id: 'block-three', slot: 30, height: 3 }
    await ingestor.rollForward(blockOne, initialNodeTip)
    await ingestor.rollForward(blockTwo, initialNodeTip)
    await ingestor.rollForward(blockThree, initialNodeTip)
    expect(await utxos()).toEqual([{
      tx_hash: 'tx-spend-again',
      output_index: 0,
      address: 'addr_three',
    }])
    expect(await addressUtxos('addr_three')).toHaveLength(1)

    await ingestor.rollBackward({ id: 'block-one', slot: 10 }, 'origin')
    expect(await utxos()).toEqual([{ tx_hash: 'tx-create', output_index: 0, address: 'addr_one' }])
    expect(await addressUtxos('addr_one')).toEqual([{
      tx_hash: 'tx-create',
      output_index: 0,
      address: 'addr_one',
    }])
    expect(await addressUtxos('addr_two')).toEqual([])
    expect(await addressUtxos('addr_three')).toEqual([])

    // A block can be adopted again. An "ever rolled back" anti-join would get
    // this wrong, while the latest canonicality event makes it visible again.
    const reAdoptionTip = { id: 'block-two', slot: 20, height: 2 }
    await ingestor.rollForward(blockTwo, reAdoptionTip)
    expect(await utxos()).toEqual([{ tx_hash: 'tx-spend', output_index: 0, address: 'addr_two' }])
    expect(await addressUtxos('addr_one')).toEqual([])

    const rollbackRows = await query<{ depth: number; reason: string }>(`
      SELECT depth, reason
      FROM rollbacks FINAL
      WHERE network_magic = {network_magic:UInt32}
    `)
    expect(rollbackRows).toEqual([{ depth: 2, reason: 'chain_sync' }])

    // Rollback members are inert until their audit/header row is published.
    const rollbackId = randomUUID()
    await client.insert({
      table: 'chain_events',
      values: [{
        network_magic: networkMagic,
        event_seq: '6',
        block_hash: 'block-two',
        slot: 20,
        block_number: 2,
        is_canonical: false,
        rollback_id: rollbackId,
        writer_id: '00000000-0000-4000-8000-000000000001',
      }],
      format: 'JSONEachRow',
    })
    expect(await utxos()).toEqual([{ tx_hash: 'tx-spend', output_index: 0, address: 'addr_two' }])
    expect(await addressUtxos('addr_two')).toHaveLength(1)

    await client.insert({
      table: 'rollbacks',
      values: [{
        network_magic: networkMagic,
        rollback_id: rollbackId,
        rollback_to_hash: 'block-one',
        rollback_to_slot: 10,
        old_tip_hash: 'block-two',
        old_tip_slot: 20,
        depth: 1,
        event_seq: '6',
        reason: 'chain_sync',
        writer_id: '00000000-0000-4000-8000-000000000001',
      }],
      format: 'JSONEachRow',
    })
    expect(await utxos()).toEqual([{ tx_hash: 'tx-create', output_index: 0, address: 'addr_one' }])
    expect(await addressUtxos('addr_one')).toHaveLength(1)

    const tip = await store.currentTip(networkMagic)
    const intersections = await store.intersectionPoints(
      networkMagic,
      4096,
      2_161,
      tip?.block_number ?? 0,
    )
    expect(intersections[0]).toMatchObject({ block_hash: 'block-one', slot: 10 })
  })

  async function utxos() {
    return query<{ tx_hash: string; output_index: number; address: string }>(`
      SELECT tx_hash, output_index, address
      FROM current_utxos
      WHERE network_magic = {network_magic:UInt32}
      ORDER BY tx_hash, output_index
    `)
  }

  async function addressUtxos(address: string) {
    return query<{ tx_hash: string; output_index: number; address: string }>(`
      SELECT tx_hash, output_index, address
      FROM current_utxos_by_address(
        network_magic = {network_magic:UInt32},
        address = {address:String}
      )
      ORDER BY tx_hash, output_index
    `, { address })
  }

  async function query<T>(sql: string, params: Record<string, unknown> = {}): Promise<T[]> {
    const result = await client.query({
      query: sql,
      query_params: { network_magic: networkMagic, ...params },
      format: 'JSONEachRow',
    })
    return result.json<T>()
  }
})

function output(address: string, lovelace: bigint) {
  return { address, value: { ada: { lovelace } } }
}

function block(
  id: string,
  ancestor: string | 'genesis',
  height: number,
  slot: number,
  transactions: Transaction[],
): BlockPraos {
  return {
    type: 'praos',
    era: 'conway',
    id,
    ancestor,
    height,
    slot,
    size: { bytes: 1024 },
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
