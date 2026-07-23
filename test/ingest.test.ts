import type { BlockBFT, Tip } from '@cardano-ogmios/schema'
import { describe, expect, it } from 'vitest'

import type {
  CanonicalPointRow,
  ChainEventRow,
  ChainPoint,
  NormalizedBlock,
  RollbackRow,
} from '../src/domain.js'
import {
  Ingestor,
  RollbackDepthExceededError,
  sampleIntersectionPoints,
} from '../src/ingest.js'
import type { ChainStore } from '../src/storage/store.js'

describe('Ingestor', () => {
  it('accepts the first Byron digest ancestor at origin and publishes a bounded batch', async () => {
    const store = new FakeStore()
    const ingestor = new Ingestor(store, 42, { batchBlocks: 2 })
    await ingestor.initialize()

    await ingestor.rollForward(byronBlock('first', 'byron-genesis-hash', 1, 1), tip('later', 9, 9))
    expect(store.batches).toHaveLength(0)

    await ingestor.rollForward(byronBlock('second', 'first', 2, 2), tip('later', 9, 9))
    expect(store.batches).toHaveLength(1)
    expect(store.batches[0]?.blocks.map((block) => block.block.block_hash)).toEqual([
      'first',
      'second',
    ])
    expect(store.batches[0]?.blocks[0]?.block.previous_block_hash).toBe('byron-genesis-hash')
    expect(store.batches[0]?.events.map((event) => event.event_seq)).toEqual(['1', '2'])
  })

  it('flushes a partial batch when Chain Sync reaches the node tip', async () => {
    const store = new FakeStore()
    const ingestor = new Ingestor(store, 42, { batchBlocks: 100 })
    await ingestor.initialize()

    const block = byronBlock('tip-block', 'genesis-digest', 1, 1)
    await ingestor.rollForward(block, tip('tip-block', 1, 1))

    expect(store.batches).toHaveLength(1)
    expect(store.batches[0]?.blocks).toHaveLength(1)
  })

  it('fails closed before publishing a rollback beyond the configured guard', async () => {
    const store = new FakeStore()
    store.tip = pointRow('tip', 10, 10, '10')
    store.rollbackDepth = 11
    const ingestor = new Ingestor(store, 42, { maxRollbackDepth: 10 })
    await ingestor.initialize()

    await expect(ingestor.rollBackward({ id: 'target', slot: 1 }, 'origin'))
      .rejects.toBeInstanceOf(RollbackDepthExceededError)
    expect(store.rollbacks).toHaveLength(0)
  })
})

describe('sampleIntersectionPoints', () => {
  it('preserves ordered database candidates and appends origin', () => {
    const rows = Array.from({ length: 100 }, (_unused, index) =>
      pointRow(`block-${index}`, index, index, String(100 - index)))

    const points = sampleIntersectionPoints(rows)

    expect(points).toHaveLength(101)
    expect(points[0]).toEqual({ id: 'block-0', slot: 0 })
    expect(points[99]).toEqual({ id: 'block-99', slot: 99 })
    expect(points[100]).toBe('origin')
  })
})

class FakeStore implements ChainStore {
  tip: CanonicalPointRow | null = null
  rollbackDepth = 0
  batches: Array<{ blocks: readonly NormalizedBlock[]; events: readonly ChainEventRow[] }> = []
  rollbacks: Array<{ rollback: RollbackRow; point: ChainPoint }> = []

  async migrate(): Promise<void> {}
  async close(): Promise<void> {}
  async ping(): Promise<void> {}
  async maxEventSequence(): Promise<bigint> { return 0n }
  async currentTip(): Promise<CanonicalPointRow | null> { return this.tip }
  async intersectionPoints(): Promise<CanonicalPointRow[]> { return [] }
  async rollbackDescendantCount(): Promise<number> { return this.rollbackDepth }

  async publishBlocks(
    blocks: readonly NormalizedBlock[],
    events: readonly ChainEventRow[],
  ): Promise<void> {
    this.batches.push({ blocks: [...blocks], events: [...events] })
  }

  async publishRollback(rollback: RollbackRow, point: ChainPoint): Promise<void> {
    this.rollbacks.push({ rollback, point })
  }
}

function byronBlock(id: string, ancestor: string, height: number, slot: number): BlockBFT {
  return {
    type: 'bft',
    era: 'byron',
    id,
    ancestor,
    height,
    slot,
    size: { bytes: 128 },
    transactions: [],
    protocol: {
      id: 0,
      version: { major: 1, minor: 0 },
      software: { appName: 'cardano-sl', number: 1 },
    },
    issuer: { verificationKey: 'issuer' },
    delegate: { verificationKey: 'delegate' },
  }
}

function tip(id: string, slot: number, height: number): Tip {
  return { id, slot, height }
}

function pointRow(
  block_hash: string,
  slot: number,
  block_number: number,
  canonical_event_seq: string,
): CanonicalPointRow {
  return { block_hash, slot, block_number, canonical_event_seq }
}
