import { randomUUID } from 'node:crypto'
import type { Block, Point, Tip } from '@cardano-ogmios/schema'
import { normalizeBlock } from './cardano/normalize.js'
import type { ChainSource, ChainSourceHandlers } from './cardano/source.js'
import type {
  CanonicalPointRow,
  ChainEventRow,
  ChainPoint,
  NormalizedBlock,
  RollbackRow,
} from './domain.js'
import { logger } from './logger.js'
import type { ChainStore } from './storage/store.js'

export function sampleIntersectionPoints(rows: readonly CanonicalPointRow[]): Array<Point | 'origin'> {
  if (rows.length === 0) return ['origin']

  const points: Array<Point | 'origin'> = []
  for (const row of rows) {
    if (row.slot !== null) {
      points.push({ id: row.block_hash, slot: row.slot })
    }
  }
  points.push('origin')
  return points
}

export class RollbackDepthExceededError extends Error {
  constructor(depth: number, maximum: number) {
    super(
      `rollback depth ${depth} exceeds CLICKSYNC_MAX_ROLLBACK_DEPTH=${maximum}; `
      + 'refusing to invalidate an established database from a stale or divergent node',
    )
    this.name = 'RollbackDepthExceededError'
  }
}

export interface IngestorOptions {
  writerId?: string
  batchBlocks?: number
  maxRollbackDepth?: number
}

export class Ingestor implements ChainSourceHandlers {
  readonly #store: ChainStore
  readonly #networkMagic: number
  readonly #writerId: string
  readonly #batchBlocks: number
  readonly #maxRollbackDepth: number
  #eventSequence = 0n
  #tip: { hash: string; slot: number | null; blockNumber: number | null } | null = null
  #pendingBlocks: NormalizedBlock[] = []
  #pendingEvents: ChainEventRow[] = []

  constructor(store: ChainStore, networkMagic: number, options: IngestorOptions = {}) {
    this.#store = store
    this.#networkMagic = networkMagic
    this.#writerId = options.writerId ?? randomUUID()
    this.#batchBlocks = options.batchBlocks ?? 250
    this.#maxRollbackDepth = options.maxRollbackDepth ?? 2_160
    if (!Number.isSafeInteger(this.#batchBlocks) || this.#batchBlocks < 1) {
      throw new Error('batchBlocks must be a positive safe integer')
    }
    if (!Number.isSafeInteger(this.#maxRollbackDepth) || this.#maxRollbackDepth < 1) {
      throw new Error('maxRollbackDepth must be a positive safe integer')
    }
  }

  async initialize(): Promise<void> {
    this.#discardPending()
    this.#eventSequence = await this.#store.maxEventSequence(this.#networkMagic)
    const tip = await this.#store.currentTip(this.#networkMagic)
    this.#tip = tip === null
      ? null
      : { hash: tip.block_hash, slot: tip.slot, blockNumber: tip.block_number }
  }

  async run(source: ChainSource, candidateLimit: number, signal: AbortSignal): Promise<void> {
    this.#discardPending()
    // Resolve the tip first so its global canonical scan does not run at the
    // same time as the intentionally broader intersection sampling query.
    const tip = await this.#store.currentTip(this.#networkMagic)
    const [candidates, storedSequence] = await Promise.all([
      this.#store.intersectionPoints(
        this.#networkMagic,
        candidateLimit,
        this.#maxRollbackDepth + 1,
        tip?.block_number ?? 0,
      ),
      this.#store.maxEventSequence(this.#networkMagic),
    ])
    if (storedSequence > this.#eventSequence) this.#eventSequence = storedSequence
    this.#tip = tip === null
      ? null
      : { hash: tip.block_hash, slot: tip.slot, blockNumber: tip.block_number }
    await source.run(sampleIntersectionPoints(candidates), this, signal)
  }

  async intersect(point: ChainPoint, _nodeTip: Tip | 'origin'): Promise<void> {
    await this.#rollbackTo(point, 'intersection')
  }

  async rollBackward(point: ChainPoint, _nodeTip: Tip | 'origin'): Promise<void> {
    await this.#rollbackTo(point, 'chain_sync')
  }

  async rollForward(block: Block, nodeTip: Tip | 'origin'): Promise<void> {
    const expectedParent = block.ancestor === 'genesis' ? null : block.ancestor
    const actualParent = this.#tip?.hash ?? null
    // Byron's first EBB/BFT block points at the genesis configuration hash,
    // not the literal `genesis`. After intersecting at origin, the first block
    // is therefore trusted; every subsequent parent must match exactly.
    if (actualParent !== null && expectedParent !== actualParent) {
      throw new Error(
        `non-contiguous roll forward ${block.id}: expected parent ${String(actualParent)}, got ${String(expectedParent)}`,
      )
    }

    const eventSeq = this.#nextSequence()
    const normalized = normalizeBlock(block, {
      networkMagic: this.#networkMagic,
      eventSeq,
    })
    const event: ChainEventRow = {
      network_magic: this.#networkMagic,
      event_seq: eventSeq.toString(),
      block_hash: normalized.block.block_hash,
      slot: normalized.block.slot,
      block_number: normalized.block.block_number,
      is_canonical: true,
      rollback_id: null,
      writer_id: this.#writerId,
    }

    this.#pendingBlocks.push(normalized)
    this.#pendingEvents.push(event)
    this.#tip = {
      hash: normalized.block.block_hash,
      slot: normalized.block.slot,
      blockNumber: normalized.block.block_number,
    }

    const isNodeTip = nodeTip !== 'origin' && nodeTip.id === normalized.block.block_hash
    if (this.#pendingBlocks.length >= this.#batchBlocks || isNodeTip) {
      await this.#flushPending()
    }

    if (normalized.block.block_number % 10_000 === 0) {
      logger.info(
        {
          blockNumber: normalized.block.block_number,
          slot: normalized.block.slot,
          hash: normalized.block.block_hash,
        },
        'chain sync progress',
      )
    }
  }

  async #rollbackTo(point: ChainPoint, reason: RollbackRow['reason']): Promise<void> {
    await this.#flushPending()
    const targetHash = point === 'origin' ? null : point.id
    const targetSlot = point === 'origin' ? null : point.slot
    if (this.#tip?.hash === targetHash || (this.#tip === null && point === 'origin')) return

    const depth = await this.#store.rollbackDescendantCount(this.#networkMagic, point)
    if (depth > this.#maxRollbackDepth) {
      throw new RollbackDepthExceededError(depth, this.#maxRollbackDepth)
    }
    const eventSeq = this.#nextSequence()
    const rollbackId = randomUUID()
    const rollback: RollbackRow = {
      network_magic: this.#networkMagic,
      rollback_id: rollbackId,
      rollback_to_hash: targetHash,
      rollback_to_slot: targetSlot,
      old_tip_hash: this.#tip?.hash ?? null,
      old_tip_slot: this.#tip?.slot ?? null,
      depth,
      event_seq: eventSeq.toString(),
      reason,
      writer_id: this.#writerId,
    }
    await this.#store.publishRollback(rollback, point)
    this.#tip = targetHash === null
      ? null
      : { hash: targetHash, slot: targetSlot, blockNumber: null }
    logger.warn(
      { rollbackId, reason, depth, point },
      'canonical chain rolled back',
    )
  }

  async #flushPending(): Promise<void> {
    if (this.#pendingBlocks.length === 0) return
    await this.#store.publishBlocks(this.#pendingBlocks, this.#pendingEvents)
    this.#pendingBlocks = []
    this.#pendingEvents = []
  }

  #discardPending(): void {
    this.#pendingBlocks = []
    this.#pendingEvents = []
  }

  #nextSequence(): bigint {
    this.#eventSequence += 1n
    return this.#eventSequence
  }
}
