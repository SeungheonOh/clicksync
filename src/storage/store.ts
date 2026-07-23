import type {
  CanonicalPointRow,
  ChainEventRow,
  ChainPoint,
  NormalizedBlock,
  RollbackRow,
} from '../domain.js'

export interface ChainStore {
  migrate(): Promise<void>
  close(): Promise<void>
  ping(): Promise<void>
  maxEventSequence(networkMagic: number): Promise<bigint>
  currentTip(networkMagic: number): Promise<CanonicalPointRow | null>
  intersectionPoints(
    networkMagic: number,
    limit: number,
    denseLimit: number,
    tipBlockNumber: number,
  ): Promise<CanonicalPointRow[]>
  rollbackDescendantCount(networkMagic: number, point: ChainPoint): Promise<number>
  publishBlocks(blocks: readonly NormalizedBlock[], events: readonly ChainEventRow[]): Promise<void>
  publishRollback(rollback: RollbackRow, point: ChainPoint): Promise<void>
}
