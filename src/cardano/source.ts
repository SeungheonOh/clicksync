import type { Block, Point, Tip } from '@cardano-ogmios/schema'
import type { ChainPoint } from '../domain.js'

export interface ChainSourceHandlers {
  intersect(point: ChainPoint, tip: Tip | 'origin'): Promise<void>
  rollForward(block: Block, tip: Tip | 'origin'): Promise<void>
  rollBackward(point: ChainPoint, tip: Tip | 'origin'): Promise<void>
}

export interface ChainSource {
  run(points: Array<Point | 'origin'>, handlers: ChainSourceHandlers, signal: AbortSignal): Promise<void>
}
