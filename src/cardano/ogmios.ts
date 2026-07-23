import {
  ChainSynchronization,
  createConnectionObject,
  createChainSynchronizationClient,
  createInteractionContext,
  getServerHealth,
} from '@cardano-ogmios/client'
import type { Point } from '@cardano-ogmios/schema'
import type { OgmiosConfig } from '../config.js'
import { logger } from '../logger.js'
import type { ChainSource, ChainSourceHandlers } from './source.js'

const ABORTED = Symbol('aborted')
const TIMED_OUT = Symbol('timed-out')

export class OgmiosConnectionTimeoutError extends Error {
  constructor(milliseconds: number) {
    super(`Ogmios connection timed out after ${milliseconds}ms`)
    this.name = 'OgmiosConnectionTimeoutError'
  }
}

function abortPromise(signal: AbortSignal): {
  promise: Promise<typeof ABORTED>
  dispose: () => void
} {
  if (signal.aborted) {
    return { promise: Promise.resolve(ABORTED), dispose: () => undefined }
  }

  let listener: (() => void) | undefined
  const promise = new Promise<typeof ABORTED>((resolve) => {
    listener = () => resolve(ABORTED)
    signal.addEventListener('abort', listener, { once: true })
  })
  return {
    promise,
    dispose: () => {
      if (listener !== undefined) signal.removeEventListener('abort', listener)
    },
  }
}

function timeoutPromise(milliseconds: number): {
  promise: Promise<typeof TIMED_OUT>
  dispose: () => void
} {
  let timer: NodeJS.Timeout | undefined
  const promise = new Promise<typeof TIMED_OUT>((resolve) => {
    timer = setTimeout(() => resolve(TIMED_OUT), milliseconds)
  })
  return {
    promise,
    dispose: () => {
      if (timer !== undefined) clearTimeout(timer)
    },
  }
}

function networkMagics(name: string): number[] | null {
  const known: Record<string, number[]> = {
    mainnet: [764_824_073, 0],
    preprod: [1],
    preview: [2],
    sanchonet: [4],
    'catastrophically-broken-testnet': [1_097_911_063],
  }
  if (known[name] !== undefined) return known[name]

  const unknown = /^unknown \((\d+)\)$/.exec(name)
  if (unknown?.[1] !== undefined) {
    const magic = Number(unknown[1])
    if (Number.isSafeInteger(magic) && magic >= 0 && magic <= 0xffff_ffff) return [magic]
  }
  return null
}

function closeSocket(socket: unknown): void {
  (socket as { close: () => void }).close()
}

export class OgmiosChainSource implements ChainSource {
  readonly #config: OgmiosConfig
  readonly #networkMagic: number

  constructor(config: OgmiosConfig, networkMagic: number) {
    this.#config = config
    this.#networkMagic = networkMagic
  }

  async run(
    points: Array<Point | 'origin'>,
    handlers: ChainSourceHandlers,
    signal: AbortSignal,
  ): Promise<void> {
    if (signal.aborted) return

    let stopping = false
    let activeHandlers = 0
    let resolveDrained: () => void = () => undefined
    const drained = new Promise<void>((resolve) => {
      resolveDrained = resolve
    })
    const stopProcessing = () => {
      stopping = true
      if (activeHandlers === 0) resolveDrained()
    }

    let rejectFailure: (error: Error) => void = () => undefined
    const failed = new Promise<never>((_resolve, reject) => {
      rejectFailure = reject
    })
    // Connection setup can be aborted before `failed` enters a race.
    void failed.catch(() => undefined)
    const fail = (error: Error) => {
      stopProcessing()
      rejectFailure(error)
    }

    const runHandler = async (operation: () => Promise<void>, nextBlock: () => void) => {
      if (stopping) return
      activeHandlers += 1
      try {
        await operation()
        if (!stopping) nextBlock()
      } catch (error) {
        fail(error instanceof Error ? error : new Error(String(error)))
      } finally {
        activeHandlers -= 1
        if (stopping && activeHandlers === 0) resolveDrained()
      }
    }

    const connectionConfig = {
      host: this.#config.host,
      port: this.#config.port,
      tls: this.#config.tls,
      maxPayload: this.#config.maxPayloadBytes,
    }
    const abort = abortPromise(signal)
    const timeout = timeoutPromise(this.#config.connectTimeoutMs)
    let context: Awaited<ReturnType<typeof createInteractionContext>> | undefined
    let client: Awaited<ReturnType<typeof createChainSynchronizationClient>> | undefined
    try {
      const connectionAttempt = (async () => {
        const health = await getServerHealth({ connection: createConnectionObject(connectionConfig) })
        const advertised = networkMagics(String(health.network))
        if (advertised === null) {
          logger.warn({ network: health.network }, 'cannot validate Ogmios network magic from health name')
        } else if (!advertised.includes(this.#networkMagic)) {
          throw new Error(
            `Ogmios is connected to ${health.network} (network magic ${advertised.join(' or ')}), `
            + `but CARDANO_NETWORK_MAGIC is ${this.#networkMagic}`,
          )
        }

        return createInteractionContext(
          (error) => fail(error),
          (code, reason) => fail(new Error(`Ogmios connection closed (${code}): ${reason}`)),
          { connection: connectionConfig },
        )
      })()
      const connectionOutcome = await Promise.race([
        connectionAttempt,
        abort.promise,
        timeout.promise,
      ])
      if (connectionOutcome === ABORTED || connectionOutcome === TIMED_OUT) {
        stopProcessing()
        void connectionAttempt.then((lateContext) => closeSocket(lateContext.socket)).catch(() => undefined)
        if (connectionOutcome === TIMED_OUT) {
          throw new OgmiosConnectionTimeoutError(this.#config.connectTimeoutMs)
        }
        return
      }
      context = connectionOutcome
      timeout.dispose()

      client = await createChainSynchronizationClient(
        context,
        {
          rollForward: ({ block, tip }, nextBlock) =>
            runHandler(() => handlers.rollForward(block, tip), nextBlock),
          rollBackward: ({ point, tip }, nextBlock) =>
            runHandler(() => handlers.rollBackward(point, tip), nextBlock),
        },
        { sequential: true },
      )

      // Find the common point before sending any nextBlock requests. This lets
      // the sink commit an intersection rollback before new blocks arrive.
      const intersection = await Promise.race([
        ChainSynchronization.findIntersection(context, points),
        abort.promise,
        failed,
      ])
      if (intersection === ABORTED) return
      await handlers.intersect(intersection.intersection, intersection.tip)
      if (stopping || signal.aborted) return
      logger.info({ intersection: intersection.intersection, nodeTip: intersection.tip }, 'chain intersection found')

      for (let index = 0; index < this.#config.inFlight; index += 1) {
        ChainSynchronization.nextBlock(context.socket)
      }

      const outcome = await Promise.race([failed, abort.promise])
      if (outcome === ABORTED) stopProcessing()
    } finally {
      stopProcessing()
      abort.dispose()
      timeout.dispose()
      if (client !== undefined) {
        await client.shutdown().catch(() => undefined)
      } else if (context !== undefined) {
        closeSocket(context.socket)
      }
      await drained
    }
  }
}
