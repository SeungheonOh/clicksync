#!/usr/bin/env node

import { setTimeout as delay } from 'node:timers/promises'
import { Command } from 'commander'
import { loadConfig } from './config.js'
import { Ingestor, RollbackDepthExceededError } from './ingest.js'
import { logger } from './logger.js'
import { ClickHouseStore } from './storage/clickhouse.js'

const program = new Command()
  .name('clicksync')
  .description('Append-only Cardano chain indexer for ClickHouse')
  .version('0.1.0')

program
  .command('migrate')
  .description('Create or upgrade the ClickHouse schema')
  .action(async () => {
    const config = loadConfig()
    const store = new ClickHouseStore(config.clickhouse, config.schemaDirectory)
    try {
      await store.migrate()
      logger.info({ database: config.clickhouse.database }, 'ClickHouse schema is current')
    } finally {
      await store.close()
    }
  })

program
  .command('status')
  .description('Check ClickHouse and print the current canonical tip')
  .action(async () => {
    const config = loadConfig()
    const store = new ClickHouseStore(config.clickhouse, config.schemaDirectory)
    try {
      await store.ping()
      const [tip, eventSequence] = await Promise.all([
        store.currentTip(config.networkMagic),
        store.maxEventSequence(config.networkMagic),
      ])
      process.stdout.write(`${JSON.stringify({
        connected: true,
        database: config.clickhouse.database,
        networkMagic: config.networkMagic,
        eventSequence: eventSequence.toString(),
        tip,
      }, null, 2)}\n`)
    } finally {
      await store.close()
    }
  })

program
  .command('sync')
  .description('Follow Cardano Chain Sync through Ogmios and index into ClickHouse')
  .option('--no-migrate', 'do not run migrations before syncing')
  .action(async (options: { migrate: boolean }) => {
    const config = loadConfig()
    const store = new ClickHouseStore(config.clickhouse, config.schemaDirectory)
    const abortController = new AbortController()
    const stop = (signal: string) => {
      if (abortController.signal.aborted) return
      logger.info({ signal }, 'shutdown requested')
      abortController.abort()
    }
    process.once('SIGINT', () => stop('SIGINT'))
    process.once('SIGTERM', () => stop('SIGTERM'))

    try {
      if (options.migrate) await store.migrate()
      const { OgmiosChainSource, OgmiosConnectionTimeoutError } =
        await import('./cardano/ogmios.js')
      const ingestor = new Ingestor(store, config.networkMagic, {
        batchBlocks: config.batchBlocks,
        maxRollbackDepth: config.maxRollbackDepth,
      })
      await ingestor.initialize()

      let retrySeconds = 1
      while (!abortController.signal.aborted) {
        try {
          const source = new OgmiosChainSource(config.ogmios, config.networkMagic)
          await ingestor.run(source, config.intersectionCandidates, abortController.signal)
          retrySeconds = 1
        } catch (error) {
          if (abortController.signal.aborted) break
          if (
            error instanceof RollbackDepthExceededError
            || error instanceof OgmiosConnectionTimeoutError
          ) {
            throw error
          }
          logger.error({ error, retrySeconds }, 'chain source stopped; reconnecting')
          await delay(retrySeconds * 1000, undefined, { signal: abortController.signal }).catch(() => undefined)
          retrySeconds = Math.min(retrySeconds * 2, 30)
        }
      }
    } finally {
      await store.close()
    }
  })

await program.parseAsync().catch((error: unknown) => {
  logger.fatal({ error }, 'clicksync failed')
  process.exitCode = 1
})
