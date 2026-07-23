import { createHash } from 'node:crypto'
import { readdir, readFile } from 'node:fs/promises'
import { resolve } from 'node:path'
import { createClient, type ClickHouseClient } from '@clickhouse/client'
import type { ClickHouseConfig } from '../config.js'
import type {
  CanonicalPointRow,
  ChainEventRow,
  ChainPoint,
  NormalizedBlock,
  RollbackRow,
} from '../domain.js'
import type { ChainStore } from './store.js'

interface MigrationRow {
  version: string
  checksum: string
}

interface SequenceRow {
  event_seq: string
}

interface CountRow {
  count: string
}

interface CanonicalPointResultRow extends Omit<CanonicalPointRow, 'canonical_event_seq'> {
  canonical_event_seq_text: string
}

function splitStatements(sql: string): string[] {
  const withoutLineComments = sql
    .split('\n')
    .filter((line) => !line.trimStart().startsWith('--'))
    .join('\n')
  return withoutLineComments
    .split(';')
    .map((statement) => statement.trim())
    .filter((statement) => statement.length > 0)
}

function serializePoint(point: ChainPoint): { hash: string | null; slot: number | null } {
  return point === 'origin' ? { hash: null, slot: null } : { hash: point.id, slot: point.slot }
}

export class ClickHouseStore implements ChainStore {
  readonly #config: ClickHouseConfig
  readonly #schemaDirectory: string
  readonly #admin: ClickHouseClient
  readonly #client: ClickHouseClient

  constructor(config: ClickHouseConfig, schemaDirectory: string) {
    this.#config = config
    this.#schemaDirectory = schemaDirectory
    const common = {
      url: config.url,
      username: config.username,
      password: config.password,
      request_timeout: config.requestTimeoutMs,
      application: 'clicksync',
      compression: { request: { codec: 'gzip' as const }, response: true },
      clickhouse_settings: {
        // ClickHouse 26.3 enables async inserts by default. Commit-marker ordering
        // requires the server response to mean the rows are already visible.
        async_insert: 0 as const,
        wait_for_async_insert: 1 as const,
      },
    }
    this.#admin = createClient(common)
    this.#client = createClient({ ...common, database: config.database })
  }

  async ping(): Promise<void> {
    const result = await this.#client.ping({ select: true })
    if (!result.success) throw result.error
  }

  async close(): Promise<void> {
    await Promise.all([this.#client.close(), this.#admin.close()])
  }

  async migrate(): Promise<void> {
    const database = this.#config.database
    await this.#admin.command({ query: `CREATE DATABASE IF NOT EXISTS ${database}` })
    await this.#client.command({
      query: `
        CREATE TABLE IF NOT EXISTS schema_migrations
        (
          version String,
          checksum FixedString(64),
          applied_at DateTime64(3, 'UTC') DEFAULT now64(3)
        )
        ENGINE = MergeTree
        ORDER BY (version, applied_at)
      `,
    })

    const appliedResult = await this.#client.query({
      query: `
        SELECT version, argMax(checksum, applied_at) AS checksum
        FROM schema_migrations
        GROUP BY version
      `,
      format: 'JSONEachRow',
    })
    const applied = new Map(
      (await appliedResult.json<MigrationRow>()).map((row) => [row.version, row.checksum]),
    )

    const directory = resolve(this.#schemaDirectory)
    const files = (await readdir(directory)).filter((file) => file.endsWith('.sql')).sort()
    for (const file of files) {
      const version = file.slice(0, -4)
      const source = await readFile(resolve(directory, file), 'utf8')
      const checksum = createHash('sha256').update(source).digest('hex')
      const previousChecksum = applied.get(version)
      if (previousChecksum !== undefined) {
        if (previousChecksum !== checksum) {
          throw new Error(`migration ${version} changed after it was applied`)
        }
        continue
      }

      const rendered = source.replaceAll('__DATABASE__', database)
      for (const statement of splitStatements(rendered)) {
        await this.#client.command({ query: statement })
      }
      await this.#client.insert({
        table: 'schema_migrations',
        values: [{ version, checksum }],
        format: 'JSONEachRow',
      })
    }
  }

  async maxEventSequence(networkMagic: number): Promise<bigint> {
    const result = await this.#client.query({
      query: `
        SELECT toString(max(event_seq)) AS event_seq
        FROM
        (
          SELECT event_seq
          FROM chain_events
          WHERE network_magic = {network_magic:UInt32}
          UNION ALL
          SELECT event_seq
          FROM rollbacks FINAL
          WHERE network_magic = {network_magic:UInt32}
          UNION ALL
          SELECT toUInt64(0) AS event_seq
        )
      `,
      query_params: { network_magic: networkMagic },
      format: 'JSONEachRow',
    })
    const [row] = await result.json<SequenceRow>()
    return BigInt(row?.event_seq ?? '0')
  }

  async currentTip(networkMagic: number): Promise<CanonicalPointRow | null> {
    const rows = await this.#canonicalPoints(networkMagic, 1, false)
    return rows[0] ?? null
  }

  async intersectionPoints(
    networkMagic: number,
    limit: number,
    denseLimit: number,
    tipBlockNumber: number,
  ): Promise<CanonicalPointRow[]> {
    const dense = Math.min(denseLimit, Math.max(1, limit - 1))
    const buckets = Math.max(1, limit - dense)
    const denseFloor = Math.max(0, tipBlockNumber - dense + 1)
    const bucketWidth = Math.max(1, Math.ceil(Math.max(1, denseFloor) / buckets))
    const result = await this.#client.query({
      query: `
        SELECT
          argMax(c.block_hash, c.canonical_event_seq) AS block_hash,
          argMax(c.slot, c.canonical_event_seq) AS slot,
          argMax(c.block_number, c.canonical_event_seq) AS block_number,
          toString(max(c.canonical_event_seq)) AS canonical_event_seq_text
        FROM current_chain AS c
        WHERE c.network_magic = {network_magic:UInt32} AND c.slot IS NOT NULL
        GROUP BY
          c.block_number >= {dense_floor:UInt64},
          if(
            c.block_number >= {dense_floor:UInt64},
            c.block_number,
            intDiv(c.block_number, {bucket_width:UInt64})
          )
        ORDER BY max(c.canonical_event_seq) DESC
        LIMIT {limit:UInt32}
      `,
      query_params: {
        network_magic: networkMagic,
        dense_floor: denseFloor,
        bucket_width: bucketWidth,
        limit,
      },
      format: 'JSONEachRow',
    })
    return (await result.json<CanonicalPointResultRow>()).map((row) => ({
      block_hash: row.block_hash,
      slot: row.slot,
      block_number: row.block_number,
      canonical_event_seq: row.canonical_event_seq_text,
    }))
  }

  async #canonicalPoints(
    networkMagic: number,
    limit: number,
    requireSlot: boolean,
  ): Promise<CanonicalPointRow[]> {
    const result = await this.#client.query({
      query: `
        SELECT
          block_hash,
          slot,
          block_number,
          toString(canonical_event_seq) AS canonical_event_seq_text
        FROM current_chain
        WHERE network_magic = {network_magic:UInt32}
          ${requireSlot ? 'AND slot IS NOT NULL' : ''}
        ORDER BY canonical_event_seq DESC
        LIMIT {limit:UInt32}
      `,
      query_params: { network_magic: networkMagic, limit },
      format: 'JSONEachRow',
    })
    return (await result.json<CanonicalPointResultRow>()).map((row) => ({
      block_hash: row.block_hash,
      slot: row.slot,
      block_number: row.block_number,
      canonical_event_seq: row.canonical_event_seq_text,
    }))
  }

  async #rollbackTargetSequence(networkMagic: number, point: ChainPoint): Promise<string | null> {
    const target = serializePoint(point)
    if (target.hash === null) return null

    const targetResult = await this.#client.query({
      query: `
        SELECT toString(canonical_event_seq) AS event_seq
        FROM current_chain
        WHERE network_magic = {network_magic:UInt32}
          AND block_hash = {block_hash:String}
          AND slot = {slot:UInt64}
        LIMIT 1
      `,
      query_params: {
        network_magic: networkMagic,
        block_hash: target.hash,
        slot: target.slot,
      },
      format: 'JSONEachRow',
    })
    const [row] = await targetResult.json<SequenceRow>()
    if (row === undefined) {
      throw new Error(`rollback target ${target.slot}/${target.hash} is not canonical`)
    }
    return row.event_seq
  }

  async rollbackDescendantCount(networkMagic: number, point: ChainPoint): Promise<number> {
    const targetSequence = await this.#rollbackTargetSequence(networkMagic, point)
    const result = await this.#client.query({
      query: `
        SELECT toString(count()) AS count
        FROM current_chain
        WHERE network_magic = {network_magic:UInt32}
          ${targetSequence === null ? '' : 'AND canonical_event_seq > {target_sequence:UInt64}'}
      `,
      query_params: {
        network_magic: networkMagic,
        ...(targetSequence === null ? {} : { target_sequence: targetSequence }),
      },
      format: 'JSONEachRow',
    })
    const [row] = await result.json<CountRow>()
    const count = Number(row?.count ?? '0')
    if (!Number.isSafeInteger(count)) throw new Error(`rollback depth is not a safe integer: ${row?.count}`)
    return count
  }

  async publishBlocks(
    blocks: readonly NormalizedBlock[],
    events: readonly ChainEventRow[],
  ): Promise<void> {
    await this.#insert('blocks', blocks.map((block) => block.block))
    await this.#insert('transactions', blocks.flatMap((block) => block.transactions))
    await this.#insert('tx_inputs', blocks.flatMap((block) => block.inputs))
    await this.#insert('tx_outputs', blocks.flatMap((block) => block.outputs))
    await this.#insert('output_assets', blocks.flatMap((block) => block.outputAssets))
    await this.#insert('mint_assets', blocks.flatMap((block) => block.mintAssets))
    // Publish the batch last. All canonical views join through these markers.
    await this.#insert('chain_events', events)
  }

  async publishRollback(rollback: RollbackRow, point: ChainPoint): Promise<void> {
    const targetSequence = await this.#rollbackTargetSequence(rollback.network_magic, point)
    // Keep arbitrarily deep membership construction inside ClickHouse. The
    // invalidations remain inert until the rollback header below is visible.
    await this.#client.command({
      query: `
        INSERT INTO chain_events
          (network_magic, event_seq, block_hash, slot, block_number,
           is_canonical, rollback_id, writer_id)
        SELECT
          network_magic,
          {event_seq:UInt64},
          block_hash,
          slot,
          block_number,
          false,
          {rollback_id:UUID},
          {writer_id:UUID}
        FROM current_chain
        WHERE network_magic = {network_magic:UInt32}
          ${targetSequence === null ? '' : 'AND canonical_event_seq > {target_sequence:UInt64}'}
      `,
      query_params: {
        network_magic: rollback.network_magic,
        event_seq: rollback.event_seq,
        rollback_id: rollback.rollback_id,
        writer_id: rollback.writer_id,
        ...(targetSequence === null ? {} : { target_sequence: targetSequence }),
      },
    })
    await this.#insert('rollbacks', [rollback])
  }

  async #insert<T extends object>(table: string, values: readonly T[]): Promise<void> {
    if (values.length === 0) return
    await this.#client.insert({ table, values, format: 'JSONEachRow' })
  }
}
