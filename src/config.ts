export interface ClickHouseConfig {
  url: string
  database: string
  username: string
  password: string
  requestTimeoutMs: number
}

export interface OgmiosConfig {
  host: string
  port: number
  tls: boolean
  connectTimeoutMs: number
  inFlight: number
  maxPayloadBytes: number
}

export interface AppConfig {
  networkMagic: number
  clickhouse: ClickHouseConfig
  ogmios: OgmiosConfig
  batchBlocks: number
  intersectionCandidates: number
  maxRollbackDepth: number
  schemaDirectory: string
}

function integerEnv(name: string, fallback: number, minimum: number, maximum: number): number {
  const raw = process.env[name]
  const value = raw === undefined ? fallback : Number(raw)
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`)
  }
  return value
}

function booleanEnv(name: string, fallback: boolean): boolean {
  const raw = process.env[name]
  if (raw === undefined) return fallback
  if (['1', 'true', 'yes'].includes(raw.toLowerCase())) return true
  if (['0', 'false', 'no'].includes(raw.toLowerCase())) return false
  throw new Error(`${name} must be one of true/false, yes/no, or 1/0`)
}

export function validateDatabaseName(database: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(database)) {
    throw new Error('CLICKHOUSE_DATABASE must be an unquoted ClickHouse identifier')
  }
  return database
}

function networkMagic(): number {
  const magic = integerEnv('CARDANO_NETWORK_MAGIC', 764_824_073, 0, 0xffff_ffff)
  const network = process.env.CARDANO_NETWORK
  const known: Record<string, number> = {
    mainnet: 764_824_073,
    preprod: 1,
    preview: 2,
    sanchonet: 4,
    'catastrophically-broken-testnet': 1_097_911_063,
  }
  const expected = network === undefined ? undefined : known[network]
  if (expected !== undefined && magic !== expected) {
    throw new Error(
      `CARDANO_NETWORK=${network} requires CARDANO_NETWORK_MAGIC=${expected}, got ${magic}`,
    )
  }
  return magic
}

export function loadConfig(): AppConfig {
  const intersectionCandidates = integerEnv('CLICKSYNC_INTERSECTION_CANDIDATES', 4096, 3, 10_000)
  const maxRollbackDepth = integerEnv('CLICKSYNC_MAX_ROLLBACK_DEPTH', 2_160, 1, 9_998)
  if (intersectionCandidates < maxRollbackDepth + 2) {
    throw new Error(
      'CLICKSYNC_INTERSECTION_CANDIDATES must be at least '
      + 'CLICKSYNC_MAX_ROLLBACK_DEPTH + 2 (dense rollback window plus a historical bucket)',
    )
  }

  return {
    networkMagic: networkMagic(),
    clickhouse: {
      url: process.env.CLICKHOUSE_URL ?? 'http://127.0.0.1:8123',
      database: validateDatabaseName(process.env.CLICKHOUSE_DATABASE ?? 'clicksync'),
      username: process.env.CLICKHOUSE_USER ?? 'default',
      password: process.env.CLICKHOUSE_PASSWORD ?? '',
      requestTimeoutMs: integerEnv('CLICKHOUSE_REQUEST_TIMEOUT_MS', 30_000, 1_000, 600_000),
    },
    ogmios: {
      host: process.env.OGMIOS_HOST ?? '127.0.0.1',
      port: integerEnv('OGMIOS_PORT', 1337, 1, 65_535),
      tls: booleanEnv('OGMIOS_TLS', false),
      connectTimeoutMs: integerEnv('OGMIOS_CONNECT_TIMEOUT_MS', 30_000, 1_000, 600_000),
      inFlight: integerEnv('OGMIOS_IN_FLIGHT', 20, 1, 100),
      maxPayloadBytes: integerEnv('OGMIOS_MAX_PAYLOAD_BYTES', 128 * 1024 * 1024, 1024, 1024 * 1024 * 1024),
    },
    batchBlocks: integerEnv('CLICKSYNC_BATCH_BLOCKS', 250, 1, 2_000),
    intersectionCandidates,
    maxRollbackDepth,
    schemaDirectory: process.env.CLICKSYNC_SCHEMA_DIR ?? 'schema',
  }
}
