import { afterEach, describe, expect, it, vi } from 'vitest'

import { loadConfig } from '../src/config.js'

afterEach(() => vi.unstubAllEnvs())

describe('configuration invariants', () => {
  it('rejects a known network name/magic mismatch', () => {
    vi.stubEnv('CLICKSYNC_MAX_ROLLBACK_DEPTH', '2160')
    vi.stubEnv('CLICKSYNC_INTERSECTION_CANDIDATES', '4096')
    vi.stubEnv('CARDANO_NETWORK', 'preprod')
    vi.stubEnv('CARDANO_NETWORK_MAGIC', '764824073')

    expect(() => loadConfig()).toThrow(/requires CARDANO_NETWORK_MAGIC=1/)
  })

  it('requires dense intersection coverage for the allowed rollback window', () => {
    vi.stubEnv('CARDANO_NETWORK', 'mainnet')
    vi.stubEnv('CARDANO_NETWORK_MAGIC', '764824073')
    vi.stubEnv('CLICKSYNC_MAX_ROLLBACK_DEPTH', '99')
    vi.stubEnv('CLICKSYNC_INTERSECTION_CANDIDATES', '100')

    expect(() => loadConfig()).toThrow(/MAX_ROLLBACK_DEPTH \+ 2/)

    vi.stubEnv('CLICKSYNC_INTERSECTION_CANDIDATES', '101')
    expect(loadConfig().maxRollbackDepth).toBe(99)
  })
})
