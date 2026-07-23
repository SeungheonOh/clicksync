import { pino } from 'pino'

const isPretty = process.env.LOG_PRETTY !== 'false' && process.stdout.isTTY

export const logger = pino(
  {
    level: process.env.LOG_LEVEL ?? 'info',
    redact: ['password', '*.password', 'clickhouse.password'],
  },
  isPretty
    ? pino.transport({
        target: 'pino-pretty',
        options: { colorize: true, singleLine: true, translateTime: 'SYS:standard' },
      })
    : undefined,
)
