export interface ApiConfig {
  databaseUrl: string
  host: string
  port: number
  corsOrigin?: string
  historyLimit: number
}

function positiveInteger(value: string | undefined, fallback: number, name: string): number {
  if (value === undefined || value.trim() === '') return fallback
  const parsed = Number(value)
  if (!Number.isInteger(parsed) || parsed <= 0 || parsed > 65_535) {
    throw new Error(`${name} must be a positive integer between 1 and 65535`)
  }
  return parsed
}

export function loadConfig(env: Record<string, string | undefined> = Bun.env): ApiConfig {
  const databaseUrl = env.SHIBA_DATABASE_URL?.trim() || env.DATABASE_URL?.trim()
  if (!databaseUrl) {
    throw new Error('DATABASE_URL or SHIBA_DATABASE_URL is required')
  }

  const corsOrigin = env.OBSERVABILITY_CORS_ORIGIN?.trim()
  return {
    databaseUrl,
    host: env.OBSERVABILITY_HOST?.trim() || '127.0.0.1',
    port: positiveInteger(env.OBSERVABILITY_PORT, 8787, 'OBSERVABILITY_PORT'),
    corsOrigin: corsOrigin || undefined,
    historyLimit: positiveInteger(env.OBSERVABILITY_HISTORY_LIMIT, 48, 'OBSERVABILITY_HISTORY_LIMIT'),
  }
}
