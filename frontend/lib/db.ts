import { Pool, QueryResult, QueryResultRow } from "pg";

// Re-use the pool across hot-reloads in development
declare global {
  var _pgPool: Pool | undefined;
}

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function createPool(): Pool {
  const portRaw = process.env.AZURE_POSTGRESQL_DB_PORT ?? "5432";
  const port = Number.parseInt(portRaw, 10);
  if (!Number.isFinite(port) || port <= 0) {
    throw new Error(`Invalid AZURE_POSTGRESQL_DB_PORT value: '${portRaw}'`);
  }

  return new Pool({
    host: requiredEnv("AZURE_POSTGRESQL_DB_HOST"),
    user: requiredEnv("AZURE_POSTGRESQL_DB_USER"),
    password: requiredEnv("AZURE_POSTGRESQL_DB_PASSWORD"),
    port,
    database: "helioscta",
    ssl: {
      rejectUnauthorized: false,
    },
    max: 5,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 15_000,
  });
}

const pool: Pool =
  process.env.NODE_ENV === "production"
    ? createPool()
    : (globalThis._pgPool ?? (globalThis._pgPool = createPool()));

export async function query<T extends QueryResultRow = QueryResultRow>(
  sql: string,
  params?: unknown[]
): Promise<QueryResult<T>> {
  return pool.query<T>(sql, params);
}
