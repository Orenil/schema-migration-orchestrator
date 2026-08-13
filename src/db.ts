import { Pool, PoolClient } from 'pg';

/** Thin wrapper so callers pass a connection string once and get a real pg Pool. */
export function createPool(connectionString: string): Pool {
  return new Pool({ connectionString });
}

/** Runs `fn` with a dedicated client checked out from the pool, always releasing it. */
export async function withClient<T>(pool: Pool, fn: (client: PoolClient) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  try {
    return await fn(client);
  } finally {
    client.release();
  }
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
