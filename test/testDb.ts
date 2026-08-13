import { Pool } from 'pg';

/**
 * Connection string for the throwaway local cluster started by `npm run pg:start`
 * (scripts/pg-testcluster.sh). Integration tests that need it call `requirePg()` first, which
 * skips (rather than fails) when the cluster isn't up, so `npm test` degrades gracefully to the
 * pure-unit tests in environments without Postgres.
 */
export function testConnectionString(): string {
  const dir = `${__dirname}/../.pgdata-test`;
  const port = process.env.SMORCH_TEST_PG_PORT ?? '55418';
  return `postgresql://postgres@localhost/smorch_test?host=${dir}&port=${port}`;
}

let cachedAvailable: boolean | null = null;

export async function isPgAvailable(): Promise<boolean> {
  if (cachedAvailable !== null) return cachedAvailable;
  const pool = new Pool({ connectionString: testConnectionString(), connectionTimeoutMillis: 1000 });
  try {
    await pool.query('SELECT 1');
    cachedAvailable = true;
  } catch {
    cachedAvailable = false;
  } finally {
    await pool.end();
  }
  return cachedAvailable;
}

/** Drops and recreates a clean schema so each test file starts from a known state. */
export async function resetSchema(pool: Pool): Promise<void> {
  await pool.query('DROP SCHEMA public CASCADE');
  await pool.query('CREATE SCHEMA public');
}
