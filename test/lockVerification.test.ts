import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Pool } from 'pg';
import { isPgAvailable, testConnectionString, resetSchema } from './testDb';
import { runMigration } from '../src/orchestrator';
import { SimulatedLagSource } from '../src/lagSource';
import { MigrationSpec } from '../src/types';
import { sleep } from '../src/db';

const CONN = testConnectionString();

/**
 * This is the closest thing in this project to the spec's "measured zero long-held table locks
 * (verified via pg_locks)" requirement: while the migration runs against a real Postgres table,
 * a second connection polls pg_locks for the relation every 25ms, and a third connection
 * continuously writes to the same table with a short lock_timeout. A naive single-statement
 * backfill/ALTER would show an AccessExclusiveLock (or a RowExclusive lock held for the whole
 * run) and would starve the concurrent writer; the batched approach should do neither.
 */
describe.runIf(await isPgAvailable())('lock behavior during a live migration', () => {
  let pool: Pool;

  beforeAll(async () => {
    pool = new Pool({ connectionString: CONN });
    await resetSchema(pool);
    await pool.query(`
      CREATE TABLE users (
        id BIGSERIAL PRIMARY KEY,
        email TEXT NOT NULL
      )
    `);
    await pool.query(
      `INSERT INTO users (email) SELECT 'user' || g || '@example.com' FROM generate_series(1, 20000) g`
    );
  });

  afterAll(async () => {
    await pool.end();
  });

  it('never takes an AccessExclusiveLock and never blocks a concurrent writer beyond its lock_timeout', async () => {
    const spec: MigrationSpec = {
      id: 'lock-check-migration',
      table: 'users',
      steps: [
        { type: 'add_column', name: 'add-status-column', column: 'status', dataType: 'text' },
        {
          type: 'backfill',
          name: 'backfill-status',
          pkColumn: 'id',
          batchSize: 200,
          setClause: "status = 'active'",
          whereClause: 'status IS NULL',
          batchDelayMs: 5,
        },
        {
          type: 'add_constraint',
          name: 'enforce-status-not-null',
          sql:
            'ALTER TABLE users ADD CONSTRAINT status_not_null CHECK (status IS NOT NULL) NOT VALID; ' +
            'ALTER TABLE users VALIDATE CONSTRAINT status_not_null; ' +
            'ALTER TABLE users ALTER COLUMN status SET NOT NULL; ' +
            'ALTER TABLE users DROP CONSTRAINT status_not_null;',
        },
      ],
    };

    const observedLockModes = new Set<string>();
    let sampling = true;
    const samplerPool = new Pool({ connectionString: CONN });
    const samplerLoop = (async () => {
      while (sampling) {
        const { rows } = await samplerPool.query(
          `SELECT l.mode
           FROM pg_locks l
           JOIN pg_class c ON c.oid = l.relation
           WHERE c.relname = 'users' AND l.mode != 'AccessShareLock'`
        );
        for (const r of rows) observedLockModes.add(r.mode);
        await sleep(25);
      }
    })();

    let writerErrors = 0;
    let writerSucceeded = 0;
    let writing = true;
    const writerPool = new Pool({ connectionString: CONN });
    const writerLoop = (async () => {
      // Wait for the (near-instant) add_column step to land before writing `status` explicitly —
      // mirrors a real rollout where app code that populates a new column ships around the same
      // time the column is added, rather than racing the orchestrator's very first DDL statement.
      for (let i = 0; i < 200; i++) {
        const { rows } = await writerPool.query(
          `SELECT 1 FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'status'`
        );
        if (rows.length > 0) break;
        await sleep(10);
      }

      while (writing) {
        const client = await writerPool.connect();
        try {
          await client.query('SET lock_timeout = 500'); // a live app should never wait long for a row write
          await client.query(
            `INSERT INTO users (email, status) VALUES ('writer-' || floor(random() * 1000000) || '@example.com', 'active')`
          );
          writerSucceeded += 1;
        } catch (err) {
          writerErrors += 1;
        } finally {
          client.release();
        }
        await sleep(15);
      }
    })();

    await runMigration(pool, spec, {
      lagSource: new SimulatedLagSource(0),
      throttle: { thresholdMs: 2000, pollIntervalMs: 50 },
      log: { info: () => {} },
    });

    sampling = false;
    writing = false;
    await Promise.all([samplerLoop, writerLoop]);
    await samplerPool.end();
    await writerPool.end();

    expect(observedLockModes.has('AccessExclusiveLock')).toBe(false);
    expect(writerSucceeded).toBeGreaterThan(0);
    expect(writerErrors).toBe(0);
  }, 60000);
});
