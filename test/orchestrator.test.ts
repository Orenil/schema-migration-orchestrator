import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { Pool } from 'pg';
import { spawn } from 'child_process';
import * as path from 'path';
import * as fs from 'fs';
import { isPgAvailable, testConnectionString, resetSchema } from './testDb';
import { runMigration, migrationStatus } from '../src/orchestrator';
import { SimulatedLagSource } from '../src/lagSource';
import { MigrationSpec } from '../src/types';

const CONN = testConnectionString();
const CLI_PATH = path.resolve(__dirname, '../dist/cli.js');

async function createUsersTable(pool: Pool, rowCount: number): Promise<void> {
  await pool.query(`
    CREATE TABLE users (
      id BIGSERIAL PRIMARY KEY,
      email TEXT NOT NULL
    )
  `);
  await pool.query(
    `INSERT INTO users (email) SELECT 'user' || g || '@example.com' FROM generate_series(1, $1) g`,
    [rowCount]
  );
}

function spec(overrides: Partial<MigrationSpec> = {}): MigrationSpec {
  return {
    id: 'backfill-user-status-v1',
    table: 'users',
    steps: [
      { type: 'add_column', name: 'add-status-column', column: 'status', dataType: 'text' },
      {
        type: 'backfill',
        name: 'backfill-status',
        pkColumn: 'id',
        batchSize: 50,
        setClause: "status = 'active'",
        whereClause: 'status IS NULL',
        batchDelayMs: 0,
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
    ...overrides,
  };
}

describe.runIf(await isPgAvailable())('orchestrator against real Postgres', () => {
  let pool: Pool;

  beforeAll(async () => {
    pool = new Pool({ connectionString: CONN });
  });

  afterAll(async () => {
    await pool.end();
  });

  beforeEach(async () => {
    await resetSchema(pool);
  });

  it('runs the full 3-step spec end to end and produces a correct final schema/data state', async () => {
    await createUsersTable(pool, 2500);
    const lagSource = new SimulatedLagSource(0);

    await runMigration(pool, spec(), {
      lagSource,
      throttle: { thresholdMs: 2000, pollIntervalMs: 50 },
      log: { info: () => {} },
    });

    const { rows: countRows } = await pool.query(
      `SELECT count(*)::int AS n FROM users WHERE status IS DISTINCT FROM 'active'`
    );
    expect(countRows[0].n).toBe(0);

    const { rows: colRows } = await pool.query(
      `SELECT is_nullable FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'status'`
    );
    expect(colRows[0].is_nullable).toBe('NO');

    // The helper CHECK constraint used to validate NOT NULL without a blocking table scan is
    // dropped once the real NOT NULL attribute is in place, so it should no longer exist.
    const { rows: constraintRows } = await pool.query(
      `SELECT 1 FROM pg_constraint WHERE conname = 'status_not_null'`
    );
    expect(constraintRows.length).toBe(0);

    const progress = await migrationStatus(pool, 'backfill-user-status-v1');
    expect(progress.every((p) => p.status === 'completed')).toBe(true);
    expect(progress.find((p) => p.stepName === 'backfill-status')?.rowsProcessed).toBe('2500');
  });

  it('is idempotent: running twice with nothing to do skips already-completed steps', async () => {
    await createUsersTable(pool, 100);
    const lagSource = new SimulatedLagSource(0);
    const opts = { lagSource, throttle: { thresholdMs: 2000, pollIntervalMs: 50 }, log: { info: () => {} } };

    await runMigration(pool, spec(), opts);
    await runMigration(pool, spec(), opts); // should be a no-op, all steps already completed

    const { rows } = await pool.query(`SELECT count(*)::int AS n FROM users WHERE status = 'active'`);
    expect(rows[0].n).toBe(100);
  });

  it('backs off under injected lag and speeds back up once lag clears', async () => {
    await createUsersTable(pool, 300);
    const lagSource = new SimulatedLagSource(5000); // starts above threshold
    let pauseCount = 0;

    // Drop the simulated lag back to normal after a couple of poll ticks, mimicking a replica
    // catching back up mid-run.
    const dropLagAfterTicks = 3;

    await runMigration(pool, spec({ steps: spec().steps.map((s) => (s.type === 'backfill' ? { ...s, batchSize: 25 } : s)) }), {
      lagSource,
      throttle: {
        thresholdMs: 1000,
        pollIntervalMs: 5,
        onPause: () => {
          pauseCount += 1;
          if (pauseCount >= dropLagAfterTicks) lagSource.setLagMs(0);
        },
      },
      log: { info: () => {} },
    });

    expect(pauseCount).toBeGreaterThanOrEqual(dropLagAfterTicks);
    const { rows } = await pool.query(`SELECT count(*)::int AS n FROM users WHERE status = 'active'`);
    expect(rows[0].n).toBe(300);
  });

  it('resumes correctly after the process is killed mid-backfill', async () => {
    await createUsersTable(pool, 6000);
    const migrationId = 'backfill-user-status-v1';
    const specPath = path.join(__dirname, '..', '.tmp-kill-resume-spec.json');
    fs.writeFileSync(
      specPath,
      JSON.stringify(
        spec({
          steps: spec().steps.map((s) => (s.type === 'backfill' ? { ...s, batchSize: 20, batchDelayMs: 30 } : s)),
        })
      )
    );

    try {
      // 1. Launch the real CLI as a child process and SIGKILL it partway through the backfill.
      const child = spawn(process.execPath, [CLI_PATH, 'run', specPath, '--db-url', CONN], {
        stdio: 'ignore',
      });
      await new Promise((resolve) => setTimeout(resolve, 700));
      child.kill('SIGKILL');
      await new Promise((resolve) => child.once('exit', resolve));

      const midProgress = await migrationStatus(pool, migrationId);
      const backfillProgress = midProgress.find((p) => p.stepName === 'backfill-status');
      expect(backfillProgress).toBeDefined();
      // It should have made partial progress but not finished — otherwise the kill timing didn't
      // actually land mid-run and this test isn't testing what it claims to.
      expect(Number(backfillProgress!.rowsProcessed)).toBeGreaterThan(0);
      expect(Number(backfillProgress!.rowsProcessed)).toBeLessThan(6000);
      expect(backfillProgress!.status).not.toBe('completed');

      const { rows: midStatusCount } = await pool.query(
        `SELECT count(*)::int AS n FROM users WHERE status = 'active'`
      );
      expect(midStatusCount[0].n).toBe(Number(backfillProgress!.rowsProcessed));

      // 2. Resume via the CLI's `resume` command and let it run to completion.
      await new Promise<void>((resolve, reject) => {
        const resumeChild = spawn(process.execPath, [CLI_PATH, 'resume', specPath, '--db-url', CONN], {
          stdio: 'ignore',
        });
        resumeChild.once('exit', (code) => (code === 0 ? resolve() : reject(new Error(`resume exited ${code}`))));
        resumeChild.once('error', reject);
      });

      const finalProgress = await migrationStatus(pool, migrationId);
      expect(finalProgress.every((p) => p.status === 'completed')).toBe(true);

      const { rows: finalCount } = await pool.query(
        `SELECT count(*)::int AS n FROM users WHERE status IS DISTINCT FROM 'active'`
      );
      expect(finalCount[0].n).toBe(0);

      const { rows: totalRows } = await pool.query(`SELECT count(*)::int AS n FROM users`);
      expect(totalRows[0].n).toBe(6000); // no rows duplicated/lost across the kill+resume

      const { rows: nnCol } = await pool.query(
        `SELECT is_nullable FROM information_schema.columns WHERE table_name = 'users' AND column_name = 'status'`
      );
      expect(nnCol[0].is_nullable).toBe('NO');
    } finally {
      fs.rmSync(specPath, { force: true });
    }
  }, 60000);
});
