import { Pool, PoolClient } from 'pg';
import { AddColumnStep, AddConstraintStep, BackfillStep, MigrationSpec, StepProgress } from './types';
import { assertValidIdentifier, quoteIdent } from './identifiers';
import { recordBatch } from './progress';
import { ThrottleController } from './throttle';
import { sleep } from './db';

export interface StepLogger {
  info: (msg: string) => void;
}

/**
 * Step 1: nullable-with-no-default column add. In Postgres 11+ this is a metadata-only change
 * (no table rewrite, no long lock) as long as there's no default — which is exactly why the
 * "add nullable, backfill, then constrain" split exists: each phase is independently cheap to lock
 * for, instead of one ALTER TABLE that rewrites/scans the whole table under a single lock.
 */
export async function executeAddColumn(pool: Pool, table: string, step: AddColumnStep, log: StepLogger): Promise<void> {
  assertValidIdentifier(table, 'table');
  assertValidIdentifier(step.column, 'column');
  const sql = `ALTER TABLE ${quoteIdent(table)} ADD COLUMN IF NOT EXISTS ${quoteIdent(step.column)} ${step.dataType} NULL`;
  log.info(`  ${sql}`);
  await pool.query(sql);
}

/**
 * Step 2: paced, batched, resumable backfill via keyset pagination (not OFFSET, which degrades
 * as the table grows and re-scans skipped rows on every page). Each batch is a single UPDATE
 * statement — bounded by LIMIT — so no batch ever holds a lock proportional to table size, and
 * the throttle controller is consulted before every batch so a lagging replica pauses the whole
 * run instead of piling on more write volume.
 */
export async function executeBackfill(
  pool: Pool,
  table: string,
  step: BackfillStep,
  stepIndex: number,
  migrationId: string,
  startProgress: StepProgress,
  throttle: ThrottleController,
  log: StepLogger
): Promise<void> {
  assertValidIdentifier(table, 'table');
  assertValidIdentifier(step.pkColumn, 'pkColumn');

  const tableIdent = quoteIdent(table);
  const pkIdent = quoteIdent(step.pkColumn);
  let cursor: string | null = startProgress.cursorValue;
  let totalThisRun = 0;

  for (;;) {
    const throttleResult = await throttle.waitIfNeeded();
    if (throttleResult.waited) {
      log.info(`  paused for lag (${throttleResult.lastLagMs}ms observed) before next batch`);
    }

    const client = await pool.connect();
    let batchRowCount = 0;
    try {
      await client.query('BEGIN');
      const updateSql = `
        UPDATE ${tableIdent}
        SET ${step.setClause}
        WHERE ${pkIdent} = ANY(
          SELECT ${pkIdent} FROM ${tableIdent}
          WHERE ($1::bigint IS NULL OR ${pkIdent} > $1::bigint)
            AND (${step.whereClause})
          ORDER BY ${pkIdent}
          LIMIT $2
        )
        RETURNING ${pkIdent}
      `;
      const res = await client.query(updateSql, [cursor, step.batchSize]);
      batchRowCount = res.rowCount ?? 0;

      if (batchRowCount > 0) {
        const maxPk = res.rows.reduce(
          (max: bigint, row: any) => {
            const v = BigInt(row[step.pkColumn]);
            return v > max ? v : max;
          },
          BigInt(cursor ?? '-1')
        );
        cursor = maxPk.toString();
        await recordBatch(client, migrationId, stepIndex, cursor, batchRowCount);
      }
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }

    if (batchRowCount === 0) break;
    totalThisRun += batchRowCount;
    log.info(`  batch complete: ${batchRowCount} rows (cursor=${cursor}, total this run=${totalThisRun})`);

    if (step.batchDelayMs) await sleep(step.batchDelayMs);
  }
}

/**
 * Step 3: constraints (NOT NULL, indexes, FKs) applied only after the backfill step is verified
 * `completed`. Callers are expected to write short-lock SQL themselves — e.g. the
 * `NOT VALID` + `VALIDATE CONSTRAINT` pattern for NOT NULL, or `CREATE INDEX CONCURRENTLY` — which
 * is why this executor runs the SQL as-is rather than wrapping it in a transaction (several of
 * those statements are illegal inside one).
 */
export async function executeAddConstraint(pool: Pool, step: AddConstraintStep, log: StepLogger): Promise<void> {
  log.info(`  ${step.sql.trim().split('\n').join(' ')}`);
  await pool.query(step.sql);
}

export function validateSpec(spec: MigrationSpec): void {
  if (!spec.id) throw new Error('Migration spec must have an "id"');
  if (!spec.table) throw new Error('Migration spec must have a "table"');
  if (!spec.steps || spec.steps.length === 0) throw new Error('Migration spec must have at least one step');
  const names = new Set<string>();
  for (const step of spec.steps) {
    if (names.has(step.name)) throw new Error(`Duplicate step name "${step.name}" in spec "${spec.id}"`);
    names.add(step.name);
  }
}
