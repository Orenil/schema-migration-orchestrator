import { Pool } from 'pg';
import { StepProgress, StepStatus } from './types';

const PROGRESS_TABLE = 'schema_migration_progress';

/**
 * The progress table is the entire resumability mechanism: every batch commits its own progress
 * row update in the same transaction as the data change, so a killed process can never end up with
 * data updated but progress un-recorded (or vice versa). Re-running `run` simply reads this table
 * and continues from the last completed batch/step instead of restarting the migration.
 */
export async function ensureProgressTable(pool: Pool): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${PROGRESS_TABLE} (
      migration_id  TEXT NOT NULL,
      step_index    INTEGER NOT NULL,
      step_name     TEXT NOT NULL,
      status        TEXT NOT NULL DEFAULT 'pending'
                       CHECK (status IN ('pending', 'in_progress', 'completed', 'failed')),
      cursor_value  TEXT,
      rows_processed BIGINT NOT NULL DEFAULT 0,
      last_error    TEXT,
      updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (migration_id, step_index)
    )
  `);
}

export async function getStepProgress(
  pool: Pool,
  migrationId: string,
  stepIndex: number
): Promise<StepProgress | null> {
  const res = await pool.query(
    `SELECT migration_id, step_index, step_name, status, cursor_value, rows_processed, updated_at, last_error
     FROM ${PROGRESS_TABLE} WHERE migration_id = $1 AND step_index = $2`,
    [migrationId, stepIndex]
  );
  if (res.rows.length === 0) return null;
  return rowToStepProgress(res.rows[0]);
}

export async function getAllProgress(pool: Pool, migrationId: string): Promise<StepProgress[]> {
  const res = await pool.query(
    `SELECT migration_id, step_index, step_name, status, cursor_value, rows_processed, updated_at, last_error
     FROM ${PROGRESS_TABLE} WHERE migration_id = $1 ORDER BY step_index`,
    [migrationId]
  );
  return res.rows.map(rowToStepProgress);
}

/** Idempotently ensures a progress row exists for this step, returning its current state. */
export async function initStep(
  pool: Pool,
  migrationId: string,
  stepIndex: number,
  stepName: string
): Promise<StepProgress> {
  await pool.query(
    `INSERT INTO ${PROGRESS_TABLE} (migration_id, step_index, step_name, status)
     VALUES ($1, $2, $3, 'pending')
     ON CONFLICT (migration_id, step_index) DO NOTHING`,
    [migrationId, stepIndex, stepName]
  );
  const progress = await getStepProgress(pool, migrationId, stepIndex);
  if (!progress) throw new Error(`Failed to initialize progress row for step "${stepName}"`);
  return progress;
}

export async function markStepStatus(
  pool: Pool,
  migrationId: string,
  stepIndex: number,
  status: StepStatus,
  lastError: string | null = null
): Promise<void> {
  await pool.query(
    `UPDATE ${PROGRESS_TABLE}
     SET status = $3, last_error = $4, updated_at = now()
     WHERE migration_id = $1 AND step_index = $2`,
    [migrationId, stepIndex, status, lastError]
  );
}

/**
 * Records one completed batch: advances the keyset cursor and row count. Callers pass a client
 * already inside the same transaction as the data-changing UPDATE so a crash between the two is
 * impossible — they commit atomically together.
 */
export async function recordBatch(
  client: { query: Pool['query'] },
  migrationId: string,
  stepIndex: number,
  cursorValue: string,
  rowsInBatch: number
): Promise<void> {
  await client.query(
    `UPDATE ${PROGRESS_TABLE}
     SET cursor_value = $3,
         rows_processed = rows_processed + $4,
         status = 'in_progress',
         updated_at = now()
     WHERE migration_id = $1 AND step_index = $2`,
    [migrationId, stepIndex, cursorValue, rowsInBatch]
  );
}

function rowToStepProgress(row: any): StepProgress {
  return {
    migrationId: row.migration_id,
    stepIndex: row.step_index,
    stepName: row.step_name,
    status: row.status,
    cursorValue: row.cursor_value,
    rowsProcessed: String(row.rows_processed),
    updatedAt: row.updated_at,
    lastError: row.last_error,
  };
}

export { PROGRESS_TABLE };
