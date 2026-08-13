import { Pool } from 'pg';
import { MigrationSpec, LagSource, ThrottleOptions } from './types';
import { ensureProgressTable, initStep, markStepStatus, getAllProgress } from './progress';
import { executeAddColumn, executeAddConstraint, executeBackfill, validateSpec, StepLogger } from './migration';
import { ThrottleController } from './throttle';

export interface RunOptions {
  lagSource: LagSource;
  throttle: ThrottleOptions;
  log?: StepLogger;
}

const defaultLog: StepLogger = {
  info: (msg: string) => console.log(msg),
};

/**
 * Drives a full migration spec to completion, step by step. Every step is checked against the
 * progress table before running: a `completed` step is skipped entirely, an `in_progress` or
 * `pending` step is (re)started from its saved cursor. This is what makes `run` and `resume` the
 * same operation — there is no separate resume code path to fall out of sync with `run`.
 */
export async function runMigration(pool: Pool, spec: MigrationSpec, opts: RunOptions): Promise<void> {
  validateSpec(spec);
  await ensureProgressTable(pool);
  const log = opts.log ?? defaultLog;
  const throttle = new ThrottleController(opts.lagSource, opts.throttle);

  log.info(`Migration "${spec.id}" on table "${spec.table}" (${spec.steps.length} step(s))`);

  for (let i = 0; i < spec.steps.length; i++) {
    const step = spec.steps[i];
    const progress = await initStep(pool, spec.id, i, step.name);

    if (progress.status === 'completed') {
      log.info(`[${i}] ${step.name} (${step.type}) — already completed, skipping`);
      continue;
    }

    log.info(`[${i}] ${step.name} (${step.type}) — starting (status was "${progress.status}")`);
    await markStepStatus(pool, spec.id, i, 'in_progress');

    try {
      switch (step.type) {
        case 'add_column':
          await executeAddColumn(pool, spec.table, step, log);
          break;
        case 'backfill':
          await executeBackfill(pool, spec.table, step, i, spec.id, progress, throttle, log);
          break;
        case 'add_constraint':
          await executeAddConstraint(pool, step, log);
          break;
      }
      await markStepStatus(pool, spec.id, i, 'completed');
      log.info(`[${i}] ${step.name} — completed`);
    } catch (err: any) {
      await markStepStatus(pool, spec.id, i, 'failed', String(err?.message ?? err));
      log.info(`[${i}] ${step.name} — FAILED: ${err?.message ?? err}`);
      throw err;
    }
  }

  log.info(`Migration "${spec.id}" complete.`);
}

export async function migrationStatus(pool: Pool, migrationId: string) {
  await ensureProgressTable(pool);
  return getAllProgress(pool, migrationId);
}
