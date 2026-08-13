#!/usr/bin/env node
import { Command } from 'commander';
import * as fs from 'fs';
import { createPool } from './db';
import { MigrationSpec } from './types';
import { runMigration, migrationStatus } from './orchestrator';
import { PgReplicationLagSource, SimulatedLagSource } from './lagSource';

const program = new Command();

program
  .name('smorch')
  .description('Run large Postgres schema migrations on live tables without blocking writes.');

function loadSpec(path: string): MigrationSpec {
  const raw = fs.readFileSync(path, 'utf8');
  return JSON.parse(raw) as MigrationSpec;
}

function commonRunOptions(cmd: Command): Command {
  return cmd
    .requiredOption('--db-url <url>', 'Postgres connection string (or set DATABASE_URL)', process.env.DATABASE_URL)
    .option('--lag-threshold-ms <n>', 'pause batching above this replication lag', '2000')
    .option('--lag-poll-interval-ms <n>', 'how often to re-check lag while paused', '1000')
    .option('--lag-max-wait-ms <n>', 'give up if lag stays high longer than this (default: wait forever)')
    .option(
      '--simulated-lag-ms <n>',
      'use a fixed simulated lag instead of querying pg_stat_replication (testing/no-replica setups)'
    );
}

// `run` and `resume` are intentionally the same implementation: progress is always read from the
// database, so re-running a finished/partial migration just continues it. `resume` exists as a
// separate, clearly-named entry point for operators restarting a killed run.
async function runCommand(specPath: string, options: any): Promise<void> {
  const spec = loadSpec(specPath);
  const pool = createPool(options.dbUrl);
  const lagSource =
    options.simulatedLagMs !== undefined
      ? new SimulatedLagSource(Number(options.simulatedLagMs))
      : new PgReplicationLagSource(pool);

  try {
    await runMigration(pool, spec, {
      lagSource,
      throttle: {
        thresholdMs: Number(options.lagThresholdMs),
        pollIntervalMs: Number(options.lagPollIntervalMs),
        maxWaitMs: options.lagMaxWaitMs !== undefined ? Number(options.lagMaxWaitMs) : undefined,
        onPause: (lagMs, waitedMs) => console.log(`  ...paused ${waitedMs}ms so far, lag=${lagMs}ms`),
      },
    });
  } finally {
    await pool.end();
  }
}

commonRunOptions(program.command('run <specPath>').description('Run (or continue) a migration spec')).action(
  runCommand
);

commonRunOptions(
  program.command('resume <specPath>').description('Resume a previously interrupted migration (alias for `run`)')
).action(runCommand);

program
  .command('status <specPath>')
  .description('Show per-step progress for a migration spec')
  .requiredOption('--db-url <url>', 'Postgres connection string (or set DATABASE_URL)', process.env.DATABASE_URL)
  .action(async (specPath: string, options: any) => {
    const spec = loadSpec(specPath);
    const pool = createPool(options.dbUrl);
    try {
      const rows = await migrationStatus(pool, spec.id);
      if (rows.length === 0) {
        console.log(`No progress recorded yet for migration "${spec.id}".`);
        return;
      }
      console.table(
        rows.map((r) => ({
          step: r.stepIndex,
          name: r.stepName,
          status: r.status,
          rows_processed: r.rowsProcessed,
          cursor: r.cursorValue,
          updated_at: r.updatedAt.toISOString(),
        }))
      );
    } finally {
      await pool.end();
    }
  });

program.parseAsync(process.argv);
