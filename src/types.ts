/**
 * Shared type definitions for the migration orchestrator.
 */

/** Step 1: add a nullable column with no default. Always a fast, metadata-only ALTER. */
export interface AddColumnStep {
  type: 'add_column';
  name: string;
  column: string;
  /** Raw Postgres type, e.g. "text", "integer", "timestamptz". */
  dataType: string;
}

/** Step 2: paced, batched backfill of an existing/new column using keyset pagination. */
export interface BackfillStep {
  type: 'backfill';
  name: string;
  /** Primary key column used for keyset pagination (must be orderable, e.g. bigint/uuid-as-text sortable). */
  pkColumn: string;
  /** Number of rows updated per batch. */
  batchSize: number;
  /** SQL SET clause applied to each batch, e.g. "status = 'active'" or "normalized_email = lower(email)". */
  setClause: string;
  /**
   * SQL predicate (no leading WHERE) selecting rows that still need backfilling, e.g. "status IS NULL".
   * Used both to find the next batch and to make the step idempotent/resumable.
   */
  whereClause: string;
  /** Milliseconds to sleep between batches when not throttled. Defaults to 0. */
  batchDelayMs?: number;
}

/** Step 3: apply constraints (NOT NULL, default, index) only after backfill is verified complete. */
export interface AddConstraintStep {
  type: 'add_constraint';
  name: string;
  /** Raw SQL executed once, expected to be a short-lock operation (e.g. NOT VALID + VALIDATE CONSTRAINT pattern). */
  sql: string;
}

export type MigrationStep = AddColumnStep | BackfillStep | AddConstraintStep;

export interface MigrationSpec {
  /** Unique identifier for this migration; used as the progress-table key. */
  id: string;
  /** Fully qualified or bare table name the migration operates against. */
  table: string;
  description?: string;
  steps: MigrationStep[];
}

export type StepStatus = 'pending' | 'in_progress' | 'completed' | 'failed';

export interface StepProgress {
  migrationId: string;
  stepName: string;
  stepIndex: number;
  status: StepStatus;
  cursorValue: string | null;
  rowsProcessed: string; // bigint comes back as string from pg
  updatedAt: Date;
  lastError: string | null;
}

/** Pluggable source of replication lag (or any other back-pressure signal), in milliseconds. */
export interface LagSource {
  getLagMs(): Promise<number>;
}

export interface ThrottleOptions {
  /** Lag above this threshold pauses batching. */
  thresholdMs: number;
  /** How often to re-check lag while paused. */
  pollIntervalMs: number;
  /** Give up (throw) if paused longer than this. Undefined = wait indefinitely. */
  maxWaitMs?: number;
  /** Called on every pause tick, useful for logging/observability. */
  onPause?: (lagMs: number, waitedMs: number) => void;
}
