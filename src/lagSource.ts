import { Pool } from 'pg';
import { LagSource } from './types';

/**
 * Production lag source: reads real streaming-replication lag from pg_stat_replication.
 * Uses replay_lag (time for a replica to apply WAL it has received) since that's the metric that
 * actually matters for read-replica staleness and hot-standby feedback pressure; falls back to 0
 * when there are no replicas attached (a primary-only setup shouldn't ever throttle on lag).
 */
export class PgReplicationLagSource implements LagSource {
  constructor(private readonly pool: Pool) {}

  async getLagMs(): Promise<number> {
    const res = await this.pool.query<{ lag_ms: string | null }>(
      `SELECT COALESCE(EXTRACT(EPOCH FROM MAX(replay_lag)) * 1000, 0)::text AS lag_ms
       FROM pg_stat_replication`
    );
    return Number(res.rows[0]?.lag_ms ?? 0);
  }
}

/**
 * Test/dev lag source: no replica required. Callers inject a lag value (or a function producing
 * one, to simulate lag rising and falling over the course of a run) via `setLagMs`. This is the
 * documented drop-in swap point for PgReplicationLagSource in environments without a replica.
 */
export class SimulatedLagSource implements LagSource {
  private lagMs: number;

  constructor(initialLagMs = 0) {
    this.lagMs = initialLagMs;
  }

  setLagMs(lagMs: number): void {
    this.lagMs = lagMs;
  }

  async getLagMs(): Promise<number> {
    return this.lagMs;
  }
}
