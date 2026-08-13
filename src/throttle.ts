import { LagSource, ThrottleOptions } from './types';
import { sleep } from './db';

export interface ThrottleResult {
  /** Whether this call had to pause at all before returning. */
  waited: boolean;
  /** Total time spent paused, in ms. */
  waitedMs: number;
  /** Last observed lag reading before returning. */
  lastLagMs: number;
}

/**
 * Auto-throttle: before every batch the orchestrator asks this controller to check the lag
 * source and block until lag is back under threshold. This replaces a fixed-rate batching loop
 * (which either wastes time being conservative or risks overloading a lagging replica) with one
 * that runs as fast as safely possible and backs off exactly when and for as long as needed.
 */
export class ThrottleController {
  constructor(private readonly lagSource: LagSource, private readonly opts: ThrottleOptions) {}

  async waitIfNeeded(): Promise<ThrottleResult> {
    const start = Date.now();
    let lag = await this.lagSource.getLagMs();
    let waited = false;

    while (lag > this.opts.thresholdMs) {
      waited = true;
      const waitedMs = Date.now() - start;
      if (this.opts.maxWaitMs !== undefined && waitedMs > this.opts.maxWaitMs) {
        throw new Error(
          `Aborting: lag stayed above threshold (${this.opts.thresholdMs}ms) for over ${this.opts.maxWaitMs}ms ` +
            `(last observed ${lag}ms).`
        );
      }
      this.opts.onPause?.(lag, waitedMs);
      await sleep(this.opts.pollIntervalMs);
      lag = await this.lagSource.getLagMs();
    }

    return { waited, waitedMs: Date.now() - start, lastLagMs: lag };
  }
}
