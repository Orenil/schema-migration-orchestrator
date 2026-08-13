import { describe, it, expect, vi } from 'vitest';
import { ThrottleController } from '../src/throttle';
import { SimulatedLagSource } from '../src/lagSource';

describe('ThrottleController', () => {
  it('returns immediately with waited=false when lag is under threshold', async () => {
    const lagSource = new SimulatedLagSource(50);
    const controller = new ThrottleController(lagSource, { thresholdMs: 2000, pollIntervalMs: 10 });
    const result = await controller.waitIfNeeded();
    expect(result.waited).toBe(false);
    expect(result.lastLagMs).toBe(50);
  });

  it('polls until lag drops back under threshold, then returns', async () => {
    const lagSource = new SimulatedLagSource(5000);
    const onPause = vi.fn();
    const controller = new ThrottleController(lagSource, {
      thresholdMs: 2000,
      pollIntervalMs: 5,
      onPause,
    });

    // Simulate lag dropping after a couple of polls, driven by the onPause callback so we don't
    // need real wall-clock waits in the test.
    let pauseCount = 0;
    onPause.mockImplementation(() => {
      pauseCount += 1;
      if (pauseCount >= 3) lagSource.setLagMs(100);
    });

    const result = await controller.waitIfNeeded();
    expect(result.waited).toBe(true);
    expect(result.lastLagMs).toBe(100);
    expect(onPause).toHaveBeenCalledTimes(3);
  });

  it('throws once paused longer than maxWaitMs', async () => {
    const lagSource = new SimulatedLagSource(9999);
    const controller = new ThrottleController(lagSource, {
      thresholdMs: 100,
      pollIntervalMs: 5,
      maxWaitMs: 15,
    });

    await expect(controller.waitIfNeeded()).rejects.toThrow(/lag stayed above threshold/);
  });

  it('never calls onPause when lag never exceeds threshold', async () => {
    const lagSource = new SimulatedLagSource(0);
    const onPause = vi.fn();
    const controller = new ThrottleController(lagSource, { thresholdMs: 100, pollIntervalMs: 5, onPause });
    await controller.waitIfNeeded();
    expect(onPause).not.toHaveBeenCalled();
  });
});
