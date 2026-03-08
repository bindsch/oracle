import { describe, expect, test, vi, beforeEach, afterEach } from "vitest";
import { startHeartbeat } from "../../src/heartbeat.js";

describe("startHeartbeat", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  test("logs messages on interval while active and stops when stopped", async () => {
    const log = vi.fn();
    const isActive = vi.fn(() => true);
    const makeMessage = vi.fn(async (elapsed: number) => `tick-${elapsed}`);

    const stop = startHeartbeat({ intervalMs: 25, log, isActive, makeMessage });

    await vi.advanceTimersByTimeAsync(30);
    await vi.advanceTimersByTimeAsync(30);

    expect(log).toHaveBeenCalledTimes(2);
    stop();
    await vi.advanceTimersByTimeAsync(50);
    expect(log).toHaveBeenCalledTimes(2);
  });

  test("no-ops when interval is missing or non-positive", async () => {
    const log = vi.fn();
    const stop = startHeartbeat({
      intervalMs: 0,
      log,
      isActive: () => true,
      makeMessage: () => "noop",
    });
    await vi.advanceTimersByTimeAsync(50);
    expect(log).not.toHaveBeenCalled();
    stop();
  });
});
