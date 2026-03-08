import { describe, expect, test, vi } from "vitest";
import { pruneOldSessions, sessionStore } from "../src/sessionStore.ts";

describe("pruneOldSessions", () => {
  test("skips invalid or missing hour values", async () => {
    const spy = vi
      .spyOn(sessionStore, "deleteOlderThan")
      .mockResolvedValue({ deleted: 0, remaining: 0 });
    await pruneOldSessions(undefined);
    await pruneOldSessions(NaN);
    await pruneOldSessions(-5);
    expect(spy).not.toHaveBeenCalled();
    spy.mockRestore();
  });

  test("prunes sessions and logs when deletions occur", async () => {
    const spy = vi
      .spyOn(sessionStore, "deleteOlderThan")
      .mockResolvedValue({ deleted: 2, remaining: 3 });
    const logger = vi.fn();
    await pruneOldSessions(12, logger);
    expect(spy).toHaveBeenCalledWith({ hours: 12 });
    expect(logger).toHaveBeenCalledWith("Pruned 2 stored sessions older than 12h.");
    spy.mockRestore();
  });
});
