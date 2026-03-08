import { describe, it, expect } from "vitest";
import { sessionStore } from "../../src/sessionStore.js";

const baseOptions = {
  prompt: "hi",
  file: [],
  model: "gpt-5.1",
  mode: "api" as const,
  browserConfig: undefined,
  verbose: false,
  heartbeatIntervalMs: 30000,
  browserAttachments: "auto" as const,
  browserInlineFiles: false,
  browserBundleFiles: false,
  background: false,
};

describe("session search persistence", () => {
  it("stores search flag when provided", async () => {
    const meta = await sessionStore.createSession({ ...baseOptions, search: false }, process.cwd());
    expect(meta.options.search).toBe(false);
  });
});
