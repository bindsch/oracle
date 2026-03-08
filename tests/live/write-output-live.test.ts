import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { describe, test, expect, beforeAll, afterAll } from "vitest";

import type { RunOracleOptions } from "../../src/oracle.ts";
import type { SessionMetadata } from "../../src/sessionManager.ts";
import { setOracleHomeDirOverrideForTest } from "../../src/oracleHome.js";

const baseUrl = process.env.OPENAI_BASE_URL ?? "";
const isOpenRouterBase = baseUrl.includes("openrouter");
const ENABLE_LIVE =
  process.env.ORACLE_LIVE_TEST === "1" && process.env.OPENAI_API_KEY && !isOpenRouterBase;

if (!ENABLE_LIVE) {
  describe.skip("write-output live e2e", () => {
    test("Set ORACLE_LIVE_TEST=1 with a real OPENAI_API_KEY (api.openai.com) to run this suite.", () => {});
  });
} else {
  describe("write-output live e2e", () => {
    let tmpHome: string;
    let performSessionRun: typeof import("../../src/cli/sessionRunner.ts").performSessionRun;
    let sessionStore: typeof import("../../src/sessionStore.ts").sessionStore;
    let getCliVersion: typeof import("../../src/version.ts").getCliVersion;
    const log = () => {};
    const write = () => true;

    beforeAll(async () => {
      tmpHome = await fs.mkdtemp(path.join(os.tmpdir(), "oracle-live-write-"));
      setOracleHomeDirOverrideForTest(tmpHome);
      ({ performSessionRun } = await import("../../src/cli/sessionRunner.ts"));
      ({ sessionStore } = await import("../../src/sessionStore.ts"));
      ({ getCliVersion } = await import("../../src/version.ts"));
    });

    afterAll(async () => {
      setOracleHomeDirOverrideForTest(null);
      await fs.rm(tmpHome, { recursive: true, force: true }).catch(() => {});
    });

    test(
      "saves assistant output file from live API run",
      async () => {
        await sessionStore.ensureStorage();
        const outputPath = path.join(tmpHome, "write-output-live.md");
        const runOptions: RunOracleOptions = {
          prompt: 'Reply with "write-output e2e" on a single line.',
          model: "gpt-4.1",
          writeOutputPath: outputPath,
          silent: true,
          heartbeatIntervalMs: 0,
        };
        const sessionMeta: SessionMetadata = await sessionStore.createSession(
          {
            ...runOptions,
            mode: "api",
          },
          process.cwd(),
        );

        try {
          await performSessionRun({
            sessionMeta,
            runOptions: { ...runOptions, sessionId: sessionMeta.id },
            mode: "api",
            cwd: process.cwd(),
            log,
            write,
            version: getCliVersion(),
          });
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          if (/model_not_found|permission/i.test(message)) {
            // Key doesn't have this model; treat as skipped rather than failing the suite.
            return;
          }
          throw error;
        }

        const saved = await fs.readFile(outputPath, "utf8");
        expect(saved.toLowerCase()).toContain("write-output e2e");
      },
      10 * 60 * 1000,
    );
  });
}
