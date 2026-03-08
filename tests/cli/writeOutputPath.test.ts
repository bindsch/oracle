import os from "node:os";
import path from "node:path";

import { describe, test, expect, beforeAll, afterAll } from "vitest";
import { setOracleHomeDirOverrideForTest } from "../../src/oracleHome.js";
import { getSessionsDir } from "../../src/sessionManager.js";

describe("resolveOutputPath", () => {
  let tmpHome: string;

  beforeAll(() => {
    tmpHome = path.join(os.tmpdir(), "oracle-write-output-test");
    setOracleHomeDirOverrideForTest(tmpHome);
  });

  afterAll(() => {
    setOracleHomeDirOverrideForTest(null);
  });

  test("rejects paths inside session storage", async () => {
    const { resolveOutputPath } = await import("../../src/cli/writeOutputPath.ts");
    const insideSessions = path.join(getSessionsDir(), "out.md");
    expect(() => resolveOutputPath(insideSessions, "/tmp")).toThrow(
      /Refusing to write output inside session storage/,
    );
  });

  test("allows tilde expansion", async () => {
    const { resolveOutputPath } = await import("../../src/cli/writeOutputPath.ts");
    const result = resolveOutputPath("~/answer.md", "/tmp");
    expect(result?.startsWith(os.homedir())).toBe(true);
  });
});
