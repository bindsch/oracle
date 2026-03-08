import { describe, expect, test } from "vitest";
import * as oracle from "../../src/oracle.js";

describe("oracle entrypoint exports", () => {
  test("exposes core helpers", () => {
    expect(oracle.DEFAULT_MODEL).toBeDefined();
    expect(typeof oracle.createDefaultClientFactory).toBe("function");
    expect(typeof oracle.runOracle).toBe("function");
    expect(typeof oracle.formatFileSection).toBe("function");
    expect(oracle.PRO_MODELS instanceof Set).toBe(true);
  });
});
