import { describe, expect, test } from "vitest";
import { formatBaseUrlForLog, maskApiKey } from "../src/oracle/logging.js";

describe("maskApiKey", () => {
  test("masks long keys with first/last 4 chars", () => {
    expect(maskApiKey("sk-abcdef1234567890")).toBe("sk-a****7890");
  });

  test("handles short keys gracefully", () => {
    expect(maskApiKey("abc")).toBe("a***c");
    expect(maskApiKey(undefined)).toBeNull();
    expect(maskApiKey(null)).toBeNull();
  });
});

describe("formatBaseUrlForLog", () => {
  test("redacts credentials, deep paths, and query values", () => {
    const formatted = formatBaseUrlForLog(
      "https://user:pass@proxy.test/v1/deep/path?api-version=2024-10-01&token=secret",
    );
    expect(formatted).toBe("https://proxy.test/v1/...?api-version=***");
  });

  test("truncates unparseable strings", () => {
    const formatted = formatBaseUrlForLog("not a url but extremely long".repeat(4));
    expect(formatted.startsWith("not a url but extremely longnot ")).toBe(true);
    expect(formatted.includes("…")).toBe(true);
    expect(formatted.endsWith("ely long")).toBe(true);
    expect(formatted.length).toBeLessThan(80);
  });
});
