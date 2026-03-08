import { describe, expect, test } from "vitest";
import { runBrowserMode, CHATGPT_URL } from "../../src/browserMode.js";

describe("browserMode exports", () => {
  test("re-exports runBrowserMode and constants", () => {
    expect(typeof runBrowserMode).toBe("function");
    expect(typeof CHATGPT_URL).toBe("string");
  });
});
