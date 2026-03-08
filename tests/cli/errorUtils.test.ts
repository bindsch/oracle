import { describe, expect, test } from "vitest";
import { isErrorLogged, markErrorLogged } from "../../src/cli/errorUtils.ts";

describe("errorUtils", () => {
  test("marks errors as logged", () => {
    const err = new Error("boom");
    expect(isErrorLogged(err)).toBe(false);
    markErrorLogged(err);
    expect(isErrorLogged(err)).toBe(true);
  });

  test("ignores non-error values", () => {
    expect(isErrorLogged("oops")).toBe(false);
    markErrorLogged("oops");
    expect(isErrorLogged("oops")).toBe(false);
  });
});
