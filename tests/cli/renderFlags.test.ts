import { describe, expect, test } from "vitest";
import { resolveRenderFlag, resolveRenderPlain } from "../../src/cli/renderFlags.ts";

describe("resolveRenderFlag", () => {
  test("prefers explicit renderMarkdown", () => {
    expect(resolveRenderFlag(false, true)).toBe(true);
  });

  test("falls back to render alias", () => {
    expect(resolveRenderFlag(true, false)).toBe(true);
  });

  test("false when neither flag is set", () => {
    expect(resolveRenderFlag(undefined, undefined)).toBe(false);
  });

  test("render-plain wins when any render flag is set", () => {
    expect(resolveRenderPlain(true, false, false)).toBe(true);
    expect(resolveRenderPlain(true, true, false)).toBe(true);
    expect(resolveRenderPlain(true, false, true)).toBe(true);
  });

  test("render-plain is false when not requested", () => {
    expect(resolveRenderPlain(false, true, true)).toBe(false);
    expect(resolveRenderPlain(undefined, true, true)).toBe(false);
  });
});
