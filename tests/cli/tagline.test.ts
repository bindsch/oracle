import { describe, expect, test } from "vitest";
import { activeTaglines, formatIntroLine, pickTagline, TAGLINES } from "../../src/cli/tagline.ts";

describe("taglines", () => {
  test("respects env override for deterministic index", () => {
    const env: Record<string, string> = {};
    env.ORACLE_TAGLINE_INDEX = "3";
    const tagline = pickTagline({ env });
    expect(tagline).toBe(TAGLINES[3]);
  });

  test("wraps index modulo tagline length", () => {
    const env: Record<string, string> = {};
    env.ORACLE_TAGLINE_INDEX = String(TAGLINES.length + 2);
    const tagline = pickTagline({ env });
    expect(tagline).toBe(TAGLINES[2]);
  });

  test("falls back to random source when no override", () => {
    const tagline = pickTagline({ random: () => 0.49 });
    expect(TAGLINES).toContain(tagline);
  });

  test("hides seasonal taglines on non-holiday dates", () => {
    const today = () => new Date(Date.UTC(2025, 10, 21)); // Nov 21 2025 UTC
    const active = activeTaglines({ now: today });
    expect(active).not.toContain("Lunar New Year sweep: clear caches, invite good deploys.");
  });

  test("shows seasonal tagline on its holiday date", () => {
    const today = () => new Date(Date.UTC(2025, 0, 29)); // Lunar New Year 2025 UTC
    const active = activeTaglines({ now: today });
    expect(active).toContain("Lunar New Year sweep: clear caches, invite good deploys.");
  });

  test("formats intro line with version (plain)", () => {
    const env: Record<string, string> = {};
    env.ORACLE_TAGLINE_INDEX = "0";
    const intro = formatIntroLine("1.2.3", { env, richTty: false });
    expect(intro.startsWith("🧿 oracle 1.2.3 — ")).toBe(true);
    expect(intro).toContain(TAGLINES[0]);
  });
});
