import { describe, expect, it } from "vitest";
import {
  formatTokenEstimate,
  formatTokenValue,
  resolvePreviewMode,
} from "../../src/oracle/runUtils.js";

describe("runUtils #resolvePreviewMode", () => {
  it("returns undefined for falsy input", () => {
    expect(resolvePreviewMode(undefined)).toBeUndefined();
    expect(resolvePreviewMode(false)).toBeUndefined();
  });

  it("normalizes true to summary", () => {
    expect(resolvePreviewMode(true)).toBe("summary");
  });

  it("accepts supported strings", () => {
    expect(resolvePreviewMode("json")).toBe("json");
    expect(resolvePreviewMode("full")).toBe("full");
  });

  it("defaults invalid strings to summary", () => {
    expect(resolvePreviewMode("nonsense")).toBe("summary");
  });
});

describe("runUtils #formatTokenEstimate", () => {
  it("returns plain number strings under 1000", () => {
    expect(formatTokenEstimate(999)).toBe("999");
  });

  it("abbreviates thousands with k and rounds to two decimals", () => {
    expect(formatTokenEstimate(4252)).toBe("4.25k");
    expect(formatTokenEstimate(4000)).toBe("4k");
  });

  it("applies formatter callback when provided", () => {
    expect(formatTokenEstimate(1500, (text) => `*${text}*`)).toBe("*1.5k*");
  });
});

describe("runUtils #formatTokenValue", () => {
  const usageWithActuals = {
    input_tokens: 10,
    output_tokens: 20,
    reasoning_tokens: 5,
    total_tokens: 35,
  };

  it("marks values as estimated when usage fields are missing", () => {
    const result = formatTokenValue(1234, {}, 0);
    expect(result.endsWith("*")).toBe(true);
  });

  it("returns clean numbers when values are present", () => {
    const result = formatTokenValue(10, usageWithActuals, 0);
    expect(result).toBe("10");
  });
});
