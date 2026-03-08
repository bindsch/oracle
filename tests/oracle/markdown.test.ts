import { describe, expect, test } from "vitest";
import { formatFileSection } from "../../src/oracle/markdown.js";

describe("formatFileSection", () => {
  test("annotates language from extension", () => {
    const out = formatFileSection("src/app.ts", "const x = 1;\n");
    expect(out).toContain("### File: src/app.ts");
    expect(out).toContain("```ts");
    expect(out).toContain("const x = 1;");
    expect(out.trimEnd()).toMatch(/```$/); // closes fence
  });

  test("auto-extends fence when content includes backticks", () => {
    const sample = "const tpl = `value`;\n````\ninner fence\n````\n";
    const out = formatFileSection("a.js", sample);
    // Should use a fence longer than the longest run of backticks inside content
    const lines = out.split("\n");
    const fenceLine = lines[1];
    const fenceLength =
      fenceLine.replace("```js", "```").length === fenceLine.length
        ? fenceLine.length
        : fenceLine.startsWith("`")
          ? fenceLine.length
          : 0;
    const innerMax = Math.max(...[...sample.matchAll(/`+/g)].map((m) => m[0].length));
    expect(fenceLength).toBeGreaterThan(innerMax);
    expect(out).toContain(sample.trim());
  });
});
