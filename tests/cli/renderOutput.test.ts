import { beforeEach, describe, expect, test, vi } from "vitest";

vi.mock("../../src/cli/markdownRenderer.ts", () => ({
  ensureShikiReady: vi.fn(async () => {}),
  renderMarkdownAnsi: vi.fn((text: string) => `RENDERED:${text}`),
}));

import { formatRenderedMarkdown, shouldRenderRich } from "../../src/cli/renderOutput.js";
import { renderMarkdownAnsi, ensureShikiReady } from "../../src/cli/markdownRenderer.js";

describe("formatRenderedMarkdown", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  test("uses renderer when rich TTY", async () => {
    const out = await formatRenderedMarkdown("**hi**", { richTty: true });
    expect(out).toBe("RENDERED:**hi**");
    expect(ensureShikiReady).toHaveBeenCalled();
    expect(renderMarkdownAnsi).toHaveBeenCalledWith("**hi**");
  });

  test("returns raw markdown when not a TTY", async () => {
    const out = await formatRenderedMarkdown("_raw_", { richTty: false });
    expect(out).toBe("_raw_");
    expect(renderMarkdownAnsi).not.toHaveBeenCalled();
  });

  test("falls back to raw markdown when renderer throws", async () => {
    vi.mocked(renderMarkdownAnsi).mockImplementationOnce(() => {
      throw new Error("boom");
    });
    const out = await formatRenderedMarkdown("**boom**", { richTty: true });
    expect(out).toBe("**boom**");
  });

  test("shouldRenderRich respects override and TTY", () => {
    expect(shouldRenderRich({ richTty: false })).toBe(false);
    expect(shouldRenderRich({ richTty: true })).toBe(true);
  });
});
