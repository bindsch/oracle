import { describe, it, expect, vi } from "vitest";
import {
  resolveModelConfig,
  safeModelSlug,
  isOpenRouterBaseUrl,
  resetOpenRouterCatalogCacheForTest,
  getOpenRouterCatalogCacheSizeForTest,
  getOpenRouterCatalogCacheMaxEntriesForTest,
} from "../src/oracle/modelResolver.js";

describe("OpenRouter helpers", () => {
  it("slugifies model ids with slashes", () => {
    expect(safeModelSlug("minimax/minimax-m2")).toBe("minimax__minimax-m2");
  });

  it("hydrates config from OpenRouter catalog", async () => {
    const fetcher = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({
        data: [
          {
            id: "minimax/minimax-m2",
            context_length: 100000,
            pricing: { prompt: 2, completion: 3 },
          },
        ],
      }),
    }) as unknown as typeof fetch;

    const config = await resolveModelConfig("minimax/minimax-m2", {
      openRouterApiKey: "dummy",
      fetcher,
    });

    expect(config.apiModel).toBe("minimax/minimax-m2");
    expect(config.inputLimit).toBe(100000);
    expect(config.pricing?.inputPerToken).toBeCloseTo(2 / 1_000_000);
    expect(config.pricing?.outputPerToken).toBeCloseTo(3 / 1_000_000);
  });

  it("falls back to OpenRouter when provider key is missing but OPENROUTER_API_KEY is present", async () => {
    const fetcher = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ data: [] }),
    }) as unknown as typeof fetch;

    const config = await resolveModelConfig("minimax/minimax-m2", {
      openRouterApiKey: "dummy",
      fetcher,
    });

    expect(config.apiModel).toBe("minimax/minimax-m2");
    expect(isOpenRouterBaseUrl("https://openrouter.ai/api/v1/responses")).toBe(true);
  });

  it("detects OpenRouter base URLs", () => {
    expect(isOpenRouterBaseUrl("https://openrouter.ai/api/v1/responses")).toBe(true);
    expect(isOpenRouterBaseUrl("https://api.openai.com")).toBe(false);
  });

  it("keeps first-party model ids unprefixed when OpenRouter is inactive", async () => {
    const openai = await resolveModelConfig("gpt-5.1");
    const claude = await resolveModelConfig("claude-3-haiku-20240307");
    const grok = await resolveModelConfig("grok-4.1");

    expect(openai.apiModel ?? openai.model).toBe("gpt-5.1");
    expect(claude.apiModel ?? claude.model).toBe("claude-3-haiku-20240307");
    const grokId = grok.apiModel ?? grok.model;
    expect(grokId.includes("/")).toBe(false);
    expect(grokId.startsWith("grok-4")).toBe(true);
  });

  it("caps OpenRouter catalog cache size", async () => {
    resetOpenRouterCatalogCacheForTest();
    const fetcher = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ data: [] }),
    }) as unknown as typeof fetch;

    const maxEntries = getOpenRouterCatalogCacheMaxEntriesForTest();
    const keys = Array.from({ length: maxEntries + 5 }, (_, i) => `dummy-${i}`);
    for (const key of keys) {
      await resolveModelConfig("minimax/minimax-m2", { openRouterApiKey: key, fetcher });
    }

    expect(getOpenRouterCatalogCacheSizeForTest()).toBe(maxEntries);
  });
});
