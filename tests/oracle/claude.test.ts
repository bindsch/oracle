import { afterEach, describe, expect, test, vi } from "vitest";
import { createClaudeClient, resolveClaudeModelId } from "../../src/oracle/claude.js";
import type { OracleRequestBody } from "../../src/oracle/types.js";

const mockBody: OracleRequestBody = {
  model: "claude-4.1-opus",
  instructions: "respond helpfully",
  input: [
    {
      role: "user",
      content: [{ type: "input_text", text: "hello" }],
    },
  ],
};

describe("claude client", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  test("resolveClaudeModelId normalizes known aliases", () => {
    expect(resolveClaudeModelId("claude-4.1-opus")).toBe("claude-opus-4-1");
    expect(resolveClaudeModelId("claude-4.5-sonnet")).toBe("claude-sonnet-4-5");
    expect(resolveClaudeModelId("claude-something-else")).toBe("claude-something-else");
  });

  test("createClaudeClient maps text output", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      json: async () => ({
        id: "abc",
        content: [{ text: "hi there" }],
        usage: { input_tokens: 1, output_tokens: 2 },
      }),
    });
    vi.stubGlobal("fetch", fetchMock);

    const client = createClaudeClient("sk-test", "claude-4.1-opus");
    const resp = await client.responses.create(mockBody);

    expect(fetchMock).toHaveBeenCalled();
    expect(resp.output_text?.[0]).toBe("hi there");
    expect(resp.usage?.total_tokens).toBe(3);
  });
});
