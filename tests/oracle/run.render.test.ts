import { describe, expect, it, vi } from "vitest";

import type { RunOracleOptions, RunOracleDeps, OracleResponse } from "../../src/oracle.js";

async function loadRunOracleWithTty(isTty: boolean, mockRendered?: string) {
  const originalTty = (process.stdout as { isTTY?: boolean }).isTTY;
  const originalForceColor = process.env.FORCE_COLOR;
  (process.stdout as { isTTY?: boolean }).isTTY = isTty;
  process.env.FORCE_COLOR = "1";
  vi.resetModules();
  // `runOracle` treats a "rich TTY" as `stdout.isTTY && chalk.level > 0`.
  // In Vitest, Chalk can be preloaded before FORCE_COLOR is set, so force a non-zero level.
  vi.doMock("chalk", async () => {
    const actual = await vi.importActual<typeof import("chalk")>("chalk");
    // biome-ignore lint/style/useNamingConvention: Chalk exports use PascalCase
    const CHALK_CTOR = (actual as unknown as { Chalk?: new (opts: { level: number }) => unknown })
      .Chalk;
    const forced = CHALK_CTOR ? new CHALK_CTOR({ level: 1 }) : actual.default;
    return { ...(actual as unknown as Record<string, unknown>), default: forced };
  });
  if (mockRendered) {
    vi.doMock("../../src/cli/markdownRenderer.js", () => ({
      renderMarkdownAnsi: vi.fn(() => mockRendered),
    }));
  }
  const { runOracle } = await import("../../src/oracle/run.js");
  const renderer = await import("../../src/cli/markdownRenderer.js");
  return {
    runOracle,
    renderer,
    restoreEnv: () => {
      if (originalTty === undefined) {
        // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
        delete (process.stdout as { isTTY?: boolean }).isTTY;
      } else {
        (process.stdout as { isTTY?: boolean }).isTTY = originalTty;
      }
      if (originalForceColor === undefined) {
        delete process.env.FORCE_COLOR;
      } else {
        process.env.FORCE_COLOR = originalForceColor;
      }
      vi.resetModules();
    },
  };
}

function makeStreamingClient(delta: string | string[]): RunOracleDeps["clientFactory"] {
  const deltas = Array.isArray(delta) ? delta : [delta];
  const fullText = deltas.join("");
  const finalResponse: OracleResponse = {
    id: "resp-1",
    status: "completed",
    usage: { input_tokens: 0, output_tokens: fullText.length, total_tokens: fullText.length },
    output: [{ type: "text", text: fullText }],
  };
  const stream = {
    async *[Symbol.asyncIterator]() {
      for (const chunk of deltas) {
        yield { type: "chunk", delta: chunk };
      }
    },
    finalResponse: async () => finalResponse,
  };
  return () => ({
    responses: {
      stream: () => stream,
      create: vi.fn(),
      retrieve: vi.fn().mockResolvedValue(finalResponse),
    },
  });
}

describe("runOracle streaming rendering", () => {
  const baseOptions: RunOracleOptions = {
    prompt: "p",
    model: "gpt-5.1",
    search: false,
  };

  it("renders streamed markdown once in rich TTY by default", async () => {
    const { runOracle, restoreEnv } = await loadRunOracleWithTty(true);
    const logSink: string[] = [];
    const stdoutSink: string[] = [];
    const stdoutSpy = vi.spyOn(process.stdout, "write").mockImplementation(((
      chunk: string | Uint8Array,
    ) => {
      stdoutSink.push(String(chunk));
      return true;
    }) as typeof process.stdout.write);

    await runOracle(baseOptions, {
      clientFactory: makeStreamingClient("# Title\n- item"),
      write: (text) => {
        logSink.push(text);
        return true;
      },
      wait: async () => {},
    });

    const rendered = stdoutSink.join("");
    const combined = rendered + logSink.join("");
    expect(combined).toContain("Title");
    expect(combined).toContain("item");
    expect(rendered.length).toBeGreaterThan(0); // stdout receives rendered markdown on TTY
    stdoutSpy.mockRestore();
    restoreEnv();
  }, 15_000);

  it("streams markdown fragments without in-place render sequences in a rich TTY", async () => {
    const { runOracle, restoreEnv } = await loadRunOracleWithTty(true);
    const stdoutSink: string[] = [];
    const stdoutSpy = vi.spyOn(process.stdout, "write").mockImplementation(((
      chunk: string | Uint8Array,
    ) => {
      stdoutSink.push(String(chunk));
      return true;
    }) as typeof process.stdout.write);

    await runOracle(baseOptions, {
      clientFactory: makeStreamingClient(["Hello\n", "**bold**\n"]),
      write: () => true,
      wait: async () => {},
    });

    const output = stdoutSink.join("");
    expect(output).toContain("Hello");
    expect(output).toContain("bold");
    expect(output).not.toContain("\u001b[?2026h"); // no synchronized output begin
    expect(output).not.toContain("\u001b[?2026l"); // no synchronized output end
    expect(output).not.toContain("\u001b[0J"); // no in-place clear
    expect(output).not.toContain("\u001b[?25l"); // no cursor hide
    expect(output).not.toContain("\u001b[?25h"); // no cursor show

    stdoutSpy.mockRestore();
    restoreEnv();
  }, 15_000);

  it("streams raw text immediately when --render-plain is used", async () => {
    const { runOracle, restoreEnv } = await loadRunOracleWithTty(true);
    const sink: string[] = [];
    const stdoutSink: string[] = [];
    const stdoutSpy = vi.spyOn(process.stdout, "write").mockImplementation(((
      chunk: string | Uint8Array,
    ) => {
      stdoutSink.push(String(chunk));
      return true;
    }) as typeof process.stdout.write);

    await runOracle(
      { ...baseOptions, renderPlain: true },
      {
        clientFactory: makeStreamingClient("# Title\n- item"),
        write: (text) => {
          sink.push(String(text));
          return true;
        },
        wait: async () => {},
      },
    );

    const output = sink.join("");
    const rendered = stdoutSink.join("");
    expect(output).toContain("# Title");
    expect(rendered).toContain("# Title");
    expect(rendered).not.toContain("\u001b[");
    stdoutSpy.mockRestore();
    restoreEnv();
  }, 15_000);

  it("streams raw markdown when stdout is not a TTY", async () => {
    const { runOracle, restoreEnv } = await loadRunOracleWithTty(false);
    const stdoutSink: string[] = [];
    const stdoutSpy = vi.spyOn(process.stdout, "write").mockImplementation(((
      chunk: string | Uint8Array,
    ) => {
      stdoutSink.push(String(chunk));
      return true;
    }) as typeof process.stdout.write);

    await runOracle(baseOptions, {
      clientFactory: makeStreamingClient("# Title\n- item"),
      write: () => true,
      wait: async () => {},
    });

    const output = stdoutSink.join("");
    expect(output).toContain("# Title");
    expect(output).toContain("- item");
    expect(output).not.toContain("\u001b[");

    stdoutSpy.mockRestore();
    restoreEnv();
  }, 15_000);
});
