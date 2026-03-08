import { describe, expect, test, vi } from "vitest";
import { applyHiddenAliases, type HiddenAliasOptions } from "../../src/cli/hiddenAliases.js";

describe("applyHiddenAliases", () => {
  test("maps --mode to engine when engine not set", () => {
    const setOptionValue = vi.fn();
    const opts: HiddenAliasOptions = { mode: "browser" };

    applyHiddenAliases(opts, setOptionValue);

    expect(opts.engine).toBe("browser");
    expect(setOptionValue).toHaveBeenCalledWith("engine", "browser");
  });

  test("does not override explicit engine", () => {
    const setOptionValue = vi.fn();
    const opts: HiddenAliasOptions = { mode: "browser", engine: "api" };

    applyHiddenAliases(opts, setOptionValue);

    expect(opts.engine).toBe("api");
    expect(setOptionValue).not.toHaveBeenCalledWith("engine", expect.anything());
  });

  test("merges --include into --file and notifies setter", () => {
    const setOptionValue = vi.fn();
    const opts: HiddenAliasOptions = { file: ["a.ts"], include: ["b.ts", "c.ts"] };

    applyHiddenAliases(opts, setOptionValue);

    expect(opts.file).toEqual(["a.ts", "b.ts", "c.ts"]);
    expect(setOptionValue).toHaveBeenCalledWith("file", ["a.ts", "b.ts", "c.ts"]);
  });

  test("maps --message to --prompt only when prompt missing", () => {
    const setOptionValue = vi.fn();
    const opts: HiddenAliasOptions = { message: "hello" };

    applyHiddenAliases(opts, setOptionValue);

    expect(opts.prompt).toBe("hello");
    expect(setOptionValue).toHaveBeenCalledWith("prompt", "hello");

    setOptionValue.mockClear();
    const optsWithPrompt: HiddenAliasOptions = { message: "ignored", prompt: "kept" };
    applyHiddenAliases(optsWithPrompt, setOptionValue);
    expect(optsWithPrompt.prompt).toBe("kept");
    expect(setOptionValue).not.toHaveBeenCalled();
  });
});
