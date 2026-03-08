import { describe, expect, test } from "vitest";
import { Command } from "commander";
import { shouldRequirePrompt } from "../../src/cli/promptRequirement.js";
import { resolvePreviewMode } from "../../src/cli/options.js";

describe("positional prompt fallback", () => {
  test("copies positional into --prompt for root command", () => {
    const program = new Command();
    program.argument("[prompt]");
    program.option("-p, --prompt <text>");
    program.allowUnknownOption(true);
    program.parse(["hello world"], { from: "user" });
    const opts = program.opts();
    const positional = program.args?.[0];
    if (!opts.prompt && positional) {
      opts.prompt = positional;
    }
    expect(opts.prompt).toBe("hello world");
  });

  test("prompt guard accepts positional when preview enabled", () => {
    const rawArgs = ["--preview"];
    const opts = { preview: resolvePreviewMode(true), prompt: "positional prompt" };
    const requires = shouldRequirePrompt(rawArgs, opts);
    expect(requires).toBe(false);
  });

  test("status subcommand still bypasses prompt requirement", () => {
    const requires = shouldRequirePrompt(["status"], { status: undefined });
    expect(requires).toBe(false);
  });
});
