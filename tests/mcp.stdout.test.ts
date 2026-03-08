import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { spawn } from "node:child_process";
import path from "node:path";
import { once } from "node:events";

describe("oracle-mcp stdout hygiene", () => {
  let proc: ReturnType<typeof spawn>;
  const stdoutChunks: string[] = [];

  beforeAll(async () => {
    const entry = path.join(process.cwd(), "dist/bin/oracle-mcp.js");
    proc = spawn(process.execPath, [entry], { stdio: ["pipe", "pipe", "pipe"] });
    proc.stdout?.on("data", (chunk) => stdoutChunks.push(String(chunk)));
    // give the process time to start; we deliberately do not send any input
    await new Promise((resolve) => setTimeout(resolve, 300));
  }, 10_000);

  afterAll(async () => {
    if (proc) {
      proc.kill("SIGTERM");
      await Promise.race([once(proc, "exit"), new Promise((r) => setTimeout(r, 500))]);
    }
  });

  it("does not emit non-JSON noise on startup", () => {
    const combined = stdoutChunks.join("").trim();
    expect(combined).toBe("");
  });
});
