import { describe, it, expect, vi, afterEach } from "vitest";
import os from "node:os";
import path from "node:path";
import { mkdtemp, readFile } from "node:fs/promises";
import { saveFirstGeminiImageFromOutput } from "../../src/gemini-web/client.js";

describe("gemini-web saveFirstGeminiImageFromOutput", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("falls back to gg-dl URLs found in rawResponseText when parsed images are absent", async () => {
    const calls: string[] = [];

    vi.stubGlobal("fetch", async (input: RequestInfo | URL) => {
      const url = String(input);
      calls.push(url);
      if (calls.length === 1) {
        return new Response(null, {
          status: 302,
          headers: { location: "https://work.fife.usercontent.google.com/rd-gg-dl/xyz=s2048" },
        });
      }
      return new Response(new Uint8Array([9, 8, 7, 6]), {
        status: 200,
        headers: { "content-type": "image/jpeg" },
      });
    });

    const tempDir = await mkdtemp(path.join(os.tmpdir(), "oracle-gemini-"));
    const outputPath = path.join(tempDir, "out.jpg");

    const output = {
      rawResponseText: "prefix https://lh3.googleusercontent.com/gg-dl/abc suffix",
      text: "",
      thoughts: null,
      metadata: null,
      images: [],
    };

    const result = await saveFirstGeminiImageFromOutput(output, { a: "b" }, outputPath);
    expect(result).toEqual({ saved: true, imageCount: 1 });
    expect(calls[0]).toContain("lh3.googleusercontent.com/gg-dl/abc");
    expect(new Uint8Array(await readFile(outputPath))).toEqual(new Uint8Array([9, 8, 7, 6]));
  });
});
