import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  resolveNotificationSettings,
  testHelpers,
  sendSessionNotification,
} from "../src/cli/notifier.js";
import type { NotificationContent, NotificationSettings } from "../src/cli/notifier.js";

vi.mock("toasted-notifier", () => ({ default: { notify: vi.fn(async () => undefined) } }));
vi.mock("node:child_process", () => ({
  spawn: vi.fn(() => ({
    on: (event: string, cb: (exit?: number) => void) => {
      if (event === "exit") cb(0);
    },
  })),
}));

const baseEnv = { ...process.env };

async function getMocks() {
  const notifier = (await import("toasted-notifier")).default as {
    notify: ReturnType<typeof vi.fn>;
  };
  const { spawn } = await import("node:child_process");
  return { notifier, spawn: spawn as unknown as ReturnType<typeof vi.fn> };
}

beforeEach(async () => {
  const { notifier, spawn } = await getMocks();
  notifier.notify.mockClear();
  spawn.mockClear?.();
});

afterEach(() => {
  vi.restoreAllMocks();
  for (const key of Object.keys(process.env)) {
    if (!(key in baseEnv)) {
      delete process.env[key];
    }
  }
  Object.assign(process.env, baseEnv);
});

describe("resolveNotificationSettings", () => {
  it("defaults to enabled when not in CI or SSH", () => {
    const result = resolveNotificationSettings({
      cliNotify: undefined,
      cliNotifySound: undefined,
      env: {},
    });
    expect(result.enabled).toBe(true);
    expect(result.sound).toBe(false);
  });

  it("disables by default in CI", () => {
    // biome-ignore lint/style/useNamingConvention: environment variable name
    const result = resolveNotificationSettings({
      cliNotify: undefined,
      cliNotifySound: undefined,
      env: { CI: "1" },
    });
    expect(result.enabled).toBe(false);
  });

  it("honors explicit CLI override", () => {
    // biome-ignore lint/style/useNamingConvention: environment variable name
    const result = resolveNotificationSettings({
      cliNotify: true,
      cliNotifySound: true,
      env: { CI: "1" },
    });
    expect(result.enabled).toBe(true);
    expect(result.sound).toBe(true);
  });

  it("parses env toggles", () => {
    // biome-ignore lint/style/useNamingConvention: environment variable name
    const result = resolveNotificationSettings({
      cliNotify: undefined,
      cliNotifySound: undefined,
      env: { ORACLE_NOTIFY: "off" },
    });
    expect(result.enabled).toBe(false);
  });

  it("sanitizes and truncates previews to 200 characters", () => {
    const longPreview = `\`code\` ${"a".repeat(300)}`;
    const sanitized = testHelpers.sanitizePreview(longPreview);
    expect(sanitized).toBeDefined();
    expect(sanitized?.length).toBe(200);
    expect(sanitized?.includes("code")).toBe(true);
    expect(sanitized?.endsWith("…")).toBe(true);
  });

  it("sends notifications in non-test envs and sanitizes output", async () => {
    // Allow notifications by clearing test env markers.
    delete process.env.VITEST;
    delete process.env.VITEST_WORKER_ID;
    delete process.env.JEST_WORKER_ID;
    process.env.NODE_ENV = "development";
    process.env.ORACLE_DISABLE_NOTIFICATIONS = "0";
    vi.spyOn(process, "platform", "get").mockReturnValue("linux" as NodeJS.Platform);

    const payload: NotificationContent = {
      sessionId: "sess-1",
      sessionName: "demo run",
      mode: "api",
      model: "gpt-5.1",
      usage: { inputTokens: 1000, outputTokens: 500 },
      characters: 1500,
    };
    const settings: NotificationSettings = { enabled: true, sound: false };
    const log = vi.fn();

    await sendSessionNotification(
      payload,
      settings,
      log,
      "Preview **bold** with `code` and a [link](https://x.test)",
    );

    const { notifier } = await getMocks();
    expect(notifier.notify).toHaveBeenCalledTimes(1);
    const call = notifier.notify.mock.calls[0]?.[0];
    expect(call?.title).toContain("Oracle");
    expect(call?.message).toContain("demo run");
    expect(call?.message).toContain("chars");
    expect(call?.message).not.toContain("**");
    expect(call?.message).not.toContain("`code`");
    expect(call?.sound).toBe(false);
    expect(log).not.toHaveBeenCalled();
  });
});
