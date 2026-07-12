import fs from "node:fs/promises";
import path from "node:path";
import { getOracleHomeDir } from "../oracleHome.js";
import { delay } from "./utils.js";

const DEFAULT_MAX_PARALLEL = 8;
const LOCK_DIR_NAME = "browser-locks";
const SLOT_SUBDIR = "slot";
const STARTUP_SUBDIR = "startup";
const STALE_LOCK_MS = 30 * 60 * 1000;
const STARTUP_STALE_LOCK_MS = 5 * 60 * 1000;
const POLL_INTERVAL_MS = 1_000;
const MAX_WAIT_MS = 30 * 60 * 1000;
const STARTUP_MAX_WAIT_MS = 30 * 60 * 1000;

interface RateLimitConfig {
  maxParallel?: number;
  maxWaitMs?: number;
}

async function ensureLockDir(subdir: string): Promise<string> {
  const dir = path.join(getOracleHomeDir(), LOCK_DIR_NAME, subdir);
  await fs.mkdir(dir, { recursive: true });
  return dir;
}

function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

async function cleanStaleLocks(lockDir: string, staleMs: number): Promise<void> {
  let entries: string[];
  try {
    entries = await fs.readdir(lockDir);
  } catch {
    return;
  }
  const now = Date.now();
  for (const entry of entries) {
    if (!entry.endsWith(".lock")) continue;
    const lockPath = path.join(lockDir, entry);
    try {
      const content = await fs.readFile(lockPath, "utf8");
      const parsed = JSON.parse(content) as { pid?: number; startedAt?: number };
      if (parsed.pid && !isProcessAlive(parsed.pid)) {
        await fs.unlink(lockPath).catch(() => {});
        continue;
      }
      if (parsed.startedAt && now - parsed.startedAt > staleMs) {
        await fs.unlink(lockPath).catch(() => {});
      }
    } catch {
      await fs.unlink(lockPath).catch(() => {});
    }
  }
}

async function countActiveLocks(lockDir: string): Promise<number> {
  let entries: string[];
  try {
    entries = await fs.readdir(lockDir);
  } catch {
    return 0;
  }
  return entries.filter((e) => e.endsWith(".lock")).length;
}

async function acquireLock(params: {
  subdir: string;
  sessionId: string;
  maxParallel: number;
  maxWaitMs: number;
  staleMs: number;
  logger: (msg: string) => void;
  waitMessage: (active: number, max: number) => string;
  timeoutMessage: (active: number, max: number, waitedMs: number) => string;
}): Promise<{ release: () => Promise<void> }> {
  const lockDir = await ensureLockDir(params.subdir);
  const lockFile = path.join(lockDir, `${params.sessionId}.lock`);

  await cleanStaleLocks(lockDir, params.staleMs);

  const start = Date.now();
  let logged = false;

  while (true) {
    const active = await countActiveLocks(lockDir);
    if (active < params.maxParallel) {
      const lockData = JSON.stringify({
        pid: process.pid,
        sessionId: params.sessionId,
        startedAt: Date.now(),
      });
      await fs.writeFile(lockFile, lockData, { flag: "wx" }).catch(async () => {
        await fs.writeFile(lockFile, lockData);
      });
      const release = async () => {
        await fs.unlink(lockFile).catch(() => {});
      };
      const exitHandler = () => {
        try {
          require("node:fs").unlinkSync(lockFile);
        } catch {
          // best effort
        }
      };
      process.once("exit", exitHandler);
      process.once("SIGINT", exitHandler);
      process.once("SIGTERM", exitHandler);
      return { release };
    }

    if (!logged) {
      params.logger(params.waitMessage(active, params.maxParallel));
      logged = true;
    }

    if (Date.now() - start > params.maxWaitMs) {
      throw new Error(params.timeoutMessage(active, params.maxParallel, params.maxWaitMs));
    }

    await delay(POLL_INTERVAL_MS);
    await cleanStaleLocks(lockDir, params.staleMs);
  }
}

export async function acquireBrowserSlot(
  sessionId: string,
  logger: (msg: string) => void,
  config?: RateLimitConfig,
): Promise<{ release: () => Promise<void> }> {
  return acquireLock({
    subdir: SLOT_SUBDIR,
    sessionId,
    maxParallel: config?.maxParallel ?? DEFAULT_MAX_PARALLEL,
    maxWaitMs: config?.maxWaitMs ?? MAX_WAIT_MS,
    staleMs: STALE_LOCK_MS,
    logger,
    waitMessage: (active, max) =>
      `Rate limit: ${active}/${max} browser sessions active, waiting for a slot…`,
    timeoutMessage: (active, max, waitedMs) =>
      `Rate limit: waited ${Math.round(waitedMs / 1000)}s for a browser slot ` +
      `(${active}/${max} active). Give up or increase ORACLE_MAX_PARALLEL_BROWSER_SESSIONS.`,
  });
}

/**
 * Serialize the Chrome-claim window: at most one browser-mode oracle may be in
 * the launch / connect / prompt-submit phase at any moment. Released as soon as
 * the prompt has been submitted, so the rest of the run executes in parallel.
 */
export async function acquireBrowserStartupLock(
  sessionId: string,
  logger: (msg: string) => void,
  config?: { maxWaitMs?: number },
): Promise<{ release: () => Promise<void> }> {
  return acquireLock({
    subdir: STARTUP_SUBDIR,
    sessionId,
    maxParallel: 1,
    maxWaitMs: config?.maxWaitMs ?? STARTUP_MAX_WAIT_MS,
    staleMs: STARTUP_STALE_LOCK_MS,
    logger,
    waitMessage: () =>
      `Another oracle is mid-startup; waiting for it to claim its Chrome tab before launching…`,
    timeoutMessage: (_active, _max, waitedMs) =>
      `Startup lock: waited ${Math.round(waitedMs / 1000)}s for the previous oracle to finish its Chrome startup phase.`,
  });
}
