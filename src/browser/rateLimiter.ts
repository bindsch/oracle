import fs from "node:fs/promises";
import path from "node:path";
import { getOracleHomeDir } from "../oracleHome.js";
import { delay } from "./utils.js";

const DEFAULT_MAX_PARALLEL = 8;
const LOCK_DIR_NAME = "browser-locks";
const STALE_LOCK_MS = 30 * 60 * 1000; // 30 minutes
const POLL_INTERVAL_MS = 5_000;
const MAX_WAIT_MS = 10 * 60 * 1000; // 10 minutes

interface RateLimitConfig {
  maxParallel?: number;
  maxWaitMs?: number;
}

function getLockDir(): string {
  return path.join(getOracleHomeDir(), LOCK_DIR_NAME);
}

async function ensureLockDir(): Promise<string> {
  const dir = getLockDir();
  await fs.mkdir(dir, { recursive: true });
  return dir;
}

async function cleanStaleLocks(lockDir: string): Promise<void> {
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
      // Remove if process is dead
      if (parsed.pid && !isProcessAlive(parsed.pid)) {
        await fs.unlink(lockPath).catch(() => {});
        continue;
      }
      // Remove if older than stale threshold
      if (parsed.startedAt && now - parsed.startedAt > STALE_LOCK_MS) {
        await fs.unlink(lockPath).catch(() => {});
      }
    } catch {
      // Corrupt lock file — remove it
      await fs.unlink(lockPath).catch(() => {});
    }
  }
}

function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
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

export async function acquireBrowserSlot(
  sessionId: string,
  logger: (msg: string) => void,
  config?: RateLimitConfig,
): Promise<{ release: () => Promise<void> }> {
  const maxParallel = config?.maxParallel ?? DEFAULT_MAX_PARALLEL;
  const maxWaitMs = config?.maxWaitMs ?? MAX_WAIT_MS;
  const lockDir = await ensureLockDir();
  const lockFile = path.join(lockDir, `${sessionId}.lock`);

  // Clean up dead/stale locks first
  await cleanStaleLocks(lockDir);

  const start = Date.now();
  let logged = false;

  while (true) {
    const active = await countActiveLocks(lockDir);
    if (active < maxParallel) {
      // Slot available — claim it
      const lockData = JSON.stringify({
        pid: process.pid,
        sessionId,
        startedAt: Date.now(),
      });
      await fs.writeFile(lockFile, lockData, { flag: "wx" }).catch(async () => {
        // File already exists (race), overwrite
        await fs.writeFile(lockFile, lockData);
      });
      const release = async () => {
        await fs.unlink(lockFile).catch(() => {});
      };
      // Clean up on process exit
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
      logger(`Rate limit: ${active}/${maxParallel} browser sessions active, waiting for a slot…`);
      logged = true;
    }

    if (Date.now() - start > maxWaitMs) {
      throw new Error(
        `Rate limit: waited ${Math.round(maxWaitMs / 1000)}s for a browser slot ` +
          `(${active}/${maxParallel} active). Give up or increase maxParallelBrowserSessions.`,
      );
    }

    await delay(POLL_INTERVAL_MS);
    // Re-clean stale locks each iteration
    await cleanStaleLocks(lockDir);
  }
}
