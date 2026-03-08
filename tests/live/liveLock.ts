import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

const LOCK_ROOT = path.join(os.homedir(), ".oracle", "live-test-locks");

type LockInfo = {
  pid: number;
  label: string;
  startedAt: number;
};

function lockDir(label: string): string {
  return path.join(LOCK_ROOT, `${label}.lock`);
}

function lockInfoPath(label: string): string {
  return path.join(lockDir(label), "info.json");
}

function isProcessRunning(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

export async function acquireLiveTestLock(
  label: string,
  timeoutMs = 20 * 60 * 1000,
): Promise<void> {
  await fs.mkdir(LOCK_ROOT, { recursive: true });
  const deadline = Date.now() + timeoutMs;
  const dir = lockDir(label);
  const infoPath = lockInfoPath(label);

  while (Date.now() < deadline) {
    try {
      await fs.mkdir(dir);
      const info: LockInfo = { pid: process.pid, label, startedAt: Date.now() };
      await fs.writeFile(infoPath, JSON.stringify(info, null, 2), "utf8");
      return;
    } catch (error) {
      const err = error as NodeJS.ErrnoException | undefined;
      if (err?.code !== "EEXIST") {
        throw error;
      }
    }

    let stale = false;
    try {
      const raw = await fs.readFile(infoPath, "utf8");
      const info = JSON.parse(raw) as Partial<LockInfo>;
      if (typeof info.pid === "number" && !isProcessRunning(info.pid)) {
        stale = true;
      }
    } catch {
      // ignore read/parse failures; treat as active lock
    }
    if (stale) {
      await fs.rm(dir, { recursive: true, force: true });
      continue;
    }
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }

  throw new Error(`Timed out waiting for live test lock (${label}).`);
}

export async function releaseLiveTestLock(label: string): Promise<void> {
  await fs.rm(lockDir(label), { recursive: true, force: true });
}
