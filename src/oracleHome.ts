import os from "node:os";
import path from "node:path";

let oracleHomeDirOverride: string | null = null;

/**
 * Test-only hook: avoid mutating process.env (shared across Vitest worker threads).
 * This override is scoped to the current Node worker.
 */
export function setOracleHomeDirOverrideForTest(dir: string | null): void {
  oracleHomeDirOverride = dir;
}

export function getOracleHomeDir(): string {
  return oracleHomeDirOverride ?? process.env.ORACLE_HOME_DIR ?? path.join(os.homedir(), ".oracle");
}
