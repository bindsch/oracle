import os from "node:os";
import path from "node:path";
import { sessionStore } from "../sessionStore.js";

export function resolveOutputPath(input: string | undefined, cwd: string): string | undefined {
  if (!input || input.trim().length === 0) {
    return undefined;
  }
  const expanded = input.startsWith("~/") ? path.join(os.homedir(), input.slice(2)) : input;
  if (expanded === "-" || expanded === "/dev/stdout") {
    return expanded;
  }
  const absolute = path.isAbsolute(expanded) ? expanded : path.resolve(cwd, expanded);
  const sessionsDir = sessionStore.sessionsDir();
  const normalizedSessionsDir = path.resolve(sessionsDir);
  const normalizedTarget = path.resolve(absolute);
  if (
    normalizedTarget === normalizedSessionsDir ||
    normalizedTarget.startsWith(`${normalizedSessionsDir}${path.sep}`)
  ) {
    throw new Error(
      `Refusing to write output inside session storage (${normalizedSessionsDir}). Choose another path.`,
    );
  }
  return absolute;
}
