import { formatUSD } from "./format.js";

export function formatElapsedCompact(ms: number): string {
  if (!Number.isFinite(ms) || ms < 0) {
    return "unknown";
  }
  if (ms < 60_000) {
    return `${(ms / 1000).toFixed(1)}s`;
  }
  if (ms < 60 * 60_000) {
    const minutes = Math.floor(ms / 60_000);
    const seconds = Math.floor((ms % 60_000) / 1000);
    return `${minutes}m${seconds.toString().padStart(2, "0")}s`;
  }
  const hours = Math.floor(ms / (60 * 60_000));
  const minutes = Math.floor((ms % (60 * 60_000)) / 60_000);
  return `${hours}h${minutes.toString().padStart(2, "0")}m`;
}

export function formatFinishLine({
  elapsedMs,
  model,
  costUsd,
  tokensPart,
  summaryExtraParts,
  detailParts,
}: {
  elapsedMs: number;
  model: string | null;
  costUsd?: number | null;
  tokensPart: string | null;
  summaryExtraParts?: Array<string | null> | null;
  detailParts?: Array<string | null> | null;
}): { line1: string; line2?: string } {
  const line1Parts: Array<string | null> = [
    formatElapsedCompact(elapsedMs),
    typeof costUsd === "number" ? formatUSD(costUsd) : null,
    model,
    tokensPart,
    ...(summaryExtraParts ?? []),
  ];
  const line1 = line1Parts
    .filter((part): part is string => typeof part === "string" && part.length > 0)
    .join(" · ");

  const line2Parts = (detailParts ?? []).filter(
    (part): part is string => typeof part === "string" && part.length > 0,
  );
  if (line2Parts.length === 0) {
    return { line1 };
  }
  return { line1, line2: line2Parts.join(" | ") };
}
