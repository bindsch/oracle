import type { OracleResponse, PreviewMode } from "./types.js";

export function resolvePreviewMode(value: boolean | string | undefined): PreviewMode | undefined {
  const allowed = new Set<PreviewMode>(["summary", "json", "full"]);
  if (typeof value === "string" && value.length > 0) {
    return allowed.has(value as PreviewMode) ? (value as PreviewMode) : "summary";
  }
  if (value) {
    return "summary";
  }
  return undefined;
}

/**
 * Format a token count, abbreviating thousands as e.g. 11.38k and trimming trailing zeros.
 */
export function formatTokenCount(value: number): string {
  if (Math.abs(value) >= 1000) {
    const abbreviated = (value / 1000)
      .toFixed(2)
      .replace(/\.0+$/, "")
      .replace(/\.([1-9]*)0$/, ".$1");
    return `${abbreviated}k`;
  }
  return value.toLocaleString();
}

export function formatTokenEstimate(
  value: number,
  format: (text: string) => string = (text) => text,
): string {
  return format(formatTokenCount(value));
}

export function formatTokenValue(
  value: number,
  usage: OracleResponse["usage"],
  index: number,
): string {
  const estimatedFlag =
    (index === 0 && usage?.input_tokens == null) ||
    (index === 1 && usage?.output_tokens == null) ||
    (index === 2 && usage?.reasoning_tokens == null) ||
    (index === 3 && usage?.total_tokens == null);
  const text = formatTokenCount(value);
  return estimatedFlag ? `${text}*` : text;
}
