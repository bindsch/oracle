export function formatUSD(value: number): string {
  if (!Number.isFinite(value)) {
    return "n/a";
  }
  // Display with 4 decimal places, rounding to $0.0001 minimum granularity.
  return `$${value.toFixed(4)}`;
}

export function formatNumber(
  value: number | null | undefined,
  { estimated = false }: { estimated?: boolean } = {},
): string {
  if (value == null) {
    return "n/a";
  }
  const suffix = estimated ? " (est.)" : "";
  return `${value.toLocaleString()}${suffix}`;
}

export function formatElapsed(ms: number): string {
  if (ms >= 60 * 60 * 1000) {
    const hours = Math.floor(ms / (60 * 60 * 1000));
    const minutes = Math.floor((ms % (60 * 60 * 1000)) / (60 * 1000));
    return `${hours}h ${minutes}m`;
  }
  if (ms >= 60 * 1000) {
    const minutes = Math.floor(ms / (60 * 1000));
    const seconds = Math.floor((ms % (60 * 1000)) / 1000);
    return `${minutes}m ${seconds}s`;
  }
  if (ms >= 1000) {
    return `${Math.floor(ms / 1000)}s`;
  }
  return `${Math.round(ms)}ms`;
}
