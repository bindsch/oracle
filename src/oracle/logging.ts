export function maskApiKey(key: string | undefined | null): string | null {
  if (!key) return null;
  if (key.length <= 8) return `${key[0] ?? ""}***${key[key.length - 1] ?? ""}`;
  const prefix = key.slice(0, 4);
  const suffix = key.slice(-4);
  return `${prefix}****${suffix}`;
}

export function formatBaseUrlForLog(raw: string | undefined): string {
  if (!raw) return "";
  try {
    const parsed = new URL(raw);
    const segments = parsed.pathname.split("/").filter(Boolean);
    let path = "";
    if (segments.length > 0) {
      path = `/${segments[0]}`;
      if (segments.length > 1) {
        path += "/...";
      }
    }
    const allowedQueryKeys = ["api-version"];
    const maskedQuery = allowedQueryKeys
      .filter((key) => parsed.searchParams.has(key))
      .map((key) => `${key}=***`);
    const query = maskedQuery.length > 0 ? `?${maskedQuery.join("&")}` : "";
    return `${parsed.protocol}//${parsed.host}${path}${query}`;
  } catch {
    const trimmed = raw.trim();
    if (trimmed.length <= 64) return trimmed;
    return `${trimmed.slice(0, 32)}…${trimmed.slice(-8)}`;
  }
}
