import type { UserConfig } from "../config.js";

export type RemoteServiceConfigSource = "cli" | "config.browser" | "env" | "unset";

export interface ResolvedRemoteServiceConfig {
  host?: string;
  token?: string;
  sources: {
    host: RemoteServiceConfigSource;
    token: RemoteServiceConfigSource;
  };
}

function normalizeString(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length ? trimmed : undefined;
}

export function resolveRemoteServiceConfig({
  cliHost,
  cliToken,
  userConfig,
  env = process.env,
}: {
  cliHost?: string;
  cliToken?: string;
  userConfig?: UserConfig;
  env?: NodeJS.ProcessEnv;
}): ResolvedRemoteServiceConfig {
  const configBrowserHost = normalizeString(userConfig?.browser?.remoteHost);
  const configBrowserToken = normalizeString(userConfig?.browser?.remoteToken);

  const envHost = normalizeString(env.ORACLE_REMOTE_HOST);
  const envToken = normalizeString(env.ORACLE_REMOTE_TOKEN);

  const cliHostValue = normalizeString(cliHost);
  const cliTokenValue = normalizeString(cliToken);

  const host = cliHostValue ?? configBrowserHost ?? envHost;
  const token = cliTokenValue ?? configBrowserToken ?? envToken;

  const hostSource: RemoteServiceConfigSource = cliHostValue
    ? "cli"
    : configBrowserHost
      ? "config.browser"
      : envHost
        ? "env"
        : "unset";

  const tokenSource: RemoteServiceConfigSource = cliTokenValue
    ? "cli"
    : configBrowserToken
      ? "config.browser"
      : envToken
        ? "env"
        : "unset";

  return { host, token, sources: { host: hostSource, token: tokenSource } };
}
