import chalk from "chalk";

// Keep snapshot/log output stable across environments
chalk.level = 0;

// Ensure prompt-length guardrails are deterministic in tests
if (!process.env.ORACLE_MIN_PROMPT_CHARS) {
  process.env.ORACLE_MIN_PROMPT_CHARS = "20";
}

// Silence noisy env-dependent defaults (e.g., notifications)
process.env.ORACLE_NO_DETACH = "1";
