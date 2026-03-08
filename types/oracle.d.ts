declare module "../src/oracle.js" {
  export function buildPrompt(...args: unknown[]): string;
  export function runOracle(...args: unknown[]): Promise<any>;
  export function renderPromptMarkdown(...args: unknown[]): Promise<string>;
}
