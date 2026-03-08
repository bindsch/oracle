import path from "node:path";

export type FenceLanguage = string | null | undefined;

const EXT_TO_LANG: Record<string, string> = {
  ".ts": "ts",
  ".tsx": "tsx",
  ".js": "js",
  ".jsx": "jsx",
  ".json": "json",
  ".swift": "swift",
  ".md": "md",
  ".sh": "bash",
  ".bash": "bash",
  ".zsh": "bash",
  ".py": "python",
  ".rb": "ruby",
  ".rs": "rust",
  ".go": "go",
  ".java": "java",
  ".c": "c",
  ".h": "c",
  ".cpp": "cpp",
  ".hpp": "cpp",
  ".css": "css",
  ".scss": "scss",
  ".sql": "sql",
  ".yaml": "yaml",
  ".yml": "yaml",
};

function detectFenceLanguage(displayPath: string): string | null {
  const ext = path.extname(displayPath).toLowerCase();
  return EXT_TO_LANG[ext] ?? null;
}

function pickFence(content: string): string {
  // Choose a fence longer than any backtick run inside the file so the block can't prematurely close.
  const matches = [...content.matchAll(/`+/g)];
  const maxTicks = matches.reduce((max, m) => Math.max(max, m[0].length), 0);
  const fenceLength = Math.max(3, maxTicks + 1);
  return "`".repeat(fenceLength);
}

export function formatFileSection(displayPath: string, content: string): string {
  const fence = pickFence(content);
  const lang = detectFenceLanguage(displayPath);
  const normalized = content.replace(/\s+$/u, "");
  const header = `### File: ${displayPath}`;
  const fenceOpen = lang ? `${fence}${lang}` : fence;
  return [header, fenceOpen, normalized, fence, ""].join("\n");
}
