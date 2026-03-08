import type { BrowserAttachment } from "./types.js";
import type { BrowserPromptArtifacts } from "./prompt.js";
import { formatBytes } from "./utils.js";

export function buildTokenEstimateSuffix(artifacts: BrowserPromptArtifacts): string {
  if (artifacts.tokenEstimateIncludesInlineFiles && artifacts.inlineFileCount > 0) {
    const count = artifacts.inlineFileCount;
    const plural = count === 1 ? "" : "s";
    return ` (includes ${count} inline file${plural})`;
  }
  if (artifacts.attachments.length > 0) {
    const count = artifacts.attachments.length;
    const plural = count === 1 ? "" : "s";
    return ` (prompt only; ${count} attachment${plural} excluded)`;
  }
  return "";
}

export function formatAttachmentLabel(attachment: BrowserAttachment): string {
  if (typeof attachment.sizeBytes !== "number" || Number.isNaN(attachment.sizeBytes)) {
    return attachment.displayPath;
  }
  return `${attachment.displayPath} (${formatBytes(attachment.sizeBytes)})`;
}
