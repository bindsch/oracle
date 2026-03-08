import { cp, mkdir, stat } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

async function main() {
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
  const source = path.join(root, "vendor", "oracle-notifier");
  const targetDir = path.join(root, "dist", "vendor");
  const target = path.join(targetDir, "oracle-notifier");

  try {
    await stat(source);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(`Skipping vendor copy: ${message}`);
    return;
  }

  await mkdir(targetDir, { recursive: true });
  await cp(source, target, { recursive: true });
  console.log(`Copied ${source} -> ${target}`);
}

main().catch((error) => {
  console.warn(`Skipping vendor copy: ${error instanceof Error ? error.message : String(error)}`);
});
