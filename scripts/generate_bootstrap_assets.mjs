import { lstatSync, mkdirSync, readFileSync, readdirSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = fileURLToPath(new URL("../", import.meta.url));
const assetDirectories = [
  { source: "policies", target: "internal/bootstrapassets/files/policies", extension: ".yaml" },
  { source: "contracts/schemas", target: "internal/bootstrapassets/files/schemas", extension: ".json" },
];

function assetDirectory(root, relativePath, allowMissing = false) {
  let directory = root;
  for (const component of relativePath.split("/")) {
    directory = path.join(directory, component);
    try {
      if (!lstatSync(directory).isDirectory()) throw new Error(`Asset directory must be a real directory: ${directory}`);
    } catch (error) {
      if (!allowMissing || error.code !== "ENOENT") throw error;
    }
  }
  return directory;
}

function assetNames(directory, extension) {
  return readdirSync(directory, { withFileTypes: true })
    .filter((entry) => entry.name.endsWith(extension))
    .map((entry) => {
      if (!entry.isFile()) throw new Error(`Asset must be a regular file: ${path.join(directory, entry.name)}`);
      return entry.name;
    })
    .sort();
}

export function synchronizeBootstrapAssets(root, { check = false } = {}) {
  const resolvedRoot = realpathSync(root);
  const changes = [];
  for (const { source, target, extension } of assetDirectories) {
    const sourceDirectory = assetDirectory(resolvedRoot, source);
    const targetDirectory = assetDirectory(resolvedRoot, target, true);
    const names = assetNames(sourceDirectory, extension);
    let targetNames;
    try {
      targetNames = assetNames(targetDirectory, extension);
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
      targetNames = [];
    }
    for (const name of names) {
      const content = readFileSync(path.join(sourceDirectory, name));
      const outputPath = path.join(targetDirectory, name);
      let previous;
      try {
        previous = readFileSync(outputPath);
      } catch (error) {
        if (error.code !== "ENOENT") throw error;
      }
      if (!previous || !content.equals(previous)) changes.push({ outputPath, content });
    }
    for (const name of targetNames) {
      if (!names.includes(name)) changes.push({ outputPath: path.join(targetDirectory, name) });
    }
  }
  if (check && changes.length) {
    throw new Error(`Bootstrap assets are stale. Run make assets-generate.\n${changes.map(({ outputPath }) => path.relative(resolvedRoot, outputPath)).join("\n")}`);
  }
  for (const { outputPath, content } of changes) {
    if (content === undefined) {
      rmSync(outputPath);
    } else {
      mkdirSync(path.dirname(outputPath), { recursive: true });
      writeFileSync(outputPath, content);
    }
  }
  return changes.length;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  if (process.argv.slice(2).some((argument) => argument !== "--check")) {
    throw new Error("Usage: node scripts/generate_bootstrap_assets.mjs [--check]");
  }
  synchronizeBootstrapAssets(repositoryRoot, { check: process.argv.includes("--check") });
  console.log("Bootstrap assets are current.");
}
