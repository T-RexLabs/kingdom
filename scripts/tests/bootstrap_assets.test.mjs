import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { synchronizeBootstrapAssets } from "../generate_bootstrap_assets.mjs";

function fixture(t) {
  const root = mkdtempSync(path.join(os.tmpdir(), "kingdom-assets-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  for (const directory of ["policies", "internal/bootstrapassets/files/policies"]) mkdirSync(path.join(root, directory), { recursive: true });
  writeFileSync(path.join(root, "internal/bootstrapassets/manifest.json"), JSON.stringify([
    { source: "policies", target: "internal/bootstrapassets/files/policies", extension: ".yaml" },
  ]));
  return root;
}

test("check detects missing, changed, and retired shipped files without changing them", (t) => {
  const root = fixture(t);
  const source = path.join(root, "policies");
  const target = path.join(root, "internal/bootstrapassets/files/policies");
  writeFileSync(path.join(source, "new.yaml"), "new");
  writeFileSync(path.join(source, "changed.yaml"), "current");
  writeFileSync(path.join(source, "README.md"), "not an asset");
  writeFileSync(path.join(target, "changed.yaml"), "old");
  writeFileSync(path.join(target, "retired.yaml"), "retired");
  assert.throws(() => synchronizeBootstrapAssets(root, { check: true }), /assets are stale/);
  assert.equal(readFileSync(path.join(target, "changed.yaml"), "utf8"), "old");
  assert.deepEqual(readdirSync(target).sort(), ["changed.yaml", "retired.yaml"]);
  assert.equal(synchronizeBootstrapAssets(root), 3);
  assert.deepEqual(readdirSync(target).sort(), ["changed.yaml", "new.yaml"]);
  assert.equal(readFileSync(path.join(target, "changed.yaml"), "utf8"), "current");
  assert.equal(synchronizeBootstrapAssets(root, { check: true }), 0);
});

test("generation creates an absent target directory", (t) => {
  const root = fixture(t);
  const target = path.join(root, "internal/bootstrapassets/files/policies");
  rmSync(target, { recursive: true });
  writeFileSync(path.join(root, "policies", "new.yaml"), "new");
  assert.throws(() => synchronizeBootstrapAssets(root, { check: true }), /assets are stale/);
  assert.equal(synchronizeBootstrapAssets(root), 1);
  assert.equal(readFileSync(path.join(target, "new.yaml"), "utf8"), "new");
});
