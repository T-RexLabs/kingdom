import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readFileSync, readdirSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { synchronizeBootstrapAssets } from "../generate_bootstrap_assets.mjs";

function fixture(t) {
  const root = mkdtempSync(path.join(os.tmpdir(), "kingdom-assets-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  for (const directory of ["policies", "contracts/schemas", "internal/bootstrapassets/files/policies", "internal/bootstrapassets/files/schemas"]) mkdirSync(path.join(root, directory), { recursive: true });
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

test("both canonical policy and schema groups are required", (t) => {
  const root = fixture(t);
  rmSync(path.join(root, "contracts/schemas"), { recursive: true });
  assert.throws(() => synchronizeBootstrapAssets(root, { check: true }), { code: "ENOENT" });
  writeFileSync(path.join(root, "policies/new.yaml"), "new");
  assert.throws(() => synchronizeBootstrapAssets(root), { code: "ENOENT" });
  assert.deepEqual(readdirSync(path.join(root, "internal/bootstrapassets/files/policies")), []);
});

test("source and destination directory symlinks cannot change files outside managed directories", (t) => {
  for (const relative of ["policies", "contracts", "internal/bootstrapassets", "internal/bootstrapassets/files/policies"]) {
    const root = fixture(t);
    const outside = mkdtempSync(path.join(os.tmpdir(), "kingdom-assets-retained-"));
    t.after(() => rmSync(outside, { recursive: true, force: true }));
    writeFileSync(path.join(outside, "retained.yaml"), "retained");
    const linked = path.join(root, relative);
    rmSync(linked, { recursive: true });
    symlinkSync(outside, linked, "dir");
    assert.throws(() => synchronizeBootstrapAssets(root), /must be a real directory/);
    assert.deepEqual(readdirSync(outside), ["retained.yaml"]);
    assert.equal(readFileSync(path.join(outside, "retained.yaml"), "utf8"), "retained");
  }
});
