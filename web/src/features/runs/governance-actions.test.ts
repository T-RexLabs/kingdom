import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";
import { describe, expect, it } from "vitest";

const rootDir = resolve(dirname(fileURLToPath(import.meta.url)), "../../");

describe("governance retry/rerun request contract", () => {
  it("sends non-empty reason payloads for retry and rerun", () => {
    const hooksContent = readFileSync(resolve(rootDir, "api/hooks-runs.ts"), "utf8");
    expect(hooksContent).toContain("manual retry from run detail");
    expect(hooksContent).toContain("manual rerun from run detail");
  });
});
