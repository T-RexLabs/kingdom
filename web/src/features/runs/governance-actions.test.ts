import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";
import { describe, expect, it } from "vitest";

const rootDir = resolve(dirname(fileURLToPath(import.meta.url)), "../../");

describe("governance resume/rerun request contract", () => {
  it("sends non-empty reason payloads for resume and rerun", () => {
    const hooksContent = readFileSync(resolve(rootDir, "api/hooks-runs.ts"), "utf8");
    const actionsContent = readFileSync(resolve(rootDir, "features/runs/governance-actions.tsx"), "utf8");
    expect(actionsContent).toContain("manual resume from run detail");
    expect(hooksContent).toContain("manual rerun from run detail");
    expect(hooksContent).not.toContain("/retry`");
  });
});
