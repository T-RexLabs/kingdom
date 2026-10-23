import { describe, expect, it } from "vitest";
import { appBase, runtimeMode } from "./runtime-mode";
function page(mode?: string, base?: string) {
  const root = document.implementation.createHTMLDocument();
  if (mode) { const tag = root.createElement("meta"); tag.name = "kingdom-runtime-mode"; tag.content = mode; root.head.append(tag); }
  if (base) { const tag = root.createElement("meta"); tag.name = "kingdom-app-base"; tag.content = base; root.head.append(tag); }
  return root;
}
describe("runtime routing", () => {
  it("keeps local defaults without central metadata", () => { expect(runtimeMode(page())).toBe("local"); expect(appBase(page())).toBe("/"); });
  it("uses only a server-selected exact project prefix", () => {
    const base = "/projects/9d38bdaa-34be-4d48-9b1c-a75ecc927847";
    expect(appBase(page("hosted-project", base))).toBe(base);
    for (const value of ["/", "//foreign.example", `${base}/../other`, `${base}?project=other`]) expect(() => appBase(page("hosted-project", value))).toThrow();
  });
  it("does not inherit a project prefix in the portal", () => { expect(appBase(page("central", "/projects/foreign"))).toBe("/"); });
});
