import { afterEach, describe, expect, it, vi } from "vitest";
import {
  openSetupWindow,
  postManifestToSetupWindow,
} from "./github-setup-window";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("GitHub setup window", () => {
  it("opens external GitHub hops with noopener", () => {
    const open = vi.spyOn(window, "open").mockReturnValue(null);

    expect(openSetupWindow("https://github.com/apps/kingdom/installations/new")).toBe(true);
    expect(open).toHaveBeenCalledWith(
      "https://github.com/apps/kingdom/installations/new",
      "kingdom-github-setup",
      "noopener",
    );
  });

  it("keeps the initial named tab available for the deferred manifest POST", () => {
    const target = { focus: vi.fn(), opener: window } as unknown as Window;
    const open = vi.spyOn(window, "open").mockReturnValue(target);
    let submittedForm: HTMLFormElement | undefined;
    const submit = vi
      .spyOn(HTMLFormElement.prototype, "submit")
      .mockImplementation(() => {
        submittedForm = document.querySelector("form") ?? undefined;
      });

    expect(openSetupWindow()).toBe(true);
    expect(open).toHaveBeenCalledWith("", "kingdom-github-setup");
    expect(target.opener).toBeNull();
    expect(target.focus).toHaveBeenCalledOnce();

    postManifestToSetupWindow("https://github.com/settings/apps/new", '{"name":"Kingdom"}');

    expect(submit).toHaveBeenCalledOnce();
    expect(submittedForm).toHaveAttribute("method", "post");
    expect(submittedForm).toHaveAttribute("action", "https://github.com/settings/apps/new");
    expect(submittedForm).toHaveAttribute("target", "kingdom-github-setup");
    expect(submittedForm?.elements.namedItem("manifest")).toHaveValue('{"name":"Kingdom"}');
    expect(document.querySelector("form")).toBeNull();
  });
});
