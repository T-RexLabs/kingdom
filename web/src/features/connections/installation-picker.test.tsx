import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { InstallationPicker } from "./installation-picker";

const candidate = {
  installation_id: "10",
  display_name: "octocat",
  account_id: "100",
  account_login: "octocat",
  app_id: "1",
  client_id: "Iv1",
  app_slug: "kingdom",
};

describe("InstallationPicker", () => {
  it("keeps management separate from selection disabling", () => {
    render(
      <InstallationPicker
        name="installation"
        candidates={[candidate]}
        selected={new Set(["10"])}
        disabled
        onToggle={vi.fn()}
      />,
    );

    expect(screen.getByRole("checkbox", { name: /@octocat/ })).toBeDisabled();
    expect(screen.getByRole("link", { name: "Manage Repositories on GitHub" })).toBeInTheDocument();
  });

  it("removes management links from focus and activation when requested", () => {
    render(
      <InstallationPicker
        name="installation"
        candidates={[candidate]}
        selected={new Set(["10"])}
        disabled
        managementLinksDisabled
        onToggle={vi.fn()}
      />,
    );

    expect(screen.queryByRole("link", { name: "Manage Repositories on GitHub" })).not.toBeInTheDocument();
    expect(screen.getByText("Manage Repositories on GitHub")).toHaveAttribute("aria-disabled", "true");
  });
});
