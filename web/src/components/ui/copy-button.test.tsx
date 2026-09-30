import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CopyButton } from "./copy-button";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("CopyButton", () => {
  it("confirms a copy, announces it, and reverts", async () => {
    // userEvent.setup() installs the clipboard stub this writes through.
    const user = userEvent.setup();
    render(<CopyButton value="ABCD-EFGH" label="Copy GitHub code" />);

    expect(screen.queryByText("Copied to Clipboard")).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Copy GitHub code" }));

    const confirmation = await screen.findByText("Copied to Clipboard");
    expect(confirmation).toHaveAttribute("aria-live", "polite");
    expect(await navigator.clipboard.readText()).toBe("ABCD-EFGH");
    await waitFor(() => expect(screen.queryByText("Copied to Clipboard")).not.toBeInTheDocument(), { timeout: 4000 });
  });

  it("reports a rejected copy instead of failing silently", async () => {
    const user = userEvent.setup();
    vi.spyOn(navigator.clipboard, "writeText").mockRejectedValue(new Error("denied"));
    render(<CopyButton value="ABCD-EFGH" label="Copy GitHub code" />);

    await user.click(screen.getByRole("button", { name: "Copy GitHub code" }));

    // An insecure context or denied permission must not look like success.
    expect(await screen.findByText(/copy failed/i)).toBeInTheDocument();
    expect(screen.queryByText("Copied to Clipboard")).not.toBeInTheDocument();
  });
});
