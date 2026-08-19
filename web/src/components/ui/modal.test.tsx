import { render, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { OVERLAY_ROOT_ID, SCENE_ROOT_ID } from "@/lib/overlay-system";
import { ConfirmDialog, Modal, ModalFooter } from "./modal-shell";

function renderInScene(ui: React.ReactElement) {
  return render(ui, { container: document.getElementById(SCENE_ROOT_ID) as HTMLElement });
}

function overlay() {
  return document.getElementById(OVERLAY_ROOT_ID) as HTMLElement;
}

describe("Modal", () => {
  // Call sites used to pass a hand-written `ariaLabel` alongside a separate
  // visible title, so screen readers announced a name the operator could not
  // see anywhere on screen.
  it("takes its accessible name from the heading the operator can see", () => {
    renderInScene(
      <Modal open onClose={vi.fn()} caption="Remove Connection" title="octocat-issues">
        <p>Body</p>
      </Modal>,
    );

    const dialog = within(overlay()).getByRole("dialog", { name: "octocat-issues" });
    expect(within(dialog).getByRole("heading", { name: "octocat-issues" })).toBeInTheDocument();
  });

  it("renders the requested size without the call site deriving panel classes", () => {
    renderInScene(<Modal open onClose={vi.fn()} title="Wide" size="xl">body</Modal>);
    expect(within(overlay()).getByRole("dialog")).toHaveClass("max-w-5xl");
  });

  it("closes on Escape", async () => {
    const onClose = vi.fn();
    const user = userEvent.setup();
    renderInScene(<Modal open onClose={onClose} title="Escapable">body</Modal>);

    await user.keyboard("{Escape}");
    expect(onClose).toHaveBeenCalled();
  });
});

describe("ConfirmDialog", () => {
  it("puts the dismiss before the confirm and defaults to a destructive confirm", () => {
    renderInScene(
      <ConfirmDialog
        open
        onClose={vi.fn()}
        onConfirm={vi.fn()}
        title="Session two"
        caption="Delete Session"
        confirmLabel="Delete Session"
      />,
    );

    const dialog = within(overlay()).getByRole("dialog", { name: "Session two" });
    const actions = within(dialog).getAllByRole("button").filter((button) => button.textContent !== "");
    expect(actions.map((button) => button.textContent)).toEqual(["Cancel", "Delete Session"]);
  });

  it("only confirms when the confirm button is pressed", async () => {
    const onConfirm = vi.fn();
    const onClose = vi.fn();
    const user = userEvent.setup();
    renderInScene(
      <ConfirmDialog open onClose={onClose} onConfirm={onConfirm} title="Careful" confirmLabel="Remove Connection" />,
    );

    await user.click(within(overlay()).getByRole("button", { name: "Cancel" }));
    expect(onConfirm).not.toHaveBeenCalled();
    expect(onClose).toHaveBeenCalledTimes(1);

    await user.click(within(overlay()).getByRole("button", { name: "Remove Connection" }));
    expect(onConfirm).toHaveBeenCalledTimes(1);
  });

  it("can disable each action independently while a mutation settles", () => {
    renderInScene(
      <ConfirmDialog
        open
        onClose={vi.fn()}
        onConfirm={vi.fn()}
        title="Busy"
        confirmLabel="Removing..."
        confirmDisabled
      />,
    );

    expect(within(overlay()).getByRole("button", { name: "Removing..." })).toBeDisabled();
    expect(within(overlay()).getByRole("button", { name: "Cancel" })).toBeEnabled();
  });
});

describe("ModalFooter", () => {
  it("is the one action row every modal shares", () => {
    const { container } = render(<ModalFooter><button type="button">Go</button></ModalFooter>);
    expect(container.firstElementChild).toHaveClass("flex", "flex-wrap", "items-center", "justify-end", "gap-2");
  });
});
