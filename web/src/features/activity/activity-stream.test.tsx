import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { ActivityChecklist, ActivityLiveStream } from "./activity-stream";
import { emptyActivityState, type TodoList } from "./activity-view";
import { builtInThemes } from "@/theme/contract";

vi.mock("lucide-react/dynamic", async () => {
  const actual = await vi.importActual<typeof import("lucide-react/dynamic")>("lucide-react/dynamic");
  return { ...actual, DynamicIcon: ({ name }: { name: string }) => <svg data-icon={name} /> };
});

const appearance = vi.hoisted(() => ({ index: 1 }));
vi.mock("@/theme/context", async () => {
  const actual = await vi.importActual<typeof import("@/theme/context")>("@/theme/context");
  const { builtInThemes } = await import("@/theme/contract");
  return { ...actual, useTheme: () => ({ theme: builtInThemes[appearance.index], assetURLs: {} }) };
});

it("switches the live planner icon with the selected theme", async () => {
  const state = { ...emptyActivityState(), currentEvent: { id: "read", label: "Reading project context" } };
  const view = render(<ActivityLiveStream state={state} isStreaming />);
  for (const index of [1, 3]) {
    appearance.index = index;
    view.rerender(<ActivityLiveStream state={state} isStreaming />);
    const symbol = builtInThemes[index].icons["role.planning-assistant"].slice("lucide:".length);
    await waitFor(() => expect(view.container.querySelector(`[data-icon="${symbol}"]`)).toBeInTheDocument());
    expect(screen.getByText("Reading project context")).toBeInTheDocument();
  }
  view.rerender(<ActivityLiveStream state={state} isStreaming={false} />);
  expect(view.container).toBeEmptyDOMElement();
});

const list: TodoList = {
  items: [
    { content: "Confirm module root", status: "completed" },
    { content: "Add greeter.go", status: "in_progress" },
    { content: "Run go test", status: "pending" },
  ],
  done: 1,
  total: 3,
};

describe("ActivityChecklist", () => {
  it("shows progress and reveals items with strikethrough on completion when expanded", async () => {
    const user = userEvent.setup();
    render(<ActivityChecklist list={list} />);

    // Collapsed by default: progress count shown, items hidden.
    const toggle = screen.getByRole("button", { name: /Checklist/ });
    expect(toggle).toHaveAttribute("aria-expanded", "false");
    expect(screen.getByText("1/3")).toBeInTheDocument();
    expect(screen.queryByText("Add greeter.go")).not.toBeInTheDocument();

    await user.click(toggle);
    expect(screen.getByText("Add greeter.go")).toBeInTheDocument();
    const completed = screen.getByText("Confirm module root");
    expect(completed.className).toContain("line-through");
  });
});
