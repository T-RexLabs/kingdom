import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";
import { ApiError } from "@/api/client";
import { ProjectInput } from "./project-input";
import { useProjectInputController } from "./use-project-input-controller";
import { SessionContext } from "./session-context";
const { centralFetch } = vi.hoisted(() => ({ centralFetch: vi.fn() }));
vi.mock("@/api/client", async () => ({ ...await vi.importActual<typeof import("@/api/client")>("@/api/client"), centralFetch }));
afterEach(() => centralFetch.mockReset());
function Input() { return <ProjectInput controller={useProjectInputController({ id: "project", display_name: "Project", role: "owner" }, true)} />; }
function view() { render(<QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}><SessionContext.Provider value={{ session_id: "session", csrf_token: "csrf", user: { id: "owner", display_name: "Owner", team_owner: true } }}><Input /></SessionContext.Provider></QueryClientProvider>); }
it("shows retained source type, file count, and repository commit without a global project selector", async () => {
  centralFetch.mockResolvedValue({ state: "imported", can_upload: false, source: { kind: "github", files: 3, repository: { commit_sha: "a".repeat(40) } } }); view();
  expect(await screen.findByText("GitHub Repository")).toBeInTheDocument();
  expect(screen.getByText("3 files were imported.")).toBeInTheDocument();
  expect(screen.getByText("a".repeat(40), { exact: false })).toBeInTheDocument();
  expect(screen.queryByLabelText("Project")).not.toBeInTheDocument();
  expect(screen.queryByLabelText("File")).not.toBeInTheDocument();
});
it("does not offer replacement after first startup", async () => {
  centralFetch.mockResolvedValue({ state: "started", can_upload: false }); view();
  expect(await screen.findByText("This project has started. Initial uploads are closed.")).toBeInTheDocument();
  expect(screen.queryByLabelText("File")).not.toBeInTheDocument();
});
it("retries the exact configured repository reference and key", async () => {
  let writes = 0; centralFetch.mockImplementation(async (path: string, options?: RequestInit) => {
    if (options?.method) { if (++writes === 1) throw new Error("lost reply"); return {}; }
    if (path.endsWith("/repository")) return { available: true };
    return { state: writes > 1 ? "imported" : "empty", can_upload: writes <= 1 };
  });
  const user = userEvent.setup(); view();
  await screen.findByRole("option", { name: "GitHub Repository" });
  await user.selectOptions(await screen.findByLabelText("Input Type"), "github");
  await user.clear(screen.getByLabelText("Branch, Tag, or Commit")); await user.type(screen.getByLabelText("Branch, Tag, or Commit"), "release");
  await user.click(screen.getByRole("button", { name: "Add Initial Files" })); await user.click(await screen.findByRole("button", { name: "Retry Same Input" }));
  await waitFor(() => expect(writes).toBe(2));
  const requests = centralFetch.mock.calls.filter(([, options]) => options?.method === "POST");
  expect(requests[0][0]).toBe("/projects/project/repository/import"); expect(requests[0][0]).toBe(requests[1][0]);
  expect(requests[0][1].body).toBe(requests[1][1].body); expect(requests[0][1].headers).toEqual(requests[1][1].headers);
  expect(JSON.parse(requests[0][1].body)).toEqual({ ref: "release" });
  await waitFor(() => expect(screen.queryByLabelText("Branch, Tag, or Commit")).not.toBeInTheDocument());
});
it("shows unavailable repository access while keeping file upload available", async () => {
  centralFetch.mockImplementation(async (path: string) => { if (path.endsWith("/repository")) throw new Error("offline"); return { state: "empty", can_upload: true }; }); view();
  expect(await screen.findByText("GitHub import access is unavailable. You can still upload a ZIP archive or one file.")).toBeInTheDocument();
  expect(screen.getByLabelText("File")).toBeInTheDocument();
  expect(screen.queryByRole("option", { name: "GitHub Repository" })).not.toBeInTheDocument();
});
it("allows edits after a known rejection instead of retaining an uncertain operation", async () => {
  centralFetch.mockImplementation(async (path: string, options?: RequestInit) => { if (options?.method) throw new ApiError(409, { message: "rejected", code: "unknown", retryable: false, category: "core", details: {}, correlationId: "fixture" }); return path.endsWith("/repository") ? { available: false } : { state: "empty", can_upload: true }; });
  const user = userEvent.setup(); view();
  await user.upload(await screen.findByLabelText("File"), new File(["fixture"], "input.zip", { type: "application/zip" }));
  await user.click(screen.getByRole("button", { name: "Add Initial Files" }));
  expect(await screen.findByText(/The input was rejected/)).toBeInTheDocument();
  await waitFor(() => expect(screen.getByLabelText("Input Type")).not.toBeDisabled());
  expect(screen.queryByRole("button", { name: "Retry Same Input" })).not.toBeInTheDocument();
});
