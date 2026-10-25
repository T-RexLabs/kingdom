import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";
import { ProjectLauncher } from "./project-launcher";
import { SessionContext, type CentralSession } from "./session-context";
import type { CentralProject } from "./access-client";

const { centralFetch } = vi.hoisted(() => ({ centralFetch: vi.fn() }));
vi.mock("@/api/client", async () => ({ ...await vi.importActual<typeof import("@/api/client")>("@/api/client"), centralFetch }));
afterEach(() => centralFetch.mockReset());
const owner: CentralSession = { session_id: "session", csrf_token: "csrf", user: { id: "owner", display_name: "Owner", team_owner: true } };
const project: CentralProject = { id: "project", display_name: "Chosen Project", role: "owner" };
function view(initialSession = owner, initialProject = project) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const navigate = vi.fn();
  const element = (session: CentralSession, item: CentralProject) => <QueryClientProvider client={client}><SessionContext.Provider value={session}><ProjectLauncher project={item} onNavigate={navigate} /></SessionContext.Provider></QueryClientProvider>;
  const result = render(element(initialSession, initialProject), { container: document.getElementById("scene-root") as HTMLElement });
  return { navigate, rerender: (session: CentralSession, item = initialProject) => result.rerender(element(session, item)) };
}
function fixture(status: object) { centralFetch.mockImplementation(async (path: string) => path.endsWith("/repository") ? { available: true } : status); }

it("does not fetch closed cards and requires confirmation before starting empty", async () => {
  fixture({ state: "empty", can_upload: true }); const user = userEvent.setup(); const { navigate } = view();
  expect(centralFetch).not.toHaveBeenCalled();
  await user.click(screen.getByRole("button", { name: "Open Project" }));
  expect(await screen.findByRole("dialog", { name: "Set Up Project" })).toBeInTheDocument();
  expect(navigate).not.toHaveBeenCalled();
  await user.click(screen.getByRole("button", { name: "Start Empty Project" }));
  const confirmation = screen.getByRole("dialog", { name: "Start Empty Project" });
  expect(within(confirmation).getByText(/closes initial uploads/i)).toBeInTheDocument();
  expect(navigate).not.toHaveBeenCalled();
  await user.click(within(confirmation).getByRole("button", { name: "Start Empty Project" }));
  await waitFor(() => expect(navigate).toHaveBeenCalledWith("/projects/project/"));
});
for (const state of ["started", "imported"]) it(`opens a confirmed ${state} project normally`, async () => {
  fixture({ state, can_upload: false }); const { navigate } = view();
  await userEvent.click(screen.getByRole("button", { name: "Open Project" }));
  await waitFor(() => expect(navigate).toHaveBeenCalledWith("/projects/project/"));
  expect(centralFetch.mock.calls.some(([path]) => path.endsWith("/repository"))).toBe(false);
});
it("does not navigate after failed status or use cached input as a fallback", async () => {
  centralFetch.mockRejectedValue(new Error("offline")); const { navigate } = view();
  await userEvent.click(screen.getByRole("button", { name: "Open Project" }));
  expect(await screen.findByText("Project input is unavailable. Check it again before opening the project.")).toBeInTheDocument();
  expect(navigate).not.toHaveBeenCalled();
  expect(screen.queryByRole("button", { name: "Start Empty Project" })).not.toBeInTheDocument();
});
it("does not open from an old successful source response when a new check fails", async () => {
  fixture({ state: "imported", can_upload: false }); const { navigate } = view(); const user = userEvent.setup();
  await user.click(screen.getByRole("button", { name: "Open Project" }));
  await waitFor(() => expect(navigate).toHaveBeenCalledTimes(1));
  centralFetch.mockRejectedValue(new Error("offline"));
  await user.click(screen.getByRole("button", { name: "Open Project" }));
  expect(await screen.findByText("Project input is unavailable. Check it again before opening the project.")).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Open Project" })).toBeDisabled();
  expect(navigate).toHaveBeenCalledTimes(1);
});
it("does not navigate if the user closes while status is loading", async () => {
  let release!: (status: object) => void;
  centralFetch.mockImplementation(() => new Promise(resolve => { release = resolve; }));
  const { navigate } = view(); const user = userEvent.setup();
  await user.click(screen.getByRole("button", { name: "Open Project" }));
  await user.click(screen.getByRole("button", { name: "Close" }));
  release({ state: "started", can_upload: false });
  await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
  expect(navigate).not.toHaveBeenCalled();
});
it("asks the project owner to configure empty projects for members", async () => {
  fixture({ state: "empty", can_upload: true }); const { navigate } = view(owner, { ...project, role: "member" });
  await userEvent.click(screen.getByRole("button", { name: "Open Project" }));
  expect(await screen.findByText("Ask a project owner to add initial files or start this project empty.")).toBeInTheDocument();
  expect(screen.queryByLabelText("File")).not.toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "Start Empty Project" })).not.toBeInTheDocument();
  expect(navigate).not.toHaveBeenCalled();
});
it("uploads then offers opening only after the source is confirmed", async () => {
  let uploaded = false; let confirm = false;
  centralFetch.mockImplementation(async (path: string, options?: RequestInit) => {
    if (options?.method === "POST") { uploaded = true; return {}; }
    if (path.endsWith("/repository")) return { available: false };
    if (uploaded && !confirm) throw new Error("status lost");
    return { state: confirm ? "imported" : "empty", can_upload: !confirm };
  });
  const user = userEvent.setup(); const { navigate } = view();
  await user.click(screen.getByRole("button", { name: "Open Project" }));
  await user.upload(await screen.findByLabelText("File"), new File(["fixture"], "input.zip", { type: "application/zip" }));
  await user.click(screen.getByRole("button", { name: "Add Initial Files" }));
  expect(await screen.findByText("Project input is unavailable. Check it again before opening the project.")).toBeInTheDocument();
  expect(navigate).not.toHaveBeenCalled(); confirm = true;
  await user.click(screen.getByRole("button", { name: "Check Project Input" }));
  await user.click(await screen.findByRole("button", { name: "Open Project" }));
  await waitFor(() => expect(navigate).toHaveBeenCalledTimes(1));
});
it("retains exact upload body and UUID after lost reply and dialog closure", async () => {
  let writes = 0;
  centralFetch.mockImplementation(async (path: string, options?: RequestInit) => {
    if (options?.method === "POST") { if (++writes === 1) throw new Error("lost reply"); return {}; }
    if (path.endsWith("/repository")) return { available: true };
    return { state: writes > 1 ? "imported" : "empty", can_upload: writes <= 1 };
  });
  const user = userEvent.setup(); view();
  await user.click(screen.getByRole("button", { name: "Open Project" }));
  const file = new File(["fixture"], "input.zip", { type: "application/zip" });
  await user.upload(await screen.findByLabelText("File"), file);
  await user.click(screen.getByRole("button", { name: "Add Initial Files" }));
  expect(await screen.findByRole("button", { name: "Retry Same Input" })).toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "Close" }));
  await user.click(screen.getByRole("button", { name: "Open Project" }));
  await user.click(await screen.findByRole("button", { name: "Retry Same Input" }));
  await waitFor(() => expect(writes).toBe(2));
  const requests = centralFetch.mock.calls.filter(([, options]) => options?.method === "POST");
  expect(requests[0][0]).toBe(requests[1][0]); expect(requests[0][1].body).toBe(file); expect(requests[1][1].body).toBe(file);
  expect(requests[0][1].headers).toEqual(requests[1][1].headers);
});
it("does not restore another actor or session's project state", async () => {
  fixture({ state: "empty", can_upload: true }); const user = userEvent.setup(); const result = view();
  await user.click(screen.getByRole("button", { name: "Open Project" }));
  await user.upload(await screen.findByLabelText("File"), new File(["private"], "private.zip", { type: "application/zip" }));
  await user.click(screen.getByRole("button", { name: "Close" }));
  centralFetch.mockClear();
  result.rerender({ ...owner, session_id: "replacement", user: { ...owner.user, id: "other" } });
  expect(centralFetch).not.toHaveBeenCalled();
  await user.click(screen.getByRole("button", { name: "Open Project" }));
  expect(await screen.findByLabelText("File")).toBeInTheDocument();
  expect(screen.queryByText("private.zip")).not.toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Add Initial Files" })).toBeDisabled();
  expect(centralFetch.mock.calls.some(([path]) => path === "/projects/project/source")).toBe(true);
});

it("keeps opening blocked until an uncertain upload is confirmed by its exact retry", async () => {
  let writes = 0;
  centralFetch.mockImplementation(async (path: string, options?: RequestInit) => {
    if (options?.method === "POST") { if (++writes === 1) throw new Error("lost reply"); return {}; }
    if (path.endsWith("/repository")) return { available: false };
    return { state: writes ? "imported" : "empty", can_upload: !writes };
  });
  const user = userEvent.setup(); const { navigate } = view();
  await user.click(screen.getByRole("button", { name: "Open Project" }));
  await user.upload(await screen.findByLabelText("File"), new File(["fixture"], "input.zip", { type: "application/zip" }));
  await user.click(screen.getByRole("button", { name: "Add Initial Files" }));
  await screen.findByRole("button", { name: "Retry Same Input" });
  await user.click(screen.getByRole("button", { name: "Close" }));
  await user.click(screen.getByRole("button", { name: "Open Project" }));
  await screen.findByText("Initial files are ready. Open the project to start work.");
  expect(screen.getByRole("button", { name: "Open Project" })).toBeDisabled();
  expect(navigate).not.toHaveBeenCalled();
  await user.click(screen.getByRole("button", { name: "Retry Same Input" }));
  await waitFor(() => expect(screen.getByRole("button", { name: "Open Project" })).not.toBeDisabled());
  await user.click(screen.getByRole("button", { name: "Open Project" }));
  await waitFor(() => expect(navigate).toHaveBeenCalledTimes(1));
  const requests = centralFetch.mock.calls.filter(([, options]) => options?.method === "POST");
  expect(requests[0][1].headers).toEqual(requests[1][1].headers);
});
it("does not fetch source again when an upload completes after the dialog closes", async () => {
  let uploaded = false; let finish!: () => void;
  centralFetch.mockImplementation(async (path: string, options?: RequestInit) => {
    if (options?.method === "POST") { await new Promise<void>(resolve => { finish = resolve; }); uploaded = true; return {}; }
    if (path.endsWith("/repository")) return { available: false };
    return { state: uploaded ? "imported" : "empty", can_upload: !uploaded };
  });
  const user = userEvent.setup(); const { navigate } = view();
  await user.click(screen.getByRole("button", { name: "Open Project" }));
  await user.upload(await screen.findByLabelText("File"), new File(["fixture"], "input.zip", { type: "application/zip" }));
  await user.click(screen.getByRole("button", { name: "Add Initial Files" }));
  expect(screen.getByRole("button", { name: "Start Empty Project" })).toBeDisabled();
  await user.click(screen.getByRole("button", { name: "Close" }));
  const reads = centralFetch.mock.calls.filter(([path]) => path.endsWith("/source")).length; finish();
  await waitFor(() => expect(uploaded).toBe(true));
  expect(centralFetch.mock.calls.filter(([path]) => path.endsWith("/source"))).toHaveLength(reads);
  expect(navigate).not.toHaveBeenCalled();
  await user.click(screen.getByRole("button", { name: "Open Project" }));
  await waitFor(() => expect(navigate).toHaveBeenCalledTimes(1));
});
it("rechecks owner input authority before accepting an empty start", async () => {
  let allowed = true; centralFetch.mockImplementation(async (path: string) => path.endsWith("/repository") ? { available: false } : { state: "empty", can_upload: allowed });
  const user = userEvent.setup(); const { navigate } = view();
  await user.click(screen.getByRole("button", { name: "Open Project" }));
  await user.click(await screen.findByRole("button", { name: "Start Empty Project" }));
  allowed = false; await user.click(screen.getByRole("button", { name: "Start Empty Project" }));
  expect(await screen.findByText("Ask a project owner to add initial files or start this project empty.")).toBeInTheDocument();
  expect(navigate).not.toHaveBeenCalled();
});
it("ignores a late source response after the active session changes", async () => {
  let finish!: (value: object) => void;
  centralFetch.mockImplementation(() => new Promise(resolve => { finish = resolve; }));
  const user = userEvent.setup(); const result = view();
  await user.click(screen.getByRole("button", { name: "Open Project" }));
  result.rerender({ ...owner, session_id: "new-session" });
  finish({ state: "started", can_upload: false });
  await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
  expect(result.navigate).not.toHaveBeenCalled();
});
