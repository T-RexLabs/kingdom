import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";
import { ApiError } from "@/api/client";
import { PersonalAccounts } from "./personal-accounts";

const { centralFetch } = vi.hoisted(() => ({ centralFetch: vi.fn() }));
vi.mock("@/api/client", async () => ({ ...await vi.importActual<typeof import("@/api/client")>("@/api/client"), centralFetch }));
afterEach(() => { centralFetch.mockReset(); sessionStorage.clear(); });
const attempt = { id: "attempt", provider: "github-copilot", status: "pending", url: "https://github.com/login/device", instructions: "Enter code: ABCD-1234", expires_at: new Date(Date.now() + 600_000).toISOString() };
function view(onConnected = vi.fn()) {
 const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
 return render(<QueryClientProvider client={client}><PersonalAccounts ownerID="owner" onConnected={onConnected} /></QueryClientProvider>);
}
it("starts a personal sign-in, opens only the safe device link, and observes the same attempt", async () => {
 const user = userEvent.setup(); const complete = vi.fn(); let checks = 0;
 centralFetch.mockImplementation(async (path: string, options?: RequestInit) => {
  if (path.endsWith("/methods")) return { providers: ["github-copilot"] };
  if (options?.method === "POST") return attempt;
  return ++checks === 1 ? { ...attempt, status: "complete", connection: { id: "connection" } } : attempt;
 });
 view(complete);
 await user.type(await screen.findByLabelText("Account Connection Name"), "My Copilot");
 await user.click(screen.getByRole("button", { name: "Sign In with GitHub Copilot" }));
 const link = await screen.findByRole("link", { name: "Open GitHub Sign-In" });
 expect(link).toHaveAttribute("href", "https://github.com/login/device");
 expect(link).toHaveAttribute("rel", "noopener noreferrer");
 expect(screen.getByText("Enter code: ABCD-1234")).toBeInTheDocument();
 expect(sessionStorage.getItem("kingdom.personal-sign-in:owner")).toBe("attempt");
 await user.click(screen.getByRole("button", { name: "Check Sign-In" }));
 await waitFor(() => expect(complete).toHaveBeenCalledOnce());
 expect(sessionStorage.getItem("kingdom.personal-sign-in:owner")).toBeNull();
 const writes = centralFetch.mock.calls.filter(call => call[1]?.method === "POST");
 expect(writes).toHaveLength(1);
 expect(JSON.parse(writes[0][1].body)).toEqual({ provider: "github-copilot", label: "My Copilot" });
 expect(writes[0][1].headers["Idempotency-Key"]).toBeTruthy();
 expect(JSON.stringify(writes[0])).not.toContain("scope");
});
it("recovers a retained attempt with GET and retries a failed status read without starting another sign-in", async () => {
 const user = userEvent.setup(); sessionStorage.setItem("kingdom.personal-sign-in:owner", "attempt"); let checks = 0;
 centralFetch.mockImplementation(async (path: string) => {
  if (path.endsWith("/methods")) return { providers: ["github-copilot"] };
  if (++checks === 1) throw new Error("offline");
  return attempt;
 });
 view();
 await user.click(await screen.findByRole("button", { name: "Retry Sign-In Status" }));
 await screen.findByText("Enter code: ABCD-1234");
 expect(centralFetch.mock.calls.filter(call => call[1]?.method === "POST")).toHaveLength(0);
 expect(centralFetch.mock.calls.filter(call => call[0] === "/personal-accounts/sign-ins/attempt")).toHaveLength(2);
});
it("never renders an arbitrary authorization URL", async () => {
 const user = userEvent.setup();
 centralFetch.mockImplementation(async (path: string) => path.endsWith("/methods") ? { providers: ["github-copilot"] } : { ...attempt, url: "https://attacker.example/consent" });
 view(); await user.type(await screen.findByLabelText("Account Connection Name"), "Mine");
 await user.click(screen.getByRole("button", { name: "Sign In with GitHub Copilot" }));
 expect(await screen.findByRole("alert")).toHaveTextContent("sign-in link is unavailable");
 expect(screen.queryByRole("link")).not.toBeInTheDocument();
});
it("does not poll an expired attempt and cancels the retained identity with a mutation key", async () => {
 const user = userEvent.setup();
 centralFetch.mockImplementation(async (path: string, options?: RequestInit) => {
  if (path.endsWith("/methods")) return { providers: ["github-copilot"] };
  if (options?.method === "DELETE") return { ...attempt, status: "canceled" };
  return { ...attempt, expires_at: new Date(Date.now() - 1000).toISOString() };
 });
 view(); await user.type(await screen.findByLabelText("Account Connection Name"), "Mine");
 await user.click(screen.getByRole("button", { name: "Sign In with GitHub Copilot" }));
 expect(await screen.findByText(/This sign-in has expired/)).toBeInTheDocument();
 await user.click(screen.getByRole("button", { name: "Cancel Sign-In" }));
 await waitFor(() => expect(centralFetch.mock.calls.some(call => call[1]?.method === "DELETE")).toBe(true));
 const request = centralFetch.mock.calls.find(call => call[1]?.method === "DELETE");
 expect(request?.[0]).toBe("/personal-accounts/sign-ins/attempt");
 expect(request?.[1].headers["Idempotency-Key"]).toBeTruthy();
});
it("keeps the ChatGPT pairing code only in memory across polls and never embeds it in the helper command", async () => {
 const user = userEvent.setup(); const storage = vi.spyOn(Storage.prototype, "setItem");
 const chatgpt = { ...attempt, provider: "openai", url: undefined, pairing_code: "private-pairing-code" };
 centralFetch.mockImplementation(async (path: string, options?: RequestInit) => {
  if (path.endsWith("/methods")) return { providers: ["openai"] };
  return options?.method === "POST" ? chatgpt : { ...chatgpt, pairing_code: undefined };
 });
 view(); await user.type(await screen.findByLabelText("Account Connection Name"), "My ChatGPT");
 await user.click(screen.getByRole("button", { name: "Connect ChatGPT" }));
 expect(await screen.findByText("private-pairing-code")).toBeInTheDocument();
 const command = screen.getByText(/kd chatgpt attach --service/);
 expect(command).toHaveTextContent(`kd chatgpt attach --service ${window.location.origin}`);
 expect(command).not.toHaveTextContent("private-pairing-code");
 expect(screen.queryByRole("link")).not.toBeInTheDocument();
 await user.click(screen.getByRole("button", { name: "Check Sign-In" }));
 expect(screen.getByText("private-pairing-code")).toBeInTheDocument();
 const start = centralFetch.mock.calls.find(call => call[1]?.method === "POST");
 expect(JSON.parse(start?.[1].body)).toEqual({ provider: "openai", label: "My ChatGPT" });
 expect(storage.mock.calls.every(call => !JSON.stringify(call).includes("private-pairing-code"))).toBe(true);
 storage.mockRestore();
});
it("recovers ChatGPT status after reload without restoring or requesting the one-time pairing code", async () => {
 const user = userEvent.setup(); sessionStorage.setItem("kingdom.personal-sign-in:owner", "attempt");
 centralFetch.mockImplementation(async (path: string) => path.endsWith("/methods") ? { providers: ["openai"] } : { ...attempt, provider: "openai", url: undefined });
 view();
 expect(await screen.findByText(/pairing code is no longer available/)).toBeInTheDocument();
 expect(screen.queryByRole("button", { name: "Copy Pairing Code" })).not.toBeInTheDocument();
 expect(centralFetch.mock.calls.some(call => call[1]?.method === "POST")).toBe(false);
 await user.click(screen.getByRole("button", { name: "Cancel Sign-In" }));
 await user.click(await screen.findByRole("button", { name: "Start New Sign-In" }));
 expect(await screen.findByRole("button", { name: "Connect ChatGPT" })).toBeInTheDocument();
});
it("keeps the exact start request and key after a lost initial response", async () => {
 const user = userEvent.setup(); let starts = 0;
 centralFetch.mockImplementation(async (path: string, options?: RequestInit) => {
  if (path.endsWith("/methods")) return { providers: ["github-copilot"] };
  if (options?.method === "POST" && ++starts === 1) throw new Error("lost response");
  return attempt;
 });
 view(); await user.type(await screen.findByLabelText("Account Connection Name"), "My Copilot");
 await user.click(screen.getByRole("button", { name: "Sign In with GitHub Copilot" }));
 expect(await screen.findByRole("alert")).toHaveFocus();
 expect(screen.getByLabelText("Account Connection Name")).toBeDisabled();
 await user.click(screen.getByRole("button", { name: "Retry Same Sign-In" }));
 await screen.findByRole("link", { name: "Open GitHub Sign-In" });
 const calls = centralFetch.mock.calls.filter(call => call[1]?.method === "POST");
 expect(calls).toHaveLength(2);
 expect(calls[0][1].body).toEqual(calls[1][1].body);
 expect(calls[0][1].headers).toEqual(calls[1][1].headers);
});

it("permits a new sign-in when a saved attempt belongs to an ended session", async () => {
 const user = userEvent.setup(); sessionStorage.setItem("kingdom.personal-sign-in:owner", "attempt");
 centralFetch.mockImplementation(async (path: string) => {
  if (path.endsWith("/methods")) return { providers: ["github-copilot"] };
  throw new ApiError(403, { message: "denied", code: "unknown", retryable: false, category: "core", details: {}, correlationId: "denied" });
 });
 view(); await screen.findByText(/no longer available in your session/);
 await user.click(screen.getByRole("button", { name: "Start New Sign-In" }));
 expect(await screen.findByLabelText("Account Connection Name")).toBeEnabled();
 expect(sessionStorage.getItem("kingdom.personal-sign-in:owner")).toBeNull();
 expect(centralFetch.mock.calls.some(call => call[1]?.method === "POST")).toBe(false);
});

it("continues status polling through publication and cleanup without showing stale OAuth instructions", async () => {
 const user = userEvent.setup(); let checks = 0;
 centralFetch.mockImplementation(async (path: string, options?: RequestInit) => {
  if (path.endsWith("/methods")) return { providers: ["github-copilot"] };
  if (options?.method === "POST") return { ...attempt, status: "publishing" };
  return ++checks === 1 ? { ...attempt, status: "cleanup" } : { ...attempt, status: "complete", connection: { id: "connection" } };
 });
 const connected=vi.fn(); view(connected);
 await user.type(await screen.findByLabelText("Account Connection Name"), "Mine");
 await user.click(screen.getByRole("button", { name: "Sign In with GitHub Copilot" }));
 expect(await screen.findByText("Finishing sign-in…")).toBeInTheDocument();
 expect(screen.queryByText("Enter code: ABCD-1234")).not.toBeInTheDocument();
 expect(screen.queryByRole("link")).not.toBeInTheDocument();
 await waitFor(() => expect(connected).toHaveBeenCalledOnce(), { timeout: 4500 });
 expect(checks).toBe(2);
}, 6000);
it("continues the ChatGPT handoff through completing", async () => {
 const user = userEvent.setup();
 centralFetch.mockImplementation(async (path: string, options?: RequestInit) => {
  if (path.endsWith("/methods")) return { providers: ["openai"] };
  return options?.method === "POST" ? { ...attempt, provider: "openai", status: "completing", url: undefined } : { ...attempt, provider: "openai", status: "complete", connection: { id: "chatgpt-connection" } };
 });
 const connected=vi.fn(); view(connected);
 await user.type(await screen.findByLabelText("Account Connection Name"), "Mine");
 await user.click(screen.getByRole("button", { name: "Connect ChatGPT" }));
 expect(await screen.findByText("Finishing sign-in…")).toBeInTheDocument();
 expect(screen.queryByText(/pairing code is no longer available/)).not.toBeInTheDocument();
 await waitFor(() => expect(connected).toHaveBeenCalledOnce(), { timeout: 3000 });
}, 5000);
