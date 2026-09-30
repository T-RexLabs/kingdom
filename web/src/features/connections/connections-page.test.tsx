import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render as renderBare, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ReactElement, ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ConnectionsPage } from "./connections-page";
import { IssuePicker } from "./issue-picker";

const route = vi.hoisted(() => ({ search: {} as { connection?: string }, listeners: new Set<() => void>() }));
vi.mock("@tanstack/react-router", async () => {
  const React = await vi.importActual<typeof import("react")>("react");
  return {
    Link: ({ children, to }: { children: ReactNode; to: string }) => <a href={to}>{children}</a>,
    useSearch: ({ select }: { select: (search: { connection?: string }) => string }) => React.useSyncExternalStore((listener) => { route.listeners.add(listener); return () => { route.listeners.delete(listener); }; }, () => select(route.search)),
    useNavigate: () => ({ search }: { search: { connection?: string } }) => { route.search = search; for (const listener of route.listeners) listener(); },
  };
});
afterEach(() => { route.search = {}; });

// A fresh client per test. The connections list is one shared cache entry in
// the app, so a client reused across tests would let one test's list satisfy
// the next test's query and hide a request that should have been made.
function render(ui: ReactElement) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return renderBare(<QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>);
}

async function startSharedUI(user: ReturnType<typeof userEvent.setup>) {
  await user.click(await screen.findByRole("button", { name: "Add Connection" }));
  await user.click(await within(screen.getByRole("dialog", { name: "Add Connection" })).findByRole("button", { name: "Connect GitHub" }));
}
async function startPrivateUI(user: ReturnType<typeof userEvent.setup>) {
  await user.click(await screen.findByRole("button", { name: "Add Connection" }));
  await user.click(within(screen.getByRole("dialog", { name: "Add Connection" })).getByRole("button", { name: "Create a Private GitHub App" }));
  await user.click(within(screen.getByRole("dialog", { name: "Create a Private GitHub App" })).getByRole("button", { name: "Create Private App on GitHub" }));
}
async function openManualUI(user: ReturnType<typeof userEvent.setup>) {
  await user.click(await screen.findByRole("button", { name: "Add Connection" }));
  await user.click(within(screen.getByRole("dialog", { name: "Add Connection" })).getByRole("button", { name: "Use Manual Setup" }));
}
async function openConnection() {
  await userEvent.click(await screen.findByRole("button", { name: /^Manage / }));
  await userEvent.click(screen.getByText("Manage Connection"));
}

const connection = (displayName: string, appID = "1") => ({
  connection_id: "conn-1",
  provider: "github" as const,
  display_name: displayName,
  app_id: appID,
  client_id: "Iv1",
  installations: [{ installation_id: "2", account_id: "10", account_login: "octocat" }],
  credential_strategy: "app_private_key" as const,
  app_slug: "kingdom-issues",
  lifecycle: "enabled" as const,
  health_class:
    appID === "1" ? ("healthy" as const) : ("never_checked" as const),
  credential_state: "present" as const,
  created_at: "2026-08-01T00:00:00Z",
  updated_at: "2026-08-12T00:00:00Z",
});

const json = (value: unknown) =>
  new Response(JSON.stringify(value), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  });

/**
 * The setup flow opens one named tab for GitHub and refuses to continue when
 * the browser blocks it, so tests must provide one.
 */
const stubSetupWindow = () => {
  const opened: Array<{ url: string; name: string }> = [];
  const target = { focus: vi.fn(), location: { href: "" } } as unknown as Window;
  vi.stubGlobal(
    "open",
    vi.fn((url?: string | URL, name?: string) => {
      opened.push({ url: String(url ?? ""), name: String(name ?? "") });
      return target;
    }),
  );
  return opened;
};

const expectIdempotencyScope = (key: string, scope: string) => {
  expect(key).toMatch(/^kd1\.[0-9]+\.86400\.[a-z0-9-]+\.[A-Za-z0-9_-]{22}$/);
  expect(key.split(".")[3]).toBe(scope);
};

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("ConnectionsPage", () => {
  it("makes the shared App the primary guided connection when enabled", async () => {
    let startIdempotencyKey = "";
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (
          url.endsWith("/connections") &&
          (!init?.method || init.method === "GET")
        )
          return json({ items: [] });
        if (url.endsWith("/connections/github/setup-options"))
          return json({
            shared_enabled: true,
            app_slug: "kingdom",
            install_url: "https://github.com/apps/kingdom/installations/new",
          });
        if (url.endsWith("/connections/github/manifest-flows"))
          return new Response(null, { status: 204 });
        if (
          url.endsWith("/connections/github/device-flows") &&
          init?.method === "POST"
        ) {
          startIdempotencyKey = new Headers(init.headers).get("Idempotency-Key") || "";
          return new Response(
            JSON.stringify({
              flow_id: "flow-1234567890123456",
              stage: "awaiting_authorization",
              user_code: "ABCD-EFGH",
              verification_uri: "https://github.com/login/device",
              expires_at: "2099-08-15T13:00:00Z",
              interval_seconds: 5,
              retry_at: "2099-08-15T12:00:05Z",
              candidates: [],
            }),
            { status: 201, headers: { "Content-Type": "application/json" } },
          );
        }
        throw new Error(`unexpected request ${url}`);
      }),
    );
    const user = userEvent.setup();
    render(<ConnectionsPage />);
    await startSharedUI(user);
    const dialog = await screen.findByRole("dialog", {
      name: "Connect GitHub",
    });
    expect(within(dialog).getByText("ABCD-EFGH")).toBeInTheDocument();
    expect(
      within(dialog).getByRole("link", { name: /open github/i }),
    ).toHaveAttribute("href", "https://github.com/login/device");
    expect(window.localStorage.length).toBe(0);
    expect(window.sessionStorage.length).toBe(0);
    expectIdempotencyScope(startIdempotencyKey, "device-start");
  });

  it("keeps an active flow across Close instead of starting a duplicate authorization", async () => {
    let starts = 0;
    let pollRequested = false;
    let resolveOldPoll!: (response: Response) => void;
    const oldPoll = new Promise<Response>((resolve) => {
      resolveOldPoll = resolve;
    });
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/connections") && (!init?.method || init.method === "GET")) return json({ items: [] });
        if (url.endsWith("/connections/github/setup-options")) return json({ shared_enabled: true, app_slug: "kingdom", install_url: "https://github.com/apps/kingdom/installations/new" });
        if (url.endsWith("/connections/github/manifest-flows")) return new Response(null, { status: 204 });
        if (url.endsWith("/connections/github/device-flows") && init?.method === "POST") {
          starts += 1;
          return new Response(JSON.stringify({
            flow_id: starts === 1 ? "flow-old-123456789" : "flow-new-123456789",
            stage: "awaiting_authorization",
            user_code: starts === 1 ? "OLD1-CODE" : "NEW2-CODE",
            verification_uri: "https://github.com/login/device",
            expires_at: "2099-08-15T13:00:00Z",
            interval_seconds: 5,
            retry_at: starts === 1 ? "2000-01-01T00:00:00Z" : "2099-08-15T12:00:05Z",
            candidates: [],
          }), { status: 201, headers: { "Content-Type": "application/json" } });
        }
        if (url.endsWith("/connections/github/device-flows/flow-old-123456789/poll")) {
          pollRequested = true;
          return oldPoll;
        }
        throw new Error(`unexpected request ${url}`);
      }),
    );

    const user = userEvent.setup();
    render(<ConnectionsPage />);
    await startSharedUI(user);
    expect(await screen.findByText("OLD1-CODE")).toBeInTheDocument();
    await waitFor(() => expect(pollRequested).toBe(true));
    await user.click(screen.getByRole("button", { name: "Close dialog" }));
    expect(screen.getByRole("button", { name: "Add Connection" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Resume Setup" }));
    expect(await screen.findByText("OLD1-CODE")).toBeInTheDocument();
    expect(starts).toBe(1);

    await act(async () => {
      resolveOldPoll(new Response(JSON.stringify({ error: { code: "dependency_blocked", message: "old flow expired" } }), {
        status: 409,
        headers: { "Content-Type": "application/json" },
      }));
      await oldPoll;
    });
    await waitFor(() => expect(screen.queryByText("old flow expired")).not.toBeInTheDocument());
    expect(await screen.findByText("GitHub authorization could not be completed. Start again.")).toBeInTheDocument();
  });

  it("accepts a same-flow poll response after the modal closes", async () => {
    let resolvePoll!: (response: Response) => void;
    const poll = new Promise<Response>((resolve) => {
      resolvePoll = resolve;
    });
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/connections") && (!init?.method || init.method === "GET")) return json({ items: [] });
      if (url.endsWith("/connections/github/setup-options")) return json({ shared_enabled: true });
      if (url.endsWith("/connections/github/manifest-flows")) return new Response(null, { status: 204 });
      if (url.endsWith("/connections/github/device-flows") && init?.method === "POST") {
        return new Response(JSON.stringify({
          flow_id: "flow-close-poll-123456",
          stage: "awaiting_authorization",
          user_code: "KEEP-CODE",
          verification_uri: "https://github.com/login/device",
          expires_at: "2099-08-15T13:00:00Z",
          interval_seconds: 5,
          retry_at: "2000-01-01T00:00:00Z",
          candidates: [],
        }), { status: 201, headers: { "Content-Type": "application/json" } });
      }
      if (url.endsWith("/device-flows/flow-close-poll-123456/poll")) return poll;
      throw new Error(`unexpected request ${url}`);
    }));
    const user = userEvent.setup();
    render(<ConnectionsPage />);

    await startSharedUI(user);
    await waitFor(() => expect(screen.getByText("KEEP-CODE")).toBeInTheDocument());
    await user.click(screen.getByRole("button", { name: "Close dialog" }));
    await act(async () => {
      resolvePoll(json({
        flow_id: "flow-close-poll-123456",
        stage: "needs_installation",
        expires_at: "2099-08-15T13:00:00Z",
        interval_seconds: 5,
        install_url: "https://github.com/apps/kingdom/installations/new",
        candidates: [],
      }));
      await poll;
    });

    await user.click(screen.getByRole("button", { name: "Resume Setup" }));
    expect(await screen.findByText("Choose Repositories on GitHub")).toBeInTheDocument();
    expect(screen.queryByText("KEEP-CODE")).not.toBeInTheDocument();
  });

  it("ignores a late poll response after the flow ID is replaced", async () => {
    let starts = 0;
    let resolveOldPoll!: (response: Response) => void;
    const oldPoll = new Promise<Response>((resolve) => {
      resolveOldPoll = resolve;
    });
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/connections") && (!init?.method || init.method === "GET")) return json({ items: [] });
      if (url.endsWith("/connections/github/setup-options")) return json({ shared_enabled: true });
      if (url.endsWith("/connections/github/manifest-flows")) return new Response(null, { status: 204 });
      if (url.endsWith("/connections/github/device-flows") && init?.method === "POST") {
        starts += 1;
        return new Response(JSON.stringify({
          flow_id: starts === 1 ? "flow-old-late-123456" : "flow-new-live-123456",
          stage: "awaiting_authorization",
          user_code: starts === 1 ? "OLD1-CODE" : "NEW2-CODE",
          verification_uri: "https://github.com/login/device",
          expires_at: "2099-08-15T13:00:00Z",
          interval_seconds: 5,
          retry_at: starts === 1 ? "2000-01-01T00:00:00Z" : "2099-08-15T12:00:05Z",
          candidates: [],
        }), { status: 201, headers: { "Content-Type": "application/json" } });
      }
      if (url.endsWith("/device-flows/flow-old-late-123456/poll")) return oldPoll;
      if (url.endsWith("/device-flows/flow-old-late-123456") && init?.method === "DELETE") return new Response(null, { status: 204 });
      throw new Error(`unexpected request ${url}`);
    }));
    const user = userEvent.setup();
    render(<ConnectionsPage />);

    await startSharedUI(user);
    expect(await screen.findByText("OLD1-CODE")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Discard Setup" }));
    await startSharedUI(user);
    expect(await screen.findByText("NEW2-CODE")).toBeInTheDocument();

    await act(async () => {
      resolveOldPoll(json({
        flow_id: "flow-old-late-123456",
        stage: "needs_installation",
        expires_at: "2099-08-15T13:00:00Z",
        interval_seconds: 5,
        candidates: [],
      }));
      await oldPoll;
    });
    expect(screen.getByText("NEW2-CODE")).toBeInTheDocument();
    expect(screen.queryByText("Choose Repositories on GitHub")).not.toBeInTheDocument();
  });

  it("ignores a late poll response after unmount", async () => {
    let resolvePoll!: (response: Response) => void;
    let pollStarted = false;
    const poll = new Promise<Response>((resolve) => {
      resolvePoll = resolve;
    });
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/connections") && (!init?.method || init.method === "GET")) return json({ items: [] });
      if (url.endsWith("/connections/github/setup-options")) return json({ shared_enabled: true });
      if (url.endsWith("/connections/github/manifest-flows")) return new Response(null, { status: 204 });
      if (url.endsWith("/connections/github/device-flows") && init?.method === "POST") {
        return new Response(JSON.stringify({
          flow_id: "flow-unmount-12345678",
          stage: "awaiting_authorization",
          user_code: "GONE-CODE",
          verification_uri: "https://github.com/login/device",
          expires_at: "2099-08-15T13:00:00Z",
          interval_seconds: 5,
          retry_at: "2000-01-01T00:00:00Z",
          candidates: [],
        }), { status: 201, headers: { "Content-Type": "application/json" } });
      }
      if (url.endsWith("/device-flows/flow-unmount-12345678/poll")) {
        pollStarted = true;
        return poll;
      }
      throw new Error(`unexpected request ${url}`);
    }));
    const user = userEvent.setup();
    const view = render(<ConnectionsPage />);
    await startSharedUI(user);
    await waitFor(() => expect(pollStarted).toBe(true));

    view.unmount();
    await act(async () => {
      resolvePoll(json({
        flow_id: "flow-unmount-12345678",
        stage: "needs_installation",
        expires_at: "2099-08-15T13:00:00Z",
        interval_seconds: 5,
        candidates: [],
      }));
      await poll;
    });
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("moves a known poll failure to a safe terminal state", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/connections") && (!init?.method || init.method === "GET"))
          return json({ items: [] });
        if (url.endsWith("/connections/github/setup-options"))
          return json({ shared_enabled: true });
        if (url.endsWith("/connections/github/manifest-flows"))
          return new Response(null, { status: 204 });
        if (url.endsWith("/connections/github/device-flows") && init?.method === "POST")
          return new Response(
            JSON.stringify({
              flow_id: "flow-terminal-123456",
              stage: "awaiting_authorization",
              user_code: "FAIL-CODE",
              verification_uri: "https://github.com/login/device",
              expires_at: "2099-08-15T13:00:00Z",
              interval_seconds: 5,
              retry_at: "2000-01-01T00:00:00Z",
              candidates: [],
            }),
            { status: 201, headers: { "Content-Type": "application/json" } },
          );
        if (url.endsWith("/device-flows/flow-terminal-123456/poll"))
          return new Response(
            JSON.stringify({
              error: {
                code: "dependency_blocked",
                message: "provider diagnostic must not be shown",
              },
            }),
            { status: 503, headers: { "Content-Type": "application/json" } },
          );
        throw new Error(`unexpected request ${url}`);
      }),
    );
    const user = userEvent.setup();
    render(<ConnectionsPage />);

    await startSharedUI(user);

    expect(
      await screen.findByText("GitHub authorization could not be completed. Start again."),
    ).toBeInTheDocument();
    expect(screen.queryByText("FAIL-CODE")).not.toBeInTheDocument();
    expect(screen.queryByText(/waiting safely/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/provider diagnostic/i)).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Start Again" })).toBeInTheDocument();
  });

  it("retries an ambiguous poll with the same key and clears retry guidance on success", async () => {
    let pollAttempts = 0;
    const keys: string[] = [];
    let resolveRetry!: (response: Response) => void;
    const retry = new Promise<Response>((resolve) => {
      resolveRetry = resolve;
    });
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/connections") && (!init?.method || init.method === "GET"))
          return json({ items: [] });
        if (url.endsWith("/connections/github/setup-options"))
          return json({ shared_enabled: true });
        if (url.endsWith("/connections/github/manifest-flows"))
          return new Response(null, { status: 204 });
        if (url.endsWith("/connections/github/device-flows") && init?.method === "POST")
          return new Response(
            JSON.stringify({
              flow_id: "flow-retry-123456789",
              stage: "awaiting_authorization",
              user_code: "KEEP-CODE",
              verification_uri: "https://github.com/login/device",
              expires_at: "2099-08-15T13:00:00Z",
              interval_seconds: 0.02,
              retry_at: "2000-01-01T00:00:00Z",
              candidates: [],
            }),
            { status: 201, headers: { "Content-Type": "application/json" } },
          );
        if (url.endsWith("/device-flows/flow-retry-123456789/poll")) {
          pollAttempts += 1;
          keys.push(new Headers(init?.headers).get("Idempotency-Key") || "");
          if (pollAttempts === 1) throw new TypeError("connection reset");
          return retry;
        }
        throw new Error(`unexpected request ${url}`);
      }),
    );
    const user = userEvent.setup();
    render(<ConnectionsPage />);

    await startSharedUI(user);
    expect(await screen.findByText(/retrying safely/i)).toBeInTheDocument();
    await waitFor(() => expect(pollAttempts).toBe(2));
    expect(keys[1]).toBe(keys[0]);
    expect(screen.getByText("KEEP-CODE")).toBeInTheDocument();

    await act(async () => {
      resolveRetry(
        json({
          flow_id: "flow-retry-123456789",
          stage: "awaiting_authorization",
          user_code: "KEEP-CODE",
          verification_uri: "https://github.com/login/device",
          expires_at: "2099-08-15T13:00:00Z",
          interval_seconds: 5,
          retry_at: "2099-08-15T12:00:05Z",
          candidates: [],
        }),
      );
      await retry;
    });
    await waitFor(() =>
      expect(screen.queryByText(/retrying safely/i)).not.toBeInTheDocument(),
    );
  });

  it("honors a 429 retry time before polling again", async () => {
    let pollAttempts = 0;
    const retryAt = new Date(Date.now() + 200).toISOString();
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/connections") && (!init?.method || init.method === "GET"))
          return json({ items: [] });
        if (url.endsWith("/connections/github/setup-options"))
          return json({ shared_enabled: true });
        if (url.endsWith("/connections/github/manifest-flows"))
          return new Response(null, { status: 204 });
        if (url.endsWith("/connections/github/device-flows") && init?.method === "POST")
          return new Response(
            JSON.stringify({
              flow_id: "flow-paced-12345678",
              stage: "awaiting_authorization",
              user_code: "PACE-CODE",
              verification_uri: "https://github.com/login/device",
              expires_at: "2099-08-15T13:00:00Z",
              interval_seconds: 5,
              retry_at: "2000-01-01T00:00:00Z",
              candidates: [],
            }),
            { status: 201, headers: { "Content-Type": "application/json" } },
          );
        if (url.endsWith("/device-flows/flow-paced-12345678/poll")) {
          pollAttempts += 1;
          if (pollAttempts === 1)
            return new Response(
              JSON.stringify({
                error: {
                  code: "dependency_blocked",
                  message: "not due",
                  retry_at: retryAt,
                },
              }),
              { status: 429, headers: { "Content-Type": "application/json" } },
            );
          return json({
            flow_id: "flow-paced-12345678",
            stage: "awaiting_authorization",
            user_code: "PACE-CODE",
            verification_uri: "https://github.com/login/device",
            expires_at: "2099-08-15T13:00:00Z",
            interval_seconds: 5,
            retry_at: "2099-08-15T12:00:05Z",
            candidates: [],
          });
        }
        throw new Error(`unexpected request ${url}`);
      }),
    );
    const user = userEvent.setup();
    render(<ConnectionsPage />);

    await startSharedUI(user);
    await waitFor(() => expect(pollAttempts).toBe(1));
    await new Promise((resolve) => window.setTimeout(resolve, 50));
    expect(pollAttempts).toBe(1);
    await waitFor(() => expect(pollAttempts).toBe(2));
    expect(screen.getByText("PACE-CODE")).toBeInTheDocument();
  });

  it("starts a fresh device flow after a terminal poll failure", async () => {
    let starts = 0;
    const startKeys: string[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/connections") && (!init?.method || init.method === "GET"))
          return json({ items: [] });
        if (url.endsWith("/connections/github/setup-options"))
          return json({ shared_enabled: true });
        if (url.endsWith("/connections/github/manifest-flows"))
          return new Response(null, { status: 204 });
        if (url.endsWith("/connections/github/device-flows") && init?.method === "POST") {
          starts += 1;
          startKeys.push(new Headers(init.headers).get("Idempotency-Key") || "");
          return new Response(
            JSON.stringify({
              flow_id: starts === 1 ? "flow-first-123456789" : "flow-fresh-123456789",
              stage: "awaiting_authorization",
              user_code: starts === 1 ? "OLD1-CODE" : "NEW2-CODE",
              verification_uri: "https://github.com/login/device",
              expires_at: "2099-08-15T13:00:00Z",
              interval_seconds: 5,
              retry_at: starts === 1 ? "2000-01-01T00:00:00Z" : "2099-08-15T12:00:05Z",
              candidates: [],
            }),
            { status: 201, headers: { "Content-Type": "application/json" } },
          );
        }
        if (url.endsWith("/device-flows/flow-first-123456789/poll"))
          return new Response(
            JSON.stringify({ error: { code: "dependency_blocked", message: "terminal" } }),
            { status: 503, headers: { "Content-Type": "application/json" } },
          );
        throw new Error(`unexpected request ${url}`);
      }),
    );
    const user = userEvent.setup();
    render(<ConnectionsPage />);

    await startSharedUI(user);
    await user.click(await screen.findByRole("button", { name: "Start Again" }));

    expect(await screen.findByText("NEW2-CODE")).toBeInTheDocument();
    expect(screen.queryByText("OLD1-CODE")).not.toBeInTheDocument();
    expect(startKeys).toHaveLength(2);
    expect(startKeys[1]).not.toBe(startKeys[0]);
  });

  it("recovers an intent conflict through transient 204 and starting status", async () => {
    const shared = {
      ...connection("Shared"),
      credential_strategy: "user_access_token" as const,
    };
    let currentReads = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/connections") && (!init?.method || init.method === "GET"))
          return json({ items: [shared] });
        if (url.endsWith("/connections/github/setup-options"))
          return json({ shared_enabled: true, app_slug: "kingdom" });
        if (url.endsWith("/connections/github/manifest-flows"))
          return new Response(null, { status: 204 });
        if (url.endsWith("/connections/github/device-flows") && (!init?.method || init.method === "GET")) {
          currentReads += 1;
          if (currentReads === 1) throw new TypeError("mount discovery failed");
          if (currentReads === 2) return new Response(null, { status: 204 });
          return json({
            flow_id: "flow-recovered-123456",
            stage: "starting_authorization",
            reconnect_connection_id: "conn-1",
            expires_at: "2099-08-15T13:00:00Z",
            interval_seconds: 5,
            candidates: [],
          });
        }
        if (url.endsWith("/device-flows/flow-recovered-123456") && (!init?.method || init.method === "GET")) {
          return json({
            flow_id: "flow-recovered-123456",
            stage: "selecting_installation",
            reconnect_connection_id: "conn-1",
            expires_at: "2099-08-15T13:00:00Z",
            interval_seconds: 5,
            install_url: "https://github.com/apps/kingdom/installations/new",
            candidates: [{
              installation_id: "2",
              display_name: "octocat",
              account_id: "10",
              account_login: "octocat",
              app_id: "1",
              client_id: "Iv1",
              app_slug: "kingdom",
            }],
          });
        }
        if (url.endsWith("/connections/github/device-flows") && init?.method === "POST")
          return new Response(JSON.stringify({
            error: {
              code: "dependency_blocked",
              message: "another GitHub setup is active",
            },
          }), { status: 409, headers: { "Content-Type": "application/json" } });
        throw new Error(`unexpected request ${url}`);
      }),
    );

    const user = userEvent.setup();
    render(<ConnectionsPage />);
    await screen.findByText("Shared");
    await waitFor(() => expect(currentReads).toBe(1));
    await startSharedUI(user);

    expect(await screen.findByText("GitHub Reconnect in Progress")).toBeInTheDocument();
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(screen.queryByText("another GitHub setup is active")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Resume Setup" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Discard Setup" })).toBeInTheDocument();
    expect(currentReads).toBe(3);

    await user.click(screen.getByRole("button", { name: "Resume Setup" }));
    expect(await screen.findByRole("dialog", { name: "Reconnect GitHub" })).toBeInTheDocument();
    expect(await screen.findByRole("checkbox", { name: /@octocat/ })).toBeChecked();
    expect(window.localStorage.length).toBe(0);
    expect(window.sessionStorage.length).toBe(0);
  });

  it("clears reconnect selections when a 409 adopts a fresh canonical flow", async () => {
    const shared = {
      ...connection("Shared"),
      credential_strategy: "user_access_token" as const,
    };
    let currentReads = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/connections") && (!init?.method || init.method === "GET")) return json({ items: [shared] });
      if (url.endsWith("/connections/github/setup-options")) return json({ shared_enabled: true });
      if (url.endsWith("/connections/github/manifest-flows")) return new Response(null, { status: 204 });
      if (url.endsWith("/connections/github/device-flows") && (!init?.method || init.method === "GET")) {
        currentReads += 1;
        if (currentReads === 1) return new Response(null, { status: 204 });
        return json({
          flow_id: "flow-canonical-fresh-1",
          stage: "selecting_installation",
          expires_at: "2099-08-15T13:00:00Z",
          interval_seconds: 5,
          install_url: "https://github.com/apps/kingdom/installations/new",
          candidates: [{
            installation_id: "2",
            display_name: "octocat",
            account_id: "10",
            account_login: "octocat",
            app_id: "1",
            client_id: "Iv1",
            app_slug: "kingdom",
          }],
        });
      }
      if (url.endsWith("/connections/github/device-flows") && init?.method === "POST") {
        return new Response(JSON.stringify({ error: { code: "dependency_blocked", message: "fresh setup already active" } }), {
          status: 409,
          headers: { "Content-Type": "application/json" },
        });
      }
      throw new Error(`unexpected request ${url}`);
    }));
    const user = userEvent.setup();
    render(<ConnectionsPage />);
    await openConnection();
    await screen.findByText("Shared");
    await waitFor(() => expect(currentReads).toBe(1));

    await user.click(screen.getByRole("button", { name: "Reconnect Connection" }));
    expect(await screen.findByText("GitHub Setup in Progress")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Resume Setup" }));

    expect(await screen.findByRole("dialog", { name: "Connect GitHub" })).toBeInTheDocument();
    expect(screen.getByRole("checkbox", { name: /@octocat/ })).not.toBeChecked();
    expect(screen.getByText("0 selected")).toBeInTheDocument();
  });

  it("stops preparing after bounded 409 rediscovery finds no current flow", async () => {
    let currentReads = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/connections") && (!init?.method || init.method === "GET")) return json({ items: [] });
      if (url.endsWith("/connections/github/setup-options")) return json({ shared_enabled: true });
      if (url.endsWith("/connections/github/manifest-flows")) return new Response(null, { status: 204 });
      if (url.endsWith("/connections/github/device-flows") && (!init?.method || init.method === "GET")) {
        currentReads += 1;
        return new Response(null, { status: 204 });
      }
      if (url.endsWith("/connections/github/device-flows") && init?.method === "POST") {
        return new Response(JSON.stringify({ error: { code: "dependency_blocked", message: "GitHub authorization is already being started" } }), {
          status: 409,
          headers: { "Content-Type": "application/json" },
        });
      }
      throw new Error(`unexpected request ${url}`);
    }));
    const user = userEvent.setup();
    render(<ConnectionsPage />);
    await waitFor(() => expect(currentReads).toBe(1));

    await startSharedUI(user);
    expect(screen.getByText("Preparing GitHub...")).toBeInTheDocument();
    expect(await screen.findByText("GitHub authorization is already being started", {}, { timeout: 2500 })).toBeInTheDocument();
    expect(currentReads).toBe(1 + 5);
    expect(screen.queryByText("Preparing GitHub...")).not.toBeInTheDocument();
    expect(screen.getByText("No active GitHub setup is available.")).toBeInTheDocument();
  });

  it("starts the primary personal manifest flow with a browser POST form into the setup tab", async () => {
    const opened = stubSetupWindow();
    let submittedForm: HTMLFormElement | undefined;
    const submit = vi
      .spyOn(HTMLFormElement.prototype, "submit")
      .mockImplementation(() => {
        submittedForm = document.querySelector<HTMLFormElement>('form[action^="https://github.com/"]') ?? undefined;
      });
    let startBody: Record<string, unknown> | undefined;
    let startIdempotencyKey = "";
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (
          url.endsWith("/connections") &&
          (!init?.method || init.method === "GET")
        )
          return json({ items: [] });
        if (
          url.endsWith("/connections/github/manifest-flows") &&
          (!init?.method || init.method === "GET")
        )
          return new Response(
            JSON.stringify({
              error: { code: "dependency_blocked", message: "none" },
            }),
            { status: 404, headers: { "Content-Type": "application/json" } },
          );
        if (
          url.endsWith("/connections/github/manifest-flows") &&
          init?.method === "POST"
        ) {
          startBody = JSON.parse(String(init.body));
          startIdempotencyKey = new Headers(init.headers).get("Idempotency-Key") || "";
          return new Response(
            JSON.stringify({
              flow_id: "flow-1234567890123456",
              form_action: "https://github.com/settings/apps/new?state=secret",
              manifest: '{"name":"Kingdom Issues"}',
              expires_at: "2026-08-15T13:00:00Z",
            }),
            { status: 201, headers: { "Content-Type": "application/json" } },
          );
        }
        throw new Error(`unexpected request ${url}`);
      }),
    );
    const user = userEvent.setup();
    render(<ConnectionsPage />);
    await startPrivateUI(user);
    await waitFor(() => expect(submit).toHaveBeenCalledOnce());
    expect(startBody).toEqual({ owner_type: "personal" });
    expectIdempotencyScope(startIdempotencyKey, "manifest-start");
    expect(submittedForm).toHaveAttribute("method", "post");
    expect(submittedForm).toHaveAttribute(
      "action",
      "https://github.com/settings/apps/new?state=secret",
    );
    expect(
      within(submittedForm as HTMLFormElement).getByDisplayValue(
        '{"name":"Kingdom Issues"}',
      ),
    ).toHaveAttribute("name", "manifest");
    // GitHub takes the dedicated tab; this tab keeps driving the flow.
    expect(submittedForm).toHaveAttribute("target", "kingdom-github-setup");
    expect(opened).toEqual([{ url: "", name: "kingdom-github-setup" }]);
    expect(document.querySelector('form[action^="https://github.com/"]')).toBeNull();
    // This tab keeps driving: it shows the stage immediately, without a reload.
    expect(await screen.findByText("Finish creating the private App on GitHub.")).toBeInTheDocument();
  });

  it("shows recovery guidance when the browser blocks a GitHub handoff tab", async () => {
    let installationRequests = 0;
    const open = vi.fn().mockReturnValue(null);
    vi.stubGlobal("open", open);
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/connections") && (!init?.method || init.method === "GET")) {
          return json({ items: [] });
        }
        if (url.endsWith("/connections/github/setup-options")) {
          return json({ shared_enabled: false });
        }
        if (url.endsWith("/connections/github/manifest-flows")) {
          return json({
            flow_id: "flow-popup-blocked-123456",
            stage: "awaiting_installation",
            expires_at: "2099-08-15T13:00:00Z",
          });
        }
        if (url.endsWith("/manifest-flows/flow-popup-blocked-123456/installation") && init?.method === "POST") {
          installationRequests += 1;
          return json({ installation_url: "https://github.com/apps/kingdom/installations/new" });
        }
        throw new Error(`unexpected request ${url}`);
      }),
    );
    const user = userEvent.setup();
    render(<ConnectionsPage />);

    await user.click(await screen.findByRole("button", { name: "Choose Repositories on GitHub" }));

    expect(await screen.findByText(/allow pop-ups for this site/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Retry This Step" })).toBeInTheDocument();
    expect(open).toHaveBeenCalledWith("https://github.com/apps/kingdom/installations/new", "kingdom-github-setup");
    expect(installationRequests).toBe(1);
  });

  it("keeps manual metadata in a secondary modal and shows post-save CLI guidance", async () => {
    let postBody: Record<string, unknown> | undefined;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (
          url.endsWith("/connections") &&
          (!init?.method || init.method === "GET")
        )
          return json({ items: [] });
        if (url.endsWith("/manifest-flows"))
          return new Response(null, { status: 404 });
        if (url.endsWith("/connections") && init?.method === "POST") {
          postBody = JSON.parse(String(init.body));
          return new Response(JSON.stringify(connection("Manual")), {
            status: 201,
            headers: { "Content-Type": "application/json" },
          });
        }
        throw new Error(`unexpected request ${url}`);
      }),
    );
    const user = userEvent.setup();
    render(<ConnectionsPage />);
    await openManualUI(user);
    const dialog = await screen.findByRole("dialog", {
      name: /manual github app setup/i,
    });
    for (const [label, value] of [
      ["Display Name", "Manual"],
      ["App ID", "1"],
      ["Client ID", "Iv1"],
      ["Installation ID", "2"],
    ]) {
      await user.type(within(dialog).getByLabelText(label), value);
    }
    await user.click(
      within(dialog).getByRole("button", { name: /save connection metadata/i }),
    );
    expect(
      await within(dialog).findByText(/metadata saved/i),
    ).toBeInTheDocument();
    expect(
      within(dialog).getByText(
        /kd connections github key provision --connection-id conn-1/i,
      ),
    ).toBeInTheDocument();
    expect(postBody).toEqual({
      provider: "github",
      credential_strategy: "app_private_key",
      display_name: "Manual",
      app_id: "1",
      client_id: "Iv1",
      installation_id: "2",
    });
  });

  // The reported bug: add a connection, go to the planner or the work order form,
  // and the import trigger is still the setup link. Both surfaces read the
  // shared connections entry, so the fix is that creating one invalidates it.
  it("stops offering setup on other surfaces as soon as a connection exists", async () => {
    let listed: Array<ReturnType<typeof connection>> = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/connections") && (!init?.method || init.method === "GET"))
          return json({ items: listed });
        if (url.endsWith("/manifest-flows")) return new Response(null, { status: 404 });
        if (url.endsWith("/connections") && init?.method === "POST") {
          listed = [connection("Manual")];
          return new Response(JSON.stringify(connection("Manual")), {
            status: 201,
            headers: { "Content-Type": "application/json" },
          });
        }
        throw new Error(`unexpected request ${url}`);
      }),
    );
    const user = userEvent.setup();
    render(
      <>
        <ConnectionsPage />
        <IssuePicker onSelect={vi.fn()} />
      </>,
    );

    expect(
      await screen.findByRole("link", { name: /connect an issue tracker/i }),
    ).toBeInTheDocument();

    await openManualUI(user);
    const dialog = await screen.findByRole("dialog", { name: /manual github app setup/i });
    for (const [label, value] of [
      ["Display Name", "Manual"],
      ["App ID", "1"],
      ["Client ID", "Iv1"],
      ["Installation ID", "2"],
    ]) {
      await user.type(within(dialog).getByLabelText(label), value);
    }
    await user.click(within(dialog).getByRole("button", { name: /save connection metadata/i }));

    // No navigation, no reload: the other surface re-reads because the entry it
    // shares with this page was invalidated.
    await waitFor(() =>
      expect(screen.queryByRole("link", { name: /connect an issue tracker/i })).not.toBeInTheDocument(),
    );
    expect(screen.getByRole("button", { name: /import from github issue/i })).toBeInTheDocument();
  });

  it("resumes an authorization callback stage and finalizes without query or browser storage", async () => {
    window.localStorage.clear();
    window.sessionStorage.clear();
    let completed = 0;
    let connectionLoads = 0;
    let completionIdempotencyKey = "";
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (
          url.endsWith("/connections") &&
          (!init?.method || init.method === "GET")
        ) {
          connectionLoads++;
          return json({ items: completed ? [connection("Ready")] : [] });
        }
        if (
          url.endsWith("/connections/github/manifest-flows") &&
          (!init?.method || init.method === "GET")
        )
          return json({
            flow_id: "flow-1234567890123456",
            stage: "authorization_received",
            expires_at: "2026-08-15T13:00:00Z",
            app_name: "Kingdom Issues",
          });
        if (
          url.endsWith(
            "/connections/github/manifest-flows/flow-1234567890123456/completion",
          ) &&
          init?.method === "POST"
        ) {
          completed++;
          completionIdempotencyKey = new Headers(init.headers).get("Idempotency-Key") || "";
          return new Response(JSON.stringify(connection("Ready")), {
            status: 201,
            headers: { "Content-Type": "application/json" },
          });
        }
        throw new Error(`unexpected request ${url}`);
      }),
    );
    render(<ConnectionsPage />);
    expect(await screen.findByText("Ready")).toBeInTheDocument();
    expect(completed).toBe(1);
    expectIdempotencyScope(completionIdempotencyKey, "manifest-completion");
    expect(connectionLoads).toBe(2);
    expect(window.location.search).toBe("");
    expect(window.localStorage.length).toBe(0);
    expect(window.sessionStorage.length).toBe(0);
  });

  it("retries a non-canonical ambiguous HTTP failure with the same private-App key", async () => {
    stubSetupWindow();
    let attempts = 0;
    const keys: string[] = [];
    let submittedForm: HTMLFormElement | undefined;
    vi.spyOn(HTMLFormElement.prototype, "submit").mockImplementation(() => {
      submittedForm = document.querySelector<HTMLFormElement>('form[action^="https://github.com/"]') ?? undefined;
    });
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/connections") && (!init?.method || init.method === "GET")) return json({ items: [] });
      if (url.endsWith("/connections/github/setup-options")) return json({ shared_enabled: false });
      if (url.endsWith("/connections/github/manifest-flows") && (!init?.method || init.method === "GET")) return new Response(null, { status: 204 });
      if (url.endsWith("/connections/github/manifest-flows") && init?.method === "POST") {
        attempts++;
        keys.push(new Headers(init.headers).get("Idempotency-Key") || "");
        if (attempts === 1) return new Response(null, { status: 502, statusText: "Bad Gateway" });
        return new Response(JSON.stringify({ flow_id: "flow-1234567890123456", form_action: "https://github.com/settings/apps/new", manifest: "{}", expires_at: "2099-08-15T13:00:00Z" }), { status: 201, headers: { "Content-Type": "application/json" } });
      }
      throw new Error(`unexpected request ${url}`);
    }));
    const user = userEvent.setup();
    render(<ConnectionsPage />);

    await startPrivateUI(user);
    const alert = await within(screen.getByRole("dialog", { name: "Create a Private GitHub App" })).findByRole("alert");
    expect(alert).toHaveTextContent("Private App setup could not start");
    expect(alert).toHaveTextContent("Bad Gateway");
    await user.click(within(alert).getByRole("button", { name: "Retry This Step" }));

    await waitFor(() => expect(submittedForm).toBeDefined());
    expect(keys).toHaveLength(2);
    expect(keys[1]).toBe(keys[0]);
  });

  it.each([
    ["app_created", "secure the App credentials", "/conversion"],
    ["awaiting_installation", "open GitHub App installation", "/installation"],
    ["installation_received", "start temporary GitHub authorization", "/authorization"],
  ] as const)("shows %s automatic failure in private setup and safely retries the stage", async (stage, guidance, suffix) => {
    const keys: string[] = [];
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/connections") && (!init?.method || init.method === "GET")) return json({ items: [] });
      if (url.endsWith("/connections/github/setup-options")) return json({ shared_enabled: true });
      if (url.endsWith("/connections/github/manifest-flows") && (!init?.method || init.method === "GET")) return json({ flow_id: "flow-1234567890123456", stage, expires_at: "2099-08-15T13:00:00Z", app_name: "Kingdom Issues" });
      if (url.endsWith(suffix) && init?.method === "POST") {
        keys.push(new Headers(init.headers).get("Idempotency-Key") || "");
        throw new TypeError("provider response was interrupted");
      }
      throw new Error(`unexpected request ${url}`);
    }));
    const user = userEvent.setup();
    render(<ConnectionsPage />);

    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent(guidance);
    expect(screen.queryAllByRole("alert")).toHaveLength(1);
    // Resumable server flow state must never open a modal by itself.
    expect(screen.queryByRole("dialog", { name: "Create a Private GitHub App" })).not.toBeInTheDocument();
    await user.click(within(alert).getByRole("button", { name: "Retry This Step" }));
    await waitFor(() => expect(keys).toHaveLength(2));
    expect(keys[1]).toBe(keys[0]);
  });

  it("hands each GitHub hop to the setup tab and advances by polling instead of navigating", async () => {
    const opened = stubSetupWindow();
    const assign = vi.fn();
    vi.stubGlobal("location", { ...window.location, assign, href: window.location.href });
    let stage = "installation_received";
    let completed = false;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/connections") && (!init?.method || init.method === "GET"))
        return json({ items: completed ? [connection("Ready")] : [] });
      if (url.endsWith("/connections/github/setup-options")) return json({ shared_enabled: false });
      if (url.endsWith("/connections/github/manifest-flows") && (!init?.method || init.method === "GET"))
        return json({ flow_id: "flow-1234567890123456", stage, expires_at: "2099-08-15T13:00:00Z" });
      if (url.endsWith("/authorization") && init?.method === "POST") {
        stage = "awaiting_authorization";
        return json({ authorization_url: "https://github.com/login/oauth/authorize?client_id=Iv1" });
      }
      if (url.endsWith("/connections/github/manifest-flows/flow-1234567890123456") && (!init?.method || init.method === "GET")) {
        // GitHub has returned through the callback while this tab waited.
        stage = "authorization_received";
        return json({ flow_id: "flow-1234567890123456", stage, expires_at: "2099-08-15T13:00:00Z" });
      }
      if (url.endsWith("/completion") && init?.method === "POST") {
        completed = true;
        return new Response(JSON.stringify(connection("Ready")), { status: 201, headers: { "Content-Type": "application/json" } });
      }
      throw new Error(`unexpected request ${url}`);
    }));
    const user = userEvent.setup();
    render(<ConnectionsPage />);

    const handoff = await screen.findByRole("button", { name: /authorize on github/i });
    // Preparing the hop must not move this tab.
    expect(assign).not.toHaveBeenCalled();
    expect(opened).toHaveLength(0);

    await user.click(handoff);
    expect(opened).toEqual([
      { url: "https://github.com/login/oauth/authorize?client_id=Iv1", name: "kingdom-github-setup" },
    ]);

    // No reload: the driving tab notices the stage change on its own. The wait
    // spans one 2s poll interval.
    expect(await screen.findByText("Ready", {}, { timeout: 6000 })).toBeInTheDocument();
    expect(assign).not.toHaveBeenCalled();
  });

  it.each([true, false])("never auto-opens private setup and permits resume/abandon with shared setup %s", async (sharedEnabled) => {
    let discardMethod = "";
    let discardKey = "";
    let currentFlowRequests = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/connections") && (!init?.method || init.method === "GET")) return json({ items: [] });
      if (url.endsWith("/connections/github/setup-options")) return json({ shared_enabled: sharedEnabled });
      if (url.endsWith("/connections/github/manifest-flows") && (!init?.method || init.method === "GET")) {
        currentFlowRequests++;
        return json({ flow_id: "flow-1234567890123456", stage: "awaiting_app_creation", expires_at: "2099-08-15T13:00:00Z" });
      }
      if (url.endsWith("/connections/github/manifest-flows/flow-1234567890123456") && init?.method === "DELETE") {
        discardMethod = init.method;
        discardKey = new Headers(init.headers).get("Idempotency-Key") || "";
        return new Response(null, { status: 204 });
      }
      throw new Error(`unexpected request ${url}`);
    }));
    const user = userEvent.setup();
    render(<ConnectionsPage />);

    // The reported bug: navigating back to Connections re-opened the modal
    // because unfinished flow state survives on the server for an hour.
    const status = await screen.findByRole("status");
    expect(status).toHaveTextContent("Finish creating the private App on GitHub.");
    expect(screen.queryByRole("dialog", { name: "Create a Private GitHub App" })).not.toBeInTheDocument();
    expect(currentFlowRequests).toBe(1);

    await user.click(screen.getByRole("button", { name: "Resume Setup" }));
    const dialog = await screen.findByRole("dialog", { name: "Create a Private GitHub App" });

    await user.click(within(dialog).getByRole("button", { name: "Abandon Setup" }));
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "Create a Private GitHub App" })).not.toBeInTheDocument());
    expect(discardMethod).toBe("DELETE");
    expectIdempotencyScope(discardKey, "manifest-discard");
    expect(screen.queryByText("Finish creating the private App on GitHub.")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Resume Setup" })).not.toBeInTheDocument();
  });

  it("retries failed automatic completion with the stable key and clears the private setup error", async () => {
    let completionAttempts = 0;
    let connectionLoads = 0;
    const keys: string[] = [];
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/connections") && (!init?.method || init.method === "GET")) {
        connectionLoads++;
        return json({ items: completionAttempts > 1 ? [connection("Recovered")] : [] });
      }
      if (url.endsWith("/connections/github/setup-options")) return json({ shared_enabled: true });
      if (url.endsWith("/connections/github/manifest-flows") && (!init?.method || init.method === "GET")) return json({ flow_id: "flow-1234567890123456", stage: "authorization_received", expires_at: "2099-08-15T13:00:00Z", app_name: "Kingdom Issues" });
      if (url.endsWith("/completion") && init?.method === "POST") {
        completionAttempts++;
        keys.push(new Headers(init.headers).get("Idempotency-Key") || "");
        if (completionAttempts === 1) throw new TypeError("completion response was lost");
        return new Response(JSON.stringify(connection("Recovered")), { status: 201, headers: { "Content-Type": "application/json" } });
      }
      throw new Error(`unexpected request ${url}`);
    }));
    const user = userEvent.setup();
    render(<ConnectionsPage />);

    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("validate and save the GitHub connection");
    expect(screen.queryByRole("dialog", { name: "Create a Private GitHub App" })).not.toBeInTheDocument();
    await user.click(within(alert).getByRole("button", { name: "Retry This Step" }));

    expect(await screen.findByText("Recovered")).toBeInTheDocument();
    expect(keys).toHaveLength(2);
    expect(keys[1]).toBe(keys[0]);
    expect(connectionLoads).toBe(2);
    expect(screen.queryByText("completion response was lost")).not.toBeInTheDocument();
  });

  it("restarts a known terminal conversion failure as a new flow with a new key", async () => {
    stubSetupWindow();
    let submittedForm: HTMLFormElement | undefined;
    vi.spyOn(HTMLFormElement.prototype, "submit").mockImplementation(() => {
      submittedForm = document.querySelector<HTMLFormElement>('form[action^="https://github.com/"]') ?? undefined;
    });
    let failedKey = "";
    let restartKey = "";
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/connections") && (!init?.method || init.method === "GET")) return json({ items: [] });
      if (url.endsWith("/connections/github/setup-options")) return json({ shared_enabled: true });
      if (url.endsWith("/connections/github/manifest-flows") && (!init?.method || init.method === "GET")) return json({ flow_id: "flow-1234567890123456", stage: "app_created", expires_at: "2099-08-15T13:00:00Z" });
      if (url.endsWith("/conversion") && init?.method === "POST") {
        failedKey = new Headers(init.headers).get("Idempotency-Key") || "";
        return new Response(JSON.stringify({ error: { code: "dependency_blocked", message: "GitHub rejected the one-time code", retryable: false, category: "core", details: {}, correlation_id: "test", occurred_at: "2026-08-15T12:00:00Z" } }), { status: 502, headers: { "Content-Type": "application/json" } });
      }
      if (url.endsWith("/connections/github/manifest-flows") && init?.method === "POST") {
        restartKey = new Headers(init.headers).get("Idempotency-Key") || "";
        return new Response(JSON.stringify({ flow_id: "flow-2222222222222222", form_action: "https://github.com/settings/apps/new", manifest: "{}", expires_at: "2099-08-15T13:00:00Z" }), { status: 201, headers: { "Content-Type": "application/json" } });
      }
      throw new Error(`unexpected request ${url}`);
    }));
    const user = userEvent.setup();
    render(<ConnectionsPage />);

    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("GitHub rejected the one-time code");
    await user.click(within(alert).getByRole("button", { name: "Start Again" }));

    await waitFor(() => expect(submittedForm).toBeDefined());
    expectIdempotencyScope(failedKey, "manifest-conversion");
    expectIdempotencyScope(restartKey, "manifest-start");
    expect(restartKey).not.toBe(failedKey);
  });

  it("edits only the display name and keeps installation identity immutable", async () => {
    let current = connection("Original");
    let patch: { headers?: HeadersInit; body?: BodyInit | null } | undefined;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (
          url.endsWith("/connections") &&
          (!init?.method || init.method === "GET")
        )
          return json({ items: [current] });
        if (url.endsWith("/connections/conn-1") && init?.method === "PATCH") {
          patch = init;
          const body = JSON.parse(String(init.body));
          current = { ...current, ...body };
          return json(current);
        }
        throw new Error(`unexpected request ${url}`);
      }),
    );
    const user = userEvent.setup();
    render(<ConnectionsPage />);
    await openConnection();

    await screen.findByText("Original");
    await user.click(screen.getByRole("button", { name: /edit/i }));
    expect(screen.getByText(/identity are immutable/i)).toBeInTheDocument();
    const editForm = screen
      .getByRole("heading", { name: /edit connection/i })
      .closest("form");
    if (!editForm) throw new Error("missing edit form");
    const fields = within(editForm);
    expect(fields.queryByLabelText("App ID")).not.toBeInTheDocument();
    await user.clear(fields.getByLabelText("Display Name"));
    await user.type(fields.getByLabelText("Display Name"), "Edited");
    await user.click(
      fields.getByRole("button", { name: /save display name/i }),
    );

    await screen.findByText("Edited");
    await waitFor(() => expect(patch).toBeDefined());
    expect(JSON.parse(String(patch?.body))).toEqual({ display_name: "Edited" });
    expectIdempotencyScope(new Headers(patch?.headers).get("Idempotency-Key") ?? "", "connection-edit");
  });

  it("retries an ordinary connection edit with the exact request and key", async () => {
    const keys: string[] = [];
    const bodies: string[] = [];
    let attempts = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/connections") && (!init?.method || init.method === "GET")) return json({ items: [connection("Original")] });
      if (url.endsWith("/connections/github/setup-options")) return json({ shared_enabled: false });
      if (url.endsWith("/connections/github/manifest-flows") && (!init?.method || init.method === "GET")) return new Response(null, { status: 204 });
      if (url.endsWith("/connections/conn-1") && init?.method === "PATCH") {
        attempts++;
        keys.push(new Headers(init.headers).get("Idempotency-Key") || "");
        bodies.push(String(init.body));
        if (attempts === 1) {
          return new Response(JSON.stringify({ error: { code: "idempotency_pending", message: "Still settling", retryable: true, category: "core", details: {}, correlation_id: "test" } }), { status: 409, headers: { "Content-Type": "application/json" } });
        }
        return json(connection("Edited"));
      }
      throw new Error(`unexpected request ${url}`);
    }));
    const user = userEvent.setup();
    render(<ConnectionsPage />);
    await openConnection();

    await user.click(await screen.findByRole("button", { name: /edit/i }));
    const editForm = screen.getByRole("heading", { name: /edit connection/i }).closest("form");
    if (!editForm) throw new Error("missing edit form");
    await user.clear(within(editForm).getByLabelText("Display Name"));
    await user.type(within(editForm).getByLabelText("Display Name"), "Edited");
    await user.click(within(editForm).getByRole("button", { name: "Save display name" }));

    expect(await within(editForm).findByRole("button", { name: "Retry save display name" })).toBeEnabled();
    expect(within(editForm).getByRole("button", { name: "Cancel" })).toBeDisabled();
    await user.click(within(editForm).getByRole("button", { name: "Retry save display name" }));

    await waitFor(() => expect(attempts).toBe(2));
    expect(keys[1]).toBe(keys[0]);
    expect(bodies[1]).toBe(bodies[0]);
  });

  it("cancels editing without changing metadata", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => json({ items: [connection("Original")] })),
    );
    const user = userEvent.setup();
    render(<ConnectionsPage />);
    await openConnection();
    await user.click(await screen.findByRole("button", { name: /edit/i }));
    const editForm = screen
      .getByRole("heading", { name: /edit connection/i })
      .closest("form");
    if (!editForm) throw new Error("missing edit form");
    await user.clear(within(editForm).getByLabelText("Display Name"));
    await user.type(
      within(editForm).getByLabelText("Display Name"),
      "Discarded",
    );
    await user.click(within(editForm).getByRole("button", { name: /cancel/i }));
    expect(screen.getByText("Original")).toBeInTheDocument();
    expect(screen.queryByText("Discarded")).not.toBeInTheDocument();
  });

  it("keeps a connection whose removal did not finish, with a way to finish it", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        json({
          items: [
            {
              ...connection("Half removed"),
              lifecycle: "removal_pending",
              health_class: "removal_pending",
              credential_state: "missing",
            },
          ],
        }),
      ),
    );
    render(<ConnectionsPage />);
    await openConnection();
    await screen.findByText("Half removed");
    // Removal genuinely did not complete, so hiding the connection would lie.
    expect(screen.getByText(/removal did not finish/i)).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: /retry removal/i }),
    ).toBeInTheDocument();
    // It is on its way out, so nothing else is offered.
    expect(
      screen.queryByRole("button", { name: /edit/i }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /enable/i }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /check/i }),
    ).not.toBeInTheDocument();
  });

  it("drops a removed connection out of the view", async () => {
    let removed = false;
    let deletes = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/connections/conn-1") && init?.method === "DELETE") {
          deletes++;
          removed = true;
          return new Response(null, { status: 204 });
        }
        // The tombstone stays in SQLite for provenance, but the operator
        // listing does not return it.
        if (url.endsWith("/connections"))
          return json({ items: removed ? [] : [connection("Remove me")] });
        throw new Error(`unexpected request ${url}`);
      }),
    );
    const user = userEvent.setup();
    render(<ConnectionsPage />);
    await openConnection();
    await user.click(await screen.findByRole("button", { name: /remove/i }));
    expect(
      await screen.findByRole("dialog", { name: /remove me/i }),
    ).toBeInTheDocument();
    expect(deletes).toBe(0);
    await user.click(screen.getByRole("button", { name: /cancel/i }));
    expect(
      screen.queryByRole("dialog", { name: /remove me/i }),
    ).not.toBeInTheDocument();
    expect(deletes).toBe(0);
    await user.click(screen.getByRole("button", { name: /remove/i }));
    await user.click(
      within(
        screen.getByRole("dialog", { name: /remove me/i }),
      ).getByRole("button", { name: /remove connection/i }),
    );
    await waitFor(() =>
      expect(screen.queryByText("Remove me")).not.toBeInTheDocument(),
    );
    expect(deletes).toBe(1);
    // No reload required: the list refetches after the delete settles.
    expect(
      screen.queryByRole("button", { name: /remove/i }),
    ).not.toBeInTheDocument();
  });

  it("keeps removal failures visible inside the confirmation modal", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/connections/conn-1") && init?.method === "DELETE")
          return new Response(
            JSON.stringify({
              error: {
                code: "internal_error",
                message: "Local key removal failed",
              },
            }),
            { status: 500, headers: { "Content-Type": "application/json" } },
          );
        if (url.endsWith("/connections"))
          return json({ items: [connection("Remove me")] });
        throw new Error(`unexpected request ${url}`);
      }),
    );
    const user = userEvent.setup();
    render(<ConnectionsPage />);
    await openConnection();
    await user.click(await screen.findByRole("button", { name: /remove/i }));
    const dialog = await screen.findByRole("dialog", {
      name: /remove me/i,
    });
    await user.click(
      within(dialog).getByRole("button", { name: /remove connection/i }),
    );
    expect(await within(dialog).findByRole("alert")).toHaveTextContent(
      "Local key removal failed",
    );
  });

  it("reloads connection-specific health state after a failed check", async () => {
    let current: Record<string, unknown> = connection("Health check");
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/health-checks") && init?.method === "POST") {
          current = { ...current, health_class: "revoked" as const };
          return new Response(
            JSON.stringify({
              error: {
                code: "dependency_blocked",
                message: "GitHub App or installation is inaccessible",
                retryable: false,
                category: "core",
                details: {},
                correlation_id: "test",
                occurred_at: "2026-08-13T00:00:00Z",
              },
            }),
            { status: 401, headers: { "Content-Type": "application/json" } },
          );
        }
        if (url.endsWith("/connections")) return json({ items: [current] });
        throw new Error(`unexpected request ${url}`);
      }),
    );
    const user = userEvent.setup();
    render(<ConnectionsPage />);
    await openConnection();
    await user.click(await screen.findByRole("button", { name: /check/i }));
    expect(await screen.findByRole("alert")).toHaveTextContent(
      /GitHub App or installation is inaccessible/i,
    );
    expect(await screen.findByText("Reconnect Required")).toBeInTheDocument();
  });

  it("guides permission mismatch repair with the exact App configuration", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => json({
        items: [{ ...connection("Permission mismatch"), health_class: "permission_mismatch" as const }],
      })),
    );

    render(<ConnectionsPage />);
    await openConnection();

    expect(await screen.findByText(/zero subscribed events and no additional permissions/i)).toBeInTheDocument();
  });

  it("shows the safe GitHub rate-limit reset time after a failed check", async () => {
    const retryAt = "2026-08-14T10:45:00Z";
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/health-checks") && init?.method === "POST") {
          return new Response(
            JSON.stringify({
              error: {
                code: "dependency_blocked",
                message: "GitHub rate limit blocked the connection check",
                retryable: false,
                category: "core",
                details: {},
                correlation_id: "test",
                occurred_at: "2026-08-14T10:40:00Z",
                retry_at: retryAt,
              },
            }),
            { status: 429, headers: { "Content-Type": "application/json" } },
          );
        }
        if (url.endsWith("/connections"))
          return json({ items: [connection("Rate limited")] });
        throw new Error(`unexpected request ${url}`);
      }),
    );
    const user = userEvent.setup();
    render(<ConnectionsPage />);
    await openConnection();

    await user.click(await screen.findByRole("button", { name: /check/i }));
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent(/retry after/i);
    expect(alert.querySelector("time")).toHaveAttribute("datetime", retryAt);
  });

  it("shows host key rotation guidance when a credential is present", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => json({ items: [connection("Rotatable")] })),
    );
    render(<ConnectionsPage />);
    await openConnection();

    await screen.findByText("Rotatable");
    expect(
      screen.getByText(/rotate the private key from the host/i),
    ).toBeInTheDocument();
    expect(
      screen.getByText(
        /kd connections github key rotate --connection-id conn-1 --from \.\/private-key\.pem/i,
      ),
    ).toBeInTheDocument();
    expect(screen.getByText(/run check after rotation/i)).toBeInTheDocument();
  });

  it("shows host key provisioning guidance when a credential is missing", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        json({
          items: [
            {
              ...connection("Needs key"),
              credential_state: "missing" as const,
            },
          ],
        }),
      ),
    );
    render(<ConnectionsPage />);
    await openConnection();

    await screen.findByText("Needs key");
    expect(
      screen.getByText(/provision the private key from the host/i),
    ).toBeInTheDocument();
    expect(
      screen.getByText(
        /kd connections github key provision --connection-id conn-1 --from \.\/private-key\.pem/i,
      ),
    ).toBeInTheDocument();
    expect(
      screen.getByText(/run check after provisioning/i),
    ).toBeInTheDocument();
  });

  it("shows Reconnect without PEM guidance for a shared connection", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const url = String(input);
        if (url.endsWith("/connections"))
          return json({
            items: [
              {
                ...connection("Shared"),
                credential_strategy: "user_access_token",
                credential_state: "present",
              },
            ],
          });
        if (url.endsWith("/connections/github/setup-options"))
          return json({
            shared_enabled: true,
            app_slug: "kingdom",
            install_url: "https://github.com/apps/kingdom/installations/new",
          });
        if (url.endsWith("/connections/github/manifest-flows"))
          return new Response(null, { status: 204 });
        throw new Error(`unexpected request ${url}`);
      }),
    );
    render(<ConnectionsPage />);
    await openConnection();
    await screen.findByText("Shared");
    expect(
      screen.getByRole("button", { name: "Reconnect Connection" }),
    ).toBeInTheDocument();
    expect(
      screen.queryByText(/kd connections github key/i),
    ).not.toBeInTheDocument();
  });

  it("keeps installing on another account reachable while choosing among existing installations", async () => {
    const candidate = (id: string, login: string, scope?: "all" | "selected") => ({
      installation_id: id,
      display_name: login,
      account_id: id,
      account_login: login,
      app_id: "1",
      client_id: "Iv1",
      app_slug: "kingdom",
      ...(scope ? { repository_selection: scope } : {}),
    });
    let refreshed = false;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/connections") && (!init?.method || init.method === "GET"))
          return json({ items: [] });
        if (url.endsWith("/connections/github/setup-options"))
          return json({
            shared_enabled: true,
            app_slug: "kingdom",
            install_url: "https://github.com/apps/kingdom/installations/new",
          });
        if (url.endsWith("/connections/github/manifest-flows"))
          return new Response(null, { status: 204 });
        if (url.endsWith("/connections/github/device-flows") && init?.method === "POST")
          return new Response(
            JSON.stringify({
              flow_id: "flow-choose-123456789",
              // The operator already has one installation, so the flow lands
              // straight on the picker and never passes through needs_installation.
              stage: "selecting_installation",
              expires_at: "2099-08-15T13:00:00Z",
              interval_seconds: 5,
              install_url: "https://github.com/apps/kingdom/installations/new",
              candidates: [candidate("10", "octocat", "selected")],
            }),
            { status: 201, headers: { "Content-Type": "application/json" } },
          );
        if (url.endsWith("/device-flows/flow-choose-123456789/candidates")) {
          refreshed = true;
          return json({
            flow_id: "flow-choose-123456789",
            stage: "selecting_installation",
            expires_at: "2099-08-15T13:00:00Z",
            interval_seconds: 5,
            install_url: "https://github.com/apps/kingdom/installations/new",
            candidates: [
              candidate("10", "octocat", "selected"),
              candidate("11", "acme", "all"),
            ],
          });
        }
        throw new Error(`unexpected request ${url}`);
      }),
    );

    const user = userEvent.setup();
    render(<ConnectionsPage />);
    await startSharedUI(user);
    expect(await screen.findByText("@octocat")).toBeInTheDocument();

    // The reported defect: with one installation present there was no route to
    // install on a second account and no way to re-list without reauthorizing.
    const install = screen.getByRole("link", { name: /Install on Another Account/ });
    expect(install).toHaveAttribute(
      "href",
      "https://github.com/apps/kingdom/installations/new",
    );
    expect(screen.getByText("Selected Repositories")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /Refresh Installations/ }));
    await waitFor(() => expect(refreshed).toBe(true));
    expect(await screen.findByText("@acme")).toBeInTheDocument();
    expect(screen.getByText("All Repositories")).toBeInTheDocument();
  });

  it("links a saved connection to its own repository selection on GitHub", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/connections") && (!init?.method || init.method === "GET"))
          return json({ items: [connection("Kingdom Issues")] });
        if (url.endsWith("/connections/github/setup-options"))
          return json({ shared_enabled: true, app_slug: "kingdom" });
        if (url.endsWith("/connections/github/manifest-flows"))
          return new Response(null, { status: 204 });
        throw new Error(`unexpected request ${url}`);
      }),
    );
    render(<ConnectionsPage />);
    await openConnection();
    await screen.findByText("Kingdom Issues");
    expect(
      screen.getByRole("link", { name: /Manage Repositories on GitHub/ }),
    ).toHaveAttribute(
      "href",
      "https://github.com/apps/kingdom-issues/installations/new/permissions?suggested_target_id=10",
    );
  });

  it("omits the manage repositories link when the installation account is not pinned yet", async () => {
    const unpinned = { ...connection("Pending Check", "2"), app_slug: "", installations: [{ installation_id: "2" }] };
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/connections") && (!init?.method || init.method === "GET"))
          return json({ items: [unpinned] });
        if (url.endsWith("/connections/github/setup-options"))
          return json({ shared_enabled: true, app_slug: "kingdom" });
        if (url.endsWith("/connections/github/manifest-flows"))
          return new Response(null, { status: 204 });
        throw new Error(`unexpected request ${url}`);
      }),
    );
    render(<ConnectionsPage />);
    await openConnection();
    await screen.findByText("Pending Check");
    // Guard against a vacuous pass: the action row must actually be rendered.
    expect(screen.getByRole("button", { name: "Check" })).toBeInTheDocument();
    expect(
      screen.queryByRole("link", { name: /Manage Repositories on GitHub/ }),
    ).not.toBeInTheDocument();
  });

  it("connects both accounts as one connection", async () => {
    const candidate = (id: string, login: string, accountID = id) => ({
      installation_id: id,
      display_name: login,
      account_id: accountID,
      account_login: login,
      app_id: "1",
      client_id: "Iv1",
      app_slug: "kingdom",
    });
    let finalized: Record<string, unknown> | undefined;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/connections") && (!init?.method || init.method === "GET"))
          return json({ items: [] });
        if (url.endsWith("/connections/github/setup-options"))
          return json({ shared_enabled: true, app_slug: "kingdom", install_url: "https://github.com/apps/kingdom/installations/new" });
        if (url.endsWith("/connections/github/manifest-flows"))
          return new Response(null, { status: 204 });
        if (url.endsWith("/connections/github/device-flows") && init?.method === "POST")
          return new Response(
            JSON.stringify({
              flow_id: "flow-multi-123456789",
              stage: "selecting_installation",
              expires_at: "2099-08-15T13:00:00Z",
              interval_seconds: 5,
              install_url: "https://github.com/apps/kingdom/installations/new",
              candidates: [candidate("10", "enkiid"), candidate("11", "T-RexLabs")],
            }),
            { status: 201, headers: { "Content-Type": "application/json" } },
          );
        if (url.endsWith("/finalize")) {
          finalized = JSON.parse(String(init?.body));
          return json(connection("Kingdom Issues"));
        }
        throw new Error(`unexpected request ${url}`);
      }),
    );

    const user = userEvent.setup();
    render(<ConnectionsPage />);
    await startSharedUI(user);

    // Both accounts are tickable at once; neither deselects the other.
    await user.click(await screen.findByRole("checkbox", { name: /@enkiid/ }));
    await user.click(screen.getByRole("checkbox", { name: /@T-RexLabs/ }));
    expect(screen.getByText("2 selected")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /Connect Selected Installations/ }));
    await waitFor(() => expect(finalized).toBeDefined());
    expect(finalized).toMatchObject({ installation_ids: ["10", "11"] });
  });

  it("disables competing shared-flow actions while finalization is active", async () => {
    const candidate = {
      installation_id: "10",
      display_name: "enkiid",
      account_id: "100",
      account_login: "enkiid",
      app_id: "1",
      client_id: "Iv1",
      app_slug: "kingdom",
    };
    let resolveFinalize!: (response: Response) => void;
    const finalize = new Promise<Response>((resolve) => {
      resolveFinalize = resolve;
    });
    let finalizeStarted = false;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/connections") && (!init?.method || init.method === "GET"))
          return json({ items: [] });
        if (url.endsWith("/connections/github/setup-options"))
          return json({ shared_enabled: true, app_slug: "kingdom" });
        if (url.endsWith("/connections/github/manifest-flows"))
          return new Response(null, { status: 204 });
        if (url.endsWith("/connections/github/device-flows") && init?.method === "POST")
          return new Response(JSON.stringify({
            flow_id: "flow-finalize-ui-race",
            stage: "selecting_installation",
            expires_at: "2099-08-15T13:00:00Z",
            interval_seconds: 5,
            install_url: "https://github.com/apps/kingdom/installations/new",
            candidates: [candidate],
          }), { status: 201, headers: { "Content-Type": "application/json" } });
        if (url.endsWith("/device-flows/flow-finalize-ui-race/finalize")) {
          finalizeStarted = true;
          return finalize;
        }
        throw new Error(`unexpected request ${url}`);
      }),
    );

    const user = userEvent.setup();
    render(<ConnectionsPage />);
    await startSharedUI(user);
    await user.click(await screen.findByRole("checkbox", { name: /@enkiid/ }));
    expect(screen.getByRole("link", { name: "Install on Another Account" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Manage Repositories on GitHub" })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Connect Selected Installations" }));
    await waitFor(() => expect(finalizeStarted).toBe(true));

    const dialog = screen.getByRole("dialog", { name: "Connect GitHub" });
    expect(within(dialog).getByRole("checkbox", { name: /@enkiid/ })).toBeDisabled();
    expect(within(dialog).getByRole("button", { name: "Refresh Installations" })).toBeDisabled();
    expect(within(dialog).getByRole("button", { name: "Discard Setup" })).toBeDisabled();
    expect(within(dialog).getByRole("button", { name: "Connect Selected Installations" })).toBeDisabled();
    expect(within(dialog).queryByRole("link", { name: "Install on Another Account" })).not.toBeInTheDocument();
    expect(within(dialog).queryByRole("link", { name: "Manage Repositories on GitHub" })).not.toBeInTheDocument();
    expect(within(dialog).getByText("Install on Another Account")).toHaveAttribute("aria-disabled", "true");
    expect(within(dialog).getByText("Manage Repositories on GitHub")).toHaveAttribute("aria-disabled", "true");

    await act(async () => {
      resolveFinalize(json(connection("Kingdom Issues")));
      await finalize;
    });
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "Connect GitHub" })).not.toBeInTheDocument());
  });

  it("reconnects the exact complete saved installation set without editing it", async () => {
    const shared = {
      ...connection("Shared"),
      credential_strategy: "user_access_token" as const,
      installations: [
        { installation_id: "10", account_id: "100", account_login: "enkiid" },
        { installation_id: "11", account_id: "101", account_login: "T-RexLabs" },
      ],
    };
    const candidate = (id: string, login: string, accountID = id) => ({
      installation_id: id,
      display_name: login,
      account_id: accountID,
      account_login: login,
      app_id: "1",
      client_id: "Iv1",
      app_slug: "kingdom",
    });
    let finalized: Record<string, unknown> | undefined;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/connections") && (!init?.method || init.method === "GET"))
          return json({ items: [shared] });
        if (url.endsWith("/connections/github/setup-options"))
          return json({ shared_enabled: true, app_slug: "kingdom" });
        if (url.endsWith("/connections/github/manifest-flows"))
          return new Response(null, { status: 204 });
        if (url.endsWith("/connections/github/device-flows") && init?.method === "POST")
          return new Response(
            JSON.stringify({
              flow_id: "flow-recon-123456789",
              stage: "selecting_installation",
              reconnect_connection_id: "conn-1",
              expires_at: "2099-08-15T13:00:00Z",
              interval_seconds: 5,
              candidates: [
                candidate("10", "enkiid", "100"),
                candidate("11", "T-RexLabs", "101"),
                candidate("12", "not-part-of-this-connection"),
              ],
            }),
            { status: 201, headers: { "Content-Type": "application/json" } },
          );
        if (url.endsWith("/finalize")) {
          finalized = JSON.parse(String(init?.body));
          return json(shared);
        }
        throw new Error(`unexpected request ${url}`);
      }),
    );

    const user = userEvent.setup();
    render(<ConnectionsPage />);
    await openConnection();
    await screen.findByText("Shared");
    await user.click(screen.getByRole("button", { name: "Reconnect Connection" }));

    const first = await screen.findByRole("checkbox", { name: /@enkiid/ });
    const second = screen.getByRole("checkbox", { name: /@T-RexLabs/ });
    expect(first).toBeChecked();
    expect(first).toBeDisabled();
    expect(second).toBeChecked();
    expect(second).toBeDisabled();
    expect(screen.queryByText("@not-part-of-this-connection")).not.toBeInTheDocument();
    expect(screen.getByText("2 saved")).toBeInTheDocument();
    const reconnectDialog = screen.getByRole("dialog", { name: "Reconnect GitHub" });
    await user.click(within(reconnectDialog).getByRole("button", { name: "Reconnect Connection" }));
    await waitFor(() => expect(finalized).toBeDefined());
    expect(finalized).toMatchObject({ installation_ids: ["10", "11"], display_name: "Shared" });
  });

  it("discovers a resumable device flow without opening it and resumes polling only on request", async () => {
    const shared = {
      ...connection("Shared"),
      credential_strategy: "user_access_token" as const,
    };
    let polls = 0;
    let discarded = false;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/connections") && (!init?.method || init.method === "GET"))
          return json({ items: [shared] });
        if (url.endsWith("/connections/github/setup-options"))
          return json({ shared_enabled: true, app_slug: "kingdom" });
        if (url.endsWith("/connections/github/manifest-flows"))
          return new Response(null, { status: 204 });
        if (url.endsWith("/connections/github/device-flows") && (!init?.method || init.method === "GET"))
          return json({
            flow_id: "flow-resume-123456789",
            stage: "awaiting_authorization",
            reconnect_connection_id: "conn-1",
            user_code: "KEEP-CODE",
            verification_uri: "https://github.com/login/device",
            expires_at: "2099-08-15T13:00:00Z",
            interval_seconds: 5,
            retry_at: "2000-01-01T00:00:00Z",
            candidates: [],
          });
        if (url.endsWith("/device-flows/flow-resume-123456789/poll")) {
          polls += 1;
          return json({
            flow_id: "flow-resume-123456789",
            stage: "needs_installation",
            reconnect_connection_id: "conn-1",
            expires_at: "2099-08-15T13:00:00Z",
            interval_seconds: 5,
            install_url: "https://github.com/apps/kingdom/installations/new",
            candidates: [],
          });
        }
        if (url.endsWith("/device-flows/flow-resume-123456789") && init?.method === "DELETE") {
          discarded = true;
          return new Response(null, { status: 204 });
        }
        throw new Error(`unexpected request ${url}`);
      }),
    );

    const user = userEvent.setup();
    render(<ConnectionsPage />);
    expect(await screen.findByText("GitHub Reconnect in Progress")).toBeInTheDocument();
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(polls).toBe(0);
    expect(screen.getByRole("button", { name: "Add Connection" })).toBeDisabled();
    await openConnection();
    expect(screen.getByRole("button", { name: "Reconnect Connection" })).toBeDisabled();

    await user.click(screen.getByRole("button", { name: "Resume Setup" }));
    expect(await screen.findByRole("dialog", { name: "Reconnect GitHub" })).toBeInTheDocument();
    await waitFor(() => expect(polls).toBe(1));
    await user.click(screen.getByRole("button", { name: "Close" }));
    const resumeSetup = screen.getByRole("button", { name: "Resume Setup" });
    expect(resumeSetup).toBeInTheDocument();
    await waitFor(() => expect(resumeSetup).toHaveFocus());

    await user.click(screen.getByRole("button", { name: "Discard Setup" }));
    await waitFor(() => expect(discarded).toBe(true));
    expect(screen.queryByText("GitHub Reconnect in Progress")).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "All Connections" }));
    expect(screen.getByRole("button", { name: "Add Connection" })).toBeEnabled();
    expect(window.localStorage.length).toBe(0);
    expect(window.sessionStorage.length).toBe(0);
  });

  it("polls a discovered installation discovery until the canonical flow is stable", async () => {
    let statusReads = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/connections") && (!init?.method || init.method === "GET")) return json({ items: [] });
      if (url.endsWith("/connections/github/setup-options")) return json({ shared_enabled: true });
      if (url.endsWith("/connections/github/manifest-flows")) return new Response(null, { status: 204 });
      if (url.endsWith("/connections/github/device-flows") && (!init?.method || init.method === "GET")) {
        return json({
          flow_id: "flow-discovering-12345",
          stage: "discovering_installations",
          expires_at: "2099-08-15T13:00:00Z",
          interval_seconds: 5,
          candidates: [],
        });
      }
      if (url.endsWith("/device-flows/flow-discovering-12345") && (!init?.method || init.method === "GET")) {
        statusReads += 1;
        return json({
          flow_id: "flow-discovering-12345",
          stage: statusReads === 1 ? "discovering_installations" : "selecting_installation",
          expires_at: "2099-08-15T13:00:00Z",
          interval_seconds: 5,
          install_url: "https://github.com/apps/kingdom/installations/new",
          candidates: statusReads === 1 ? [] : [{
            installation_id: "10",
            display_name: "octocat",
            account_id: "100",
            account_login: "octocat",
            app_id: "1",
            client_id: "Iv1",
            app_slug: "kingdom",
          }],
        });
      }
      throw new Error(`unexpected request ${url}`);
    }));
    const user = userEvent.setup();
    render(<ConnectionsPage />);

    expect(await screen.findByText("GitHub Setup in Progress")).toBeInTheDocument();
    await waitFor(() => expect(statusReads).toBe(2));
    await user.click(screen.getByRole("button", { name: "Resume Setup" }));
    expect(await screen.findByText("@octocat")).toBeInTheDocument();
  });

  it("invalidates connections and clears a discovered finalization on completion", async () => {
    let connectionReads = 0;
    let statusReads = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/connections") && (!init?.method || init.method === "GET")) {
        connectionReads += 1;
        return json({ items: connectionReads > 1 ? [connection("Completed")] : [] });
      }
      if (url.endsWith("/connections/github/setup-options")) return json({ shared_enabled: true });
      if (url.endsWith("/connections/github/manifest-flows")) return new Response(null, { status: 204 });
      if (url.endsWith("/connections/github/device-flows") && (!init?.method || init.method === "GET")) {
        return json({
          flow_id: "flow-finalizing-123456",
          stage: "finalizing",
          expires_at: "2099-08-15T13:00:00Z",
          interval_seconds: 5,
          candidates: [],
        });
      }
      if (url.endsWith("/device-flows/flow-finalizing-123456") && (!init?.method || init.method === "GET")) {
        statusReads += 1;
        return json({
          flow_id: "flow-finalizing-123456",
          stage: "completed",
          connection_id: "conn-1",
          expires_at: "2099-08-15T13:00:00Z",
          interval_seconds: 5,
          candidates: [],
        });
      }
      throw new Error(`unexpected request ${url}`);
    }));
    render(<ConnectionsPage />);

    expect(await screen.findByText("Completed")).toBeInTheDocument();
    expect(statusReads).toBe(1);
    expect(connectionReads).toBe(2);
    expect(screen.queryByText("GitHub Setup in Progress")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Add Connection" })).toBeEnabled();
  });

  it("adopts canonical failure after a definitive installation refresh failure", async () => {
    let canonicalRead = false;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/connections") && (!init?.method || init.method === "GET")) return json({ items: [] });
      if (url.endsWith("/connections/github/setup-options")) return json({ shared_enabled: true });
      if (url.endsWith("/connections/github/manifest-flows")) return new Response(null, { status: 204 });
      if (url.endsWith("/connections/github/device-flows") && init?.method === "POST") {
        return new Response(JSON.stringify({
          flow_id: "flow-refresh-failed-1",
          stage: "selecting_installation",
          expires_at: "2099-08-15T13:00:00Z",
          interval_seconds: 5,
          install_url: "https://github.com/apps/kingdom/installations/new",
          candidates: [{
            installation_id: "10",
            display_name: "octocat",
            account_id: "100",
            account_login: "octocat",
            app_id: "1",
            client_id: "Iv1",
            app_slug: "kingdom",
          }],
        }), { status: 201, headers: { "Content-Type": "application/json" } });
      }
      if (url.endsWith("/device-flows/flow-refresh-failed-1/candidates")) {
        return new Response(JSON.stringify({ error: { code: "dependency_blocked", message: "GitHub installations could not be refreshed" } }), {
          status: 503,
          headers: { "Content-Type": "application/json" },
        });
      }
      if (url.endsWith("/device-flows/flow-refresh-failed-1") && (!init?.method || init.method === "GET")) {
        canonicalRead = true;
        return json({
          flow_id: "flow-refresh-failed-1",
          stage: "failed",
          expires_at: "2099-08-15T13:00:00Z",
          interval_seconds: 5,
          candidates: [],
          failure: "GitHub installations could not be refreshed. Reconnect to continue.",
        });
      }
      throw new Error(`unexpected request ${url}`);
    }));
    const user = userEvent.setup();
    render(<ConnectionsPage />);
    await startSharedUI(user);

    await user.click(await screen.findByRole("button", { name: "Refresh Installations" }));
    expect(await screen.findByText("GitHub installations could not be refreshed. Reconnect to continue.")).toBeInTheDocument();
    expect(canonicalRead).toBe(true);
    expect(screen.queryByRole("button", { name: "Refresh Installations" })).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Install on Another Account" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Start Again" })).toBeInTheDocument();
  });

  it("blocks reconnect when a saved installation is missing and points to Change", async () => {
    const shared = {
      ...connection("Shared"),
      credential_strategy: "user_access_token" as const,
      installations: [
        { installation_id: "10", account_id: "100", account_login: "enkiid" },
        { installation_id: "11", account_id: "101", account_login: "T-RexLabs" },
      ],
    };
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/connections") && (!init?.method || init.method === "GET")) return json({ items: [shared] });
        if (url.endsWith("/connections/github/setup-options")) return json({ shared_enabled: true, app_slug: "kingdom" });
        if (url.endsWith("/connections/github/manifest-flows")) return new Response(null, { status: 204 });
        if (url.endsWith("/connections/github/device-flows") && init?.method === "POST")
          return new Response(JSON.stringify({
            flow_id: "flow-missing-12345678",
            stage: "selecting_installation",
            reconnect_connection_id: "conn-1",
            expires_at: "2099-08-15T13:00:00Z",
            interval_seconds: 5,
            candidates: [{ installation_id: "10", display_name: "enkiid", account_id: "100", account_login: "enkiid", app_id: "1", client_id: "Iv1", app_slug: "kingdom" }],
          }), { status: 201, headers: { "Content-Type": "application/json" } });
        throw new Error(`unexpected request ${url}`);
      }),
    );

    const user = userEvent.setup();
    render(<ConnectionsPage />);
    await openConnection();
    await screen.findByText("Shared");
    await user.click(screen.getByRole("button", { name: "Reconnect Connection" }));
    expect(await screen.findByText("Some Saved Installations Are Missing")).toBeInTheDocument();
    expect(screen.getAllByText(/@T-RexLabs/).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/use Change/i).length).toBeGreaterThan(1);
    expect(within(screen.getByRole("dialog", { name: "Reconnect GitHub" })).getByRole("button", { name: "Reconnect Connection" })).toBeDisabled();
  });

  it("adds a second account to a saved connection", async () => {
    let saved: Record<string, unknown> | undefined;
    let current = connection("Kingdom Issues");
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/connections") && (!init?.method || init.method === "GET"))
          return json({ items: [current] });
        if (url.endsWith("/connections/github/setup-options"))
          return json({ shared_enabled: true, app_slug: "kingdom" });
        if (url.endsWith("/connections/github/manifest-flows"))
          return new Response(null, { status: 204 });
        if (url.endsWith("/installation-candidates"))
          return json({
            items: [
              { installation_id: "2", display_name: "octocat", account_id: "10", account_login: "octocat", app_id: "1", client_id: "Iv1", app_slug: "kingdom-issues", connected: true },
              { installation_id: "11", display_name: "T-RexLabs", account_id: "101", account_login: "T-RexLabs", app_id: "1", client_id: "Iv1", app_slug: "kingdom-issues", connected: false },
            ],
          });
        if (url.endsWith("/installations") && init?.method === "PUT") {
          saved = JSON.parse(String(init.body));
          current = {
            ...current,
            health_class: "never_checked",
            installations: [
              ...current.installations,
              { installation_id: "11", account_id: "101", account_login: "T-RexLabs" },
            ],
            updated_at: "2026-08-24T11:00:00Z",
          };
          return json(current);
        }
        throw new Error(`unexpected request ${url}`);
      }),
    );

    const user = userEvent.setup();
    render(<ConnectionsPage />);
    await openConnection();
    await screen.findByText("Kingdom Issues");
    await user.click(screen.getByRole("button", { name: "Change" }));

    // What the connection already covers arrives already ticked.
    expect(await screen.findByRole("checkbox", { name: /@octocat/ })).toBeChecked();
    await user.click(screen.getByRole("checkbox", { name: /@T-RexLabs/ }));
    await user.click(screen.getByRole("button", { name: /Save Installations/ }));
    await waitFor(() => expect(saved).toBeDefined());
    expect(saved).toEqual({ installation_ids: ["11", "2"] });
    expect(await screen.findByText("Not Checked")).toBeInTheDocument();
  });
});
