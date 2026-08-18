import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ConnectionsPage } from "./connections-page";

const connection = (displayName: string, appID = "1") => ({
  connection_id: "conn-1",
  provider: "github" as const,
  display_name: displayName,
  app_id: appID,
  client_id: "Iv1",
  installation_id: "2",
  credential_strategy: "app_private_key" as const,
  app_slug: "kingdom-issues",
  installation_account_id: "10",
  installation_account_login: "octocat",
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
    await user.click(
      await screen.findByRole("button", { name: "Connect GitHub" }),
    );
    const dialog = await screen.findByRole("dialog", {
      name: "Connect GitHub",
    });
    expect(within(dialog).getByText("ABCD-EFGH")).toBeInTheDocument();
    expect(
      within(dialog).getByRole("link", { name: /open github/i }),
    ).toHaveAttribute("href", "https://github.com/login/device");
    expect(
      screen.getByRole("button", { name: "Create a private GitHub App" }),
    ).toBeInTheDocument();
    expect(window.localStorage.length).toBe(0);
    expect(window.sessionStorage.length).toBe(0);
    expectIdempotencyScope(startIdempotencyKey, "device-start");
  });

  it("ignores a late poll failure from a replaced device flow", async () => {
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
    await user.click(await screen.findByRole("button", { name: "Connect GitHub" }));
    expect(await screen.findByText("OLD1-CODE")).toBeInTheDocument();
    await waitFor(() => expect(pollRequested).toBe(true));
    await user.click(screen.getByRole("button", { name: "Close dialog" }));
    await user.click(screen.getByRole("button", { name: "Connect GitHub" }));
    expect(await screen.findByText("NEW2-CODE")).toBeInTheDocument();

    await act(async () => {
      resolveOldPoll(new Response(JSON.stringify({ error: { code: "dependency_blocked", message: "old flow expired" } }), {
        status: 409,
        headers: { "Content-Type": "application/json" },
      }));
      await oldPoll;
    });
    await waitFor(() => expect(screen.queryByText("old flow expired")).not.toBeInTheDocument());
    expect(screen.getByText("NEW2-CODE")).toBeInTheDocument();
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

    await user.click(await screen.findByRole("button", { name: "Connect GitHub" }));

    expect(
      await screen.findByText("GitHub authorization could not be completed. Start again."),
    ).toBeInTheDocument();
    expect(screen.queryByText("FAIL-CODE")).not.toBeInTheDocument();
    expect(screen.queryByText(/waiting safely/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/provider diagnostic/i)).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Start again" })).toBeInTheDocument();
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

    await user.click(await screen.findByRole("button", { name: "Connect GitHub" }));
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

    await user.click(await screen.findByRole("button", { name: "Connect GitHub" }));
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

    await user.click(await screen.findByRole("button", { name: "Connect GitHub" }));
    await user.click(await screen.findByRole("button", { name: "Start again" }));

    expect(await screen.findByText("NEW2-CODE")).toBeInTheDocument();
    expect(screen.queryByText("OLD1-CODE")).not.toBeInTheDocument();
    expect(startKeys).toHaveLength(2);
    expect(startKeys[1]).not.toBe(startKeys[0]);
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
    await user.click(
      await screen.findByRole("button", {
        name: "Create a private GitHub App",
      }),
    );
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
    await user.click(
      await screen.findByRole("button", { name: /use manual setup/i }),
    );
    const dialog = await screen.findByRole("dialog", {
      name: /manual github app setup/i,
    });
    for (const [label, value] of [
      ["Display name", "Manual"],
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

    await user.click(await screen.findByRole("button", { name: "Create a private GitHub App" }));
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("Private App setup could not start");
    expect(alert).toHaveTextContent("Bad Gateway");
    await user.click(within(alert).getByRole("button", { name: "Retry this step" }));

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
    expect(screen.queryByRole("dialog", { name: "Create a private GitHub App" })).not.toBeInTheDocument();
    await user.click(within(alert).getByRole("button", { name: "Retry this step" }));
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

  it("never auto-opens private setup and lets an unfinished flow be resumed or abandoned", async () => {
    let discardMethod = "";
    let discardKey = "";
    let currentFlowRequests = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/connections") && (!init?.method || init.method === "GET")) return json({ items: [] });
      if (url.endsWith("/connections/github/setup-options")) return json({ shared_enabled: true });
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
    expect(screen.queryByRole("dialog", { name: "Create a private GitHub App" })).not.toBeInTheDocument();
    expect(currentFlowRequests).toBe(1);

    await user.click(screen.getByRole("button", { name: "Resume setup" }));
    const dialog = await screen.findByRole("dialog", { name: "Create a private GitHub App" });

    await user.click(within(dialog).getByRole("button", { name: "Abandon setup" }));
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "Create a private GitHub App" })).not.toBeInTheDocument());
    expect(discardMethod).toBe("DELETE");
    expectIdempotencyScope(discardKey, "manifest-discard");
    expect(screen.queryByText("Finish creating the private App on GitHub.")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Resume setup" })).not.toBeInTheDocument();
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
    expect(screen.queryByRole("dialog", { name: "Create a private GitHub App" })).not.toBeInTheDocument();
    await user.click(within(alert).getByRole("button", { name: "Retry this step" }));

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
    await user.click(within(alert).getByRole("button", { name: "Start again" }));

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

    await screen.findByText("Original");
    await user.click(screen.getByRole("button", { name: /edit/i }));
    expect(screen.getByText(/identity are immutable/i)).toBeInTheDocument();
    const editForm = screen
      .getByRole("heading", { name: /edit connection/i })
      .closest("form");
    if (!editForm) throw new Error("missing edit form");
    const fields = within(editForm);
    expect(fields.queryByLabelText("App ID")).not.toBeInTheDocument();
    await user.clear(fields.getByLabelText("Display name"));
    await user.type(fields.getByLabelText("Display name"), "Edited");
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

    await user.click(await screen.findByRole("button", { name: /edit/i }));
    const editForm = screen.getByRole("heading", { name: /edit connection/i }).closest("form");
    if (!editForm) throw new Error("missing edit form");
    await user.clear(within(editForm).getByLabelText("Display name"));
    await user.type(within(editForm).getByLabelText("Display name"), "Edited");
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
    await user.click(await screen.findByRole("button", { name: /edit/i }));
    const editForm = screen
      .getByRole("heading", { name: /edit connection/i })
      .closest("form");
    if (!editForm) throw new Error("missing edit form");
    await user.clear(within(editForm).getByLabelText("Display name"));
    await user.type(
      within(editForm).getByLabelText("Display name"),
      "Discarded",
    );
    await user.click(within(editForm).getByRole("button", { name: /cancel/i }));
    expect(screen.getByText("Original")).toBeInTheDocument();
    expect(screen.queryByText("Discarded")).not.toBeInTheDocument();
  });

  it("renders removed connections as read-only tombstones", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        json({
          items: [
            {
              ...connection("Removed"),
              lifecycle: "removed",
              health_class: "removed",
              credential_state: "missing",
            },
          ],
        }),
      ),
    );
    render(<ConnectionsPage />);
    await screen.findByText("Removed");
    expect(screen.getByText(/read-only tombstone/i)).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /edit/i }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /enable/i }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /remove/i }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByText(/kd connections github key provision/i),
    ).not.toBeInTheDocument();
  });

  it("reloads a removed connection into a read-only tombstone", async () => {
    let current: Record<string, unknown> = connection("Remove me");
    let deletes = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = String(input);
        if (url.endsWith("/connections/conn-1") && init?.method === "DELETE") {
          deletes++;
          current = {
            ...current,
            lifecycle: "removed" as const,
            health_class: "removed" as const,
            credential_state: "missing" as const,
          };
          return new Response(null, { status: 204 });
        }
        if (url.endsWith("/connections")) return json({ items: [current] });
        throw new Error(`unexpected request ${url}`);
      }),
    );
    const user = userEvent.setup();
    render(<ConnectionsPage />);
    await user.click(await screen.findByRole("button", { name: /remove/i }));
    expect(
      await screen.findByRole("dialog", { name: /remove github connection/i }),
    ).toBeInTheDocument();
    expect(deletes).toBe(0);
    await user.click(screen.getByRole("button", { name: /cancel/i }));
    expect(
      screen.queryByRole("dialog", { name: /remove github connection/i }),
    ).not.toBeInTheDocument();
    expect(deletes).toBe(0);
    await user.click(screen.getByRole("button", { name: /remove/i }));
    await user.click(
      within(
        screen.getByRole("dialog", { name: /remove github connection/i }),
      ).getByRole("button", { name: /remove connection/i }),
    );
    await screen.findByText(/read-only tombstone/i);
    expect(deletes).toBe(1);
    expect(
      screen.queryByRole("button", { name: /enable/i }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByText(/kd connections github key provision/i),
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
    await user.click(await screen.findByRole("button", { name: /remove/i }));
    const dialog = await screen.findByRole("dialog", {
      name: /remove github connection/i,
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
    await user.click(await screen.findByRole("button", { name: /check/i }));
    expect(await screen.findByRole("alert")).toHaveTextContent(
      /GitHub App or installation is inaccessible/i,
    );
    expect(await screen.findByText("revoked")).toBeInTheDocument();
  });

  it("guides permission mismatch repair with the exact App configuration", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => json({
        items: [{ ...connection("Permission mismatch"), health_class: "permission_mismatch" as const }],
      })),
    );

    render(<ConnectionsPage />);

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
    await screen.findByText("Shared");
    expect(
      screen.getByRole("button", { name: "Reconnect" }),
    ).toBeInTheDocument();
    expect(
      screen.queryByText(/kd connections github key/i),
    ).not.toBeInTheDocument();
  });
});
