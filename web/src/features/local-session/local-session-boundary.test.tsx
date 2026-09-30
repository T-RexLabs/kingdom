import { StrictMode, useEffect, useState, type ReactNode } from "react";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { API_BASE, apiFetch, setCSRFToken } from "@/api/client";
import { NotificationProvider, NotificationStack } from "@/components/notifications";
import { subscribeSSE } from "@/lib/sse-stream";
import { LocalSessionBoundary } from "./local-session-boundary";

function renderBoundary(children: ReactNode, queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } }), strict = false) {
  const content = (
    <QueryClientProvider client={queryClient}>
      <NotificationProvider>
        <LocalSessionBoundary>{children}</LocalSessionBoundary>
        <NotificationStack />
      </NotificationProvider>
    </QueryClientProvider>
  );
  return { queryClient, ...render(strict ? <StrictMode>{content}</StrictMode> : content) };
}

function sessionResponse(csrf = "csrf-session"): Response {
  return new Response(JSON.stringify({
    session_id: `binding-${csrf}`,
    actor_id: "local-operator",
    role_id: "operator",
    csrf_token: csrf,
    expires_at: "2026-08-18T00:00:00Z",
  }), { status: 201, headers: { "Content-Type": "application/json" } });
}

function ProtectedApplication() {
  const [status, setStatus] = useState("");
  async function load() {
    try {
      const result = await apiFetch<{ status: string }>("/protected", { method: "POST" });
      setStatus(result.status);
    } catch {
      // The session boundary owns authentication recovery.
    }
  }
  return <div>Kingdom application {status}<button type="button" onClick={() => void load()}>Load protected data</button></div>;
}

function SSEApplication() {
  useEffect(() => {
    const controller = new AbortController();
    void subscribeSSE({
      url: `${API_BASE}/events/stream?run_id=run-1&follow=true`,
      buildHeaders: () => ({}),
      onEvent: () => true,
      onConnectionChange: () => {},
      isTerminalEventType: (type) => type === "done",
      signal: controller.signal,
      sleep: async () => {},
    });
    return () => controller.abort();
  }, []);
  return <div>Kingdom SSE application</div>;
}

function completedStream(): Response {
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(new TextEncoder().encode("event: done\ndata: {}\n\n"));
      controller.close();
    },
  });
  return new Response(body, { status: 200, headers: { "Content-Type": "text/event-stream" } });
}

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  setCSRFToken("");
  localStorage.clear();
  sessionStorage.clear();
});

describe("LocalSessionBoundary", () => {
  it("opens the local application without an authentication ceremony", async () => {
    const storageWrite = vi.spyOn(Storage.prototype, "setItem");
    const fetchMock = vi.fn().mockResolvedValue(sessionResponse("csrf-memory-only"));
    vi.stubGlobal("fetch", fetchMock);

    renderBoundary(<div>Kingdom application</div>);

    expect(await screen.findByText("Kingdom application")).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledWith("/api/v1/auth/session", expect.objectContaining({
      method: "POST",
      credentials: "same-origin",
      headers: { "X-Kingdom-Browser-Bootstrap": "1" },
    }));
    expect(storageWrite).not.toHaveBeenCalled();
    expect(JSON.stringify({ ...localStorage, ...sessionStorage })).not.toContain("csrf-memory-only");
    expect(screen.queryByRole("textbox")).not.toBeInTheDocument();
  });

  it("coordinates bootstrap across same-origin browser tabs", async () => {
    const requestLock = vi.fn(async (_name: string, callback: () => Promise<unknown>) => callback());
    vi.stubGlobal("navigator", { locks: { request: requestLock } });
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(sessionResponse()));

    renderBoundary(<div>Kingdom application</div>);

    expect(await screen.findByText("Kingdom application")).toBeInTheDocument();
    expect(requestLock).toHaveBeenCalledWith("kingdom:browser-session:/api/v1", expect.any(Function));
  });

  it("shows a stable local-only state when bootstrap is forbidden", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({ error: { message: "forbidden" } }), { status: 403, headers: { "Content-Type": "application/json" } })));

    renderBoundary(<div>Kingdom application</div>, undefined, true);

    expect(await screen.findByRole("heading", { name: "Open Kingdom on its host machine" })).toHaveFocus();
    expect(screen.getByText(/localhost or its loopback address/i)).toBeInTheDocument();
    expect(screen.queryByText("Kingdom application")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /try again/i })).not.toBeInTheDocument();
  });

  it("recovers once from a mutation 401 without replaying the mutation", async () => {
    let sessionRequests = 0;
    let protectedRequests = 0;
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/auth/session")) {
        sessionRequests += 1;
        return sessionResponse(`csrf-${sessionRequests}`);
      }
      if (url.endsWith("/protected")) {
        protectedRequests += 1;
        return protectedRequests === 1
          ? new Response(null, { status: 401, statusText: "Unauthorized" })
          : new Response(JSON.stringify({ status: "ready" }), { status: 200, headers: { "Content-Type": "application/json" } });
      }
      throw new Error(`unexpected request ${url}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    const user = userEvent.setup();

    renderBoundary(<ProtectedApplication />);
    await screen.findByText(/Kingdom application/);
    await user.click(screen.getByRole("button", { name: "Load protected data" }));

    await waitFor(() => expect(sessionRequests).toBe(2));
    expect(protectedRequests).toBe(1);
    expect(await screen.findByText("Kingdom reconnected")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Load protected data" }));
    expect(await screen.findByText(/Kingdom application ready/)).toBeInTheDocument();
    expect(protectedRequests).toBe(2);
  });

  it("coalesces an SSE 401 into one transparent session recovery", async () => {
    let sessionRequests = 0;
    let streamRequests = 0;
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/auth/session")) {
        sessionRequests += 1;
        return sessionResponse(`csrf-${sessionRequests}`);
      }
      if (url.includes("/events/stream")) {
        streamRequests += 1;
        return streamRequests === 1
          ? new Response(null, { status: 401, statusText: "Unauthorized" })
          : completedStream();
      }
      throw new Error(`unexpected request ${url}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    renderBoundary(<SSEApplication />);

    await waitFor(() => expect(sessionRequests).toBe(2));
    expect(await screen.findByText("Kingdom SSE application")).toBeInTheDocument();
    await waitFor(() => expect(streamRequests).toBe(2));
  });

  it("shares one in-flight bootstrap across StrictMode remounts", async () => {
    let resolveBootstrap!: (response: Response) => void;
    const fetchMock = vi.fn().mockImplementation(() => new Promise<Response>((resolve) => {
      resolveBootstrap = resolve;
    }));
    vi.stubGlobal("fetch", fetchMock);

    renderBoundary(<div>Kingdom application</div>, undefined, true);
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    resolveBootstrap(sessionResponse());

    expect(await screen.findByText("Kingdom application")).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});
