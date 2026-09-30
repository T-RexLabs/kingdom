import { type ReactNode, useEffect, useLayoutEffect, useRef, useState } from "react";
import { Laptop, RefreshCw, ShieldCheck, WifiOff } from "lucide-react";
import { useQueryClient } from "@tanstack/react-query";
import { API_BASE, setCSRFToken, subscribeToUnauthorized } from "@/api/client";
import type { components } from "@/api/schema";
import { useNotifications } from "@/components/notifications";

type AuthSession = components["schemas"]["AuthSession"];

type BoundaryState =
  | { status: "bootstrapping"; recovery: boolean }
  | { status: "ready" }
  | { status: "unsupported" }
  | { status: "unavailable"; message: string };

class SessionBootstrapError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

let bootstrapRequest: Promise<AuthSession> | null = null;
let recoveryNotificationSequence = 0;

function requestSession(): Promise<AuthSession> {
  return fetch(`${API_BASE}/auth/session`, {
    method: "POST",
    cache: "no-store",
    credentials: "same-origin",
    headers: { "X-Kingdom-Browser-Bootstrap": "1" },
  }).then(async (response) => {
    if (!response.ok) {
      const payload = await response.json().catch(() => null) as { error?: { message?: string } } | null;
      throw new SessionBootstrapError(response.status, payload?.error?.message ?? "Kingdom could not establish a local browser session.");
    }
    return response.json() as Promise<AuthSession>;
  });
}

function bootstrapSession(): Promise<AuthSession> {
  if (bootstrapRequest) return bootstrapRequest;
  const request = () => requestSession();
  const coordinated = navigator.locks
    ? navigator.locks.request<Promise<AuthSession>>(`kingdom:browser-session:${API_BASE}`, request).then((session) => session)
    : request();
  const pending = coordinated.finally(() => {
    bootstrapRequest = null;
  });
  bootstrapRequest = pending;
  return pending;
}

function wait(milliseconds: number): Promise<void> {
  return new Promise((resolve) => window.setTimeout(resolve, milliseconds));
}

export function LocalSessionBoundary({ children }: { children: ReactNode }) {
  const queryClient = useQueryClient();
  const { addNotification } = useNotifications();
  const [state, setState] = useState<BoundaryState>({ status: "bootstrapping", recovery: false });
  const [bootstrapAttempt, setBootstrapAttempt] = useState(0);
  const pendingMutationFailure = useRef(false);
  const recoveryAttempt = useRef(false);
  const failureHeading = useRef<HTMLHeadingElement>(null);

  useEffect(() => subscribeToUnauthorized((event) => {
    pendingMutationFailure.current ||= event.source === "mutation";
    recoveryAttempt.current = true;
    void queryClient.cancelQueries();
    setState({ status: "bootstrapping", recovery: true });
    setBootstrapAttempt((attempt) => attempt + 1);
  }), [queryClient]);

  useEffect(() => {
    let current = true;
    const recover = recoveryAttempt.current;
    async function establishSession() {
      const delays = [0, 250, 1_000];
      for (let index = 0; index < delays.length; index += 1) {
        if (delays[index] > 0) await wait(delays[index]);
        if (!current) return;
        try {
          const session = await bootstrapSession();
          if (!current) return;
          setCSRFToken(session.csrf_token, session.session_id);
          await queryClient.invalidateQueries();
          if (!current) return;
          if (pendingMutationFailure.current) {
            pendingMutationFailure.current = false;
            recoveryNotificationSequence += 1;
            addNotification({
              id: `session-recovery-${recoveryNotificationSequence}`,
              status: "info",
              title: "Kingdom reconnected",
              message: "Your previous action was not repeated. Review the current state before trying again.",
              autoDismissMs: 8_000,
            });
          }
          recoveryAttempt.current = false;
          setState({ status: "ready" });
          return;
        } catch (reason) {
          if (!current) return;
          if (reason instanceof SessionBootstrapError && reason.status === 403) {
            setState({ status: "unsupported" });
            return;
          }
          if (reason instanceof SessionBootstrapError && reason.status === 429) {
            setState({ status: "unavailable", message: "Kingdom has reached its local browser-session limit. Try again after an existing session expires." });
            return;
          }
          if (index === delays.length - 1) {
            setState({
              status: "unavailable",
              message: recover
                ? "Kingdom could not restore the local browser session. The runtime may still be restarting."
                : "Kingdom could not establish a local browser session. Confirm that the runtime is running on this machine.",
            });
          }
        }
      }
    }
    void establishSession();
    return () => {
      current = false;
    };
  }, [addNotification, bootstrapAttempt, queryClient]);

  useLayoutEffect(() => {
    if (state.status === "unsupported" || state.status === "unavailable") {
      failureHeading.current?.focus();
    }
  }, [state.status]);

  if (state.status === "ready") return children;

  if (state.status === "bootstrapping") {
    return (
      <main className="grid min-h-dvh place-items-center bg-background px-5 text-foreground">
        <div role="status" aria-live="polite" className="flex items-center gap-3 text-sm text-foreground-muted">
          <span className="grid h-9 w-9 place-items-center border border-accent/35 bg-accent/10 text-accent">
            <ShieldCheck aria-hidden="true" className="h-4 w-4 motion-safe:animate-pulse" />
          </span>
          <span>{state.recovery ? "Restoring local session..." : "Opening Kingdom..."}</span>
        </div>
      </main>
    );
  }

  const unsupported = state.status === "unsupported";
  return (
    <main className="relative grid min-h-dvh place-items-center overflow-hidden bg-background px-4 py-10 text-foreground sm:px-6">
      <div aria-hidden="true" className="absolute inset-0 opacity-40 [background-image:radial-gradient(circle_at_18%_12%,hsl(var(--accent)/.16),transparent_34%),linear-gradient(115deg,transparent_35%,hsl(var(--border)/.32)_35.2%,transparent_35.5%)]" />
      <section aria-labelledby="local-session-title" className="relative w-full max-w-xl border border-border bg-surface/95 shadow-2xl shadow-black/40">
        <div className="flex items-center justify-between border-b border-border px-5 py-4 sm:px-7 sm:py-5">
          <div className="flex items-center gap-3">
            <span className="grid h-11 w-11 place-items-center border border-accent/40 bg-accent/10 text-accent">
              {unsupported ? <Laptop aria-hidden="true" className="h-5 w-5" /> : <WifiOff aria-hidden="true" className="h-5 w-5" />}
            </span>
            <div>
              <p className="font-display text-lg font-bold tracking-wide text-accent">Kingdom Local</p>
              <p className="text-[10px] uppercase tracking-[0.24em] text-foreground-subtle">Host-bound control plane</p>
            </div>
          </div>
          <span className="h-px w-12 bg-gradient-to-r from-accent/50 to-transparent sm:w-20" />
        </div>

        <div className="p-6 sm:p-8">
          <h1 id="local-session-title" ref={failureHeading} tabIndex={-1} className="font-display text-2xl font-semibold outline-none sm:text-3xl">
            {unsupported ? "Open Kingdom on its host machine" : "The local runtime is unavailable"}
          </h1>
          <p className="mt-3 max-w-lg text-sm leading-6 text-foreground-muted sm:text-base sm:leading-7">
            {unsupported
              ? "This browser is not connecting through the machine where Kingdom is running. Open the runtime there using localhost or its loopback address."
              : state.message}
          </p>

          {unsupported ? (
            <div className="mt-6 border-l-2 border-accent/50 bg-background/60 px-4 py-3 text-sm leading-6 text-foreground-muted">
              Remote automation can use the authenticated HTTPS API with an explicitly supplied operator credential.
            </div>
          ) : (
            <button
              type="button"
              onClick={() => {
                setState({ status: "bootstrapping", recovery: false });
                setBootstrapAttempt((attempt) => attempt + 1);
              }}
              className="mt-7 inline-flex min-h-11 items-center gap-2 border border-accent/60 bg-accent px-5 py-3 text-sm font-semibold text-background outline-none transition hover:bg-accent/90 focus-visible:ring-2 focus-visible:ring-accent focus-visible:ring-offset-2 focus-visible:ring-offset-background"
            >
              <RefreshCw aria-hidden="true" className="h-4 w-4" />
              Try again
            </button>
          )}
        </div>
      </section>
    </main>
  );
}
