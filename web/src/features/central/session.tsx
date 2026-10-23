import { useEffect, useRef, useState, type ReactNode } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { setCSRFToken, subscribeToUnauthorized } from "@/api/client";
import { Button } from "@/components/ui/button";

import { SessionContext, type CentralSession } from "./session-context";

type State = { kind: "loading" } | { kind: "ready"; session: CentralSession } | { kind: "signed-out"; providers: string[] } | { kind: "failed" };
export function HostedSessionBoundary({ children }: { children: ReactNode }) {
  const queryClient = useQueryClient();
  const [state, setState] = useState<State>({ kind: "loading" });
  const [attempt, setAttempt] = useState(0);
  const heading = useRef<HTMLHeadingElement>(null);
  useEffect(() => subscribeToUnauthorized(() => {
    setCSRFToken("");
    void queryClient.cancelQueries();
    queryClient.clear();
    setState({ kind: "loading" });
    setAttempt(value => value + 1);
  }), [queryClient]);
  useEffect(() => {
    const controller = new AbortController();
    async function establish() {
      try {
        const response = await fetch("/api/central/v1/session", { credentials: "same-origin", cache: "no-store", signal: controller.signal });
        if (response.status === 401) {
          const providers = await fetch("/auth/providers", { credentials: "same-origin", cache: "no-store", signal: controller.signal });
          if (!providers.ok) throw new Error("Sign-in is unavailable.");
          const body = await providers.json() as { providers: string[] };
          if (!controller.signal.aborted) setState({ kind: "signed-out", providers: body.providers.filter(value => value === "oidc" || value === "github") });
          return;
        }
        if (!response.ok) throw new Error("Session is unavailable.");
        const session = await response.json() as CentralSession;
        if (!session.session_id || !session.csrf_token || !session.user?.id) throw new Error("Session is invalid.");
        if (!controller.signal.aborted) {
          setCSRFToken(session.csrf_token, session.session_id);
          setState({ kind: "ready", session });
        }
      } catch {
        if (!controller.signal.aborted) setState({ kind: "failed" });
      }
    }
    void establish();
    return () => controller.abort();
  }, [attempt]);
  useEffect(() => { if (state.kind === "failed" || state.kind === "signed-out") heading.current?.focus(); }, [state.kind]);
  if (state.kind === "ready") return <SessionContext value={state.session}>{children}</SessionContext>;
  return <main className="grid min-h-dvh place-items-center bg-background px-5 text-foreground">
    <section className="ui-card w-full max-w-md rounded-lg border border-border bg-surface p-8">
      <p className="mb-8 font-display text-2xl text-accent">Kingdom</p>
      {state.kind === "loading" ? <p role="status" className="text-sm text-foreground-muted">Checking your session…</p> : <>
        <h1 ref={heading} tabIndex={-1} className="text-xl font-medium">{state.kind === "failed" ? "Connection Unavailable" : "Sign In"}</h1>
        <p className="mb-6 mt-2 text-sm leading-relaxed text-foreground-muted">{state.kind === "failed" ? "Kingdom could not check your session. Check your connection and try again." : "Sign in to open your projects and use your team’s model connections."}</p>
        {state.kind === "failed" ? <Button onClick={() => { setState({ kind: "loading" }); setAttempt(value => value + 1); }}>Try Again</Button> : <div className="space-y-3">
          {state.providers.map(provider => <a key={provider} href={`/auth/${provider}/start`} className="ui-button ui-button--primary block rounded-md border border-accent bg-accent px-4 py-2 text-center text-sm font-medium text-background">{provider === "github" ? "Continue with GitHub" : "Continue with Your Organization"}</a>)}
          {state.providers.length === 0 && <p role="alert" className="text-sm">No sign-in provider is available. Contact your Kingdom operator.</p>}
        </div>}
      </>}
    </section>
  </main>;
}
