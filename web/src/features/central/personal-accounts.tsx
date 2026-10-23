import { useCallback, useEffect, useRef, useState, type FormEvent } from "react";
import { useQuery } from "@tanstack/react-query";
import { ApiError, centralFetch } from "@/api/client";
import { Button } from "@/components/ui/button";
import { fieldClass, fieldLabelClass } from "@/components/ui/field-styles";

export type PersonalProvider = "github-copilot" | "openai";
type SignIn = {
  id: string;
  provider: PersonalProvider;
  status: "starting" | "pending" | "publishing" | "cleanup" | "canceling" | "completing" | "complete" | "failed" | "expired" | "canceled" | "interrupted";
  expires_at: string;
  url?: string;
  instructions?: string;
  pairing_code?: string;
  connection?: { id: string };
};
type StartRequest = { key: string; body: string };
const pendingStatus = (status: SignIn["status"]) => ["starting", "pending", "publishing", "cleanup", "canceling", "completing"].includes(status);
const authorizationPending = (status: SignIn["status"]) => status === "pending" || status === "starting";
function retainedAttempt(key: string): string | null {
  try { return sessionStorage.getItem(key); } catch { return null; }
}
function rememberAttempt(key: string, id: string | null) {
  try { if (id) sessionStorage.setItem(key, id); else sessionStorage.removeItem(key); } catch { /* Status can still be checked in this tab. */ }
}
function githubSignInURL(raw: string | undefined): string | undefined {
  if (!raw) return undefined;
  try {
    const url = new URL(raw);
    if (url.protocol === "https:" && url.hostname === "github.com" && url.port === "" && !url.username && !url.password && url.pathname === "/login/device" && !url.search && !url.hash) return url.href;
  } catch { /* An invalid provider link cannot be opened. */ }
  return undefined;
}

export function PersonalAccounts({ ownerID, onConnected }: { ownerID: string; onConnected: () => void }) {
  const storageKey = `kingdom.personal-sign-in:${ownerID}`;
  const methods = useQuery({ queryKey: ["personal-account-methods", ownerID], queryFn: ({ signal }) => centralFetch<{ providers: PersonalProvider[] }>("/personal-accounts/methods", { signal }), retry: false });
  const [provider, setProvider] = useState<PersonalProvider>("github-copilot");
  const [label, setLabel] = useState("");
  const [attemptID, setAttemptID] = useState<string | null>(() => retainedAttempt(storageKey));
  const [attempt, setAttempt] = useState<SignIn | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const pending = useRef<StartRequest | null>(null);
  const cancellation = useRef<{ id: string; key: string } | null>(null);
  const requests = useRef(new Set<AbortController>());
  const completed = useRef(new Set<string>());
  const notice = useRef<HTMLParagraphElement>(null);
  useEffect(() => { if (error) notice.current?.focus(); }, [error]);
  useEffect(() => { const active = requests.current; return () => { for (const request of active) request.abort(); }; }, []);

  const accept = useCallback((result: SignIn) => {
    if (result.status === "complete" && !result.connection?.id) throw new Error("Connected account identity missing");
    setAttempt(previous => previous?.id === result.id && pendingStatus(result.status) && !result.pairing_code ? { ...result, pairing_code: previous.pairing_code } : result);
    pending.current = null;
    if (result.status === "complete") {
      setAttemptID(null); rememberAttempt(storageKey, null);
      if (!completed.current.has(result.id)) { completed.current.add(result.id); onConnected(); }
    } else {
      setAttemptID(result.id); rememberAttempt(storageKey, result.id);
    }
  }, [storageKey, onConnected]);

  const check = useCallback(async (id: string) => {
    const controller = new AbortController(); requests.current.add(controller);
    setBusy(true); setError("");
    try {
      const result = await centralFetch<SignIn>(`/personal-accounts/sign-ins/${encodeURIComponent(id)}`, { signal: controller.signal });
      if (result.id !== id) throw new Error("Sign-in identity mismatch");
      if (!controller.signal.aborted) accept(result);
    } catch (cause) {
      if (!controller.signal.aborted) {
        if (cause instanceof ApiError && [403, 404].includes(cause.status)) {
          accept({ id, provider, status: "interrupted", expires_at: new Date(Date.now() + 60_000).toISOString() });
          setError("This sign-in is no longer available in your session. Start a new sign-in.");
        } else setError("Sign-in status is unavailable. Check the same request before you start another sign-in.");
      }
    } finally { requests.current.delete(controller); if (!controller.signal.aborted) setBusy(false); }
  }, [accept, provider]);

  useEffect(() => {
    if (!attemptID || busy || error || attempt && !pendingStatus(attempt.status)) return;
    const expiry = attempt && authorizationPending(attempt.status) ? Date.parse(attempt.expires_at) : Number.POSITIVE_INFINITY;
    const delay = attempt ? Math.max(0, Math.min(1500, expiry - Date.now())) : 0;
    const timer = window.setTimeout(() => {
      if (attempt && authorizationPending(attempt.status) && (!Number.isFinite(expiry) || expiry <= Date.now())) setAttempt({ ...attempt, status: "expired" });
      else void check(attemptID);
    }, Number.isFinite(delay) ? delay : 0);
    return () => window.clearTimeout(timer);
  }, [attemptID, attempt, busy, error, check]);

  async function start(event?: FormEvent) {
    event?.preventDefault();
    if (busy || attemptID || !label.trim()) return;
    pending.current ??= { key: crypto.randomUUID(), body: JSON.stringify({ provider: selected ?? provider, label: label.trim() }) };
    const request = pending.current;
    const controller = new AbortController(); requests.current.add(controller);
    setBusy(true); setError(""); setMessage("");
    try {
      const result = await centralFetch<SignIn>("/personal-accounts/sign-ins", { method: "POST", body: request.body, headers: { "Idempotency-Key": request.key }, signal: controller.signal });
      if (!controller.signal.aborted) accept(result);
    } catch (cause) {
      if (!controller.signal.aborted) {
        if (cause instanceof ApiError && [400, 403, 409].includes(cause.status)) { pending.current = null; setError("Sign-in was rejected. Check your access and try again."); }
        else setError("Sign-in is not confirmed. Retry the same request to check its result.");
      }
    } finally { requests.current.delete(controller); if (!controller.signal.aborted) setBusy(false); }
  }
  async function cancel() {
    if (busy || !attemptID) return;
    cancellation.current ??= { id: attemptID, key: crypto.randomUUID() };
    const request = cancellation.current;
    const controller = new AbortController(); requests.current.add(controller);
    setBusy(true); setError("");
    try {
      await centralFetch<void>(`/personal-accounts/sign-ins/${encodeURIComponent(request.id)}`, { method: "DELETE", headers: { "Idempotency-Key": request.key }, signal: controller.signal });
      if (!controller.signal.aborted) {
        accept({ id: request.id, provider: attempt?.provider ?? provider, expires_at: attempt?.expires_at ?? new Date().toISOString(), status: "canceled" });
        cancellation.current = null;
      }
    } catch { if (!controller.signal.aborted) setError("Cancellation is not confirmed. Check the same sign-in or retry cancellation."); }
    finally { requests.current.delete(controller); if (!controller.signal.aborted) setBusy(false); }
  }
  function reset() {
    setAttempt(null); setAttemptID(null); setError(""); setMessage("");
    pending.current = null; cancellation.current = null; rememberAttempt(storageKey, null);
  }
  async function copyCode() {
    if (!attempt?.pairing_code) return;
    try { await navigator.clipboard.writeText(attempt.pairing_code); setMessage("Pairing code copied."); }
    catch { setMessage("Select and copy the pairing code shown below."); }
  }
  const providers = methods.data?.providers ?? [];
  const selected = providers.includes(provider) ? provider : providers[0];
  const effectiveProvider = attempt?.provider ?? selected;
  const link = attempt?.provider === "github-copilot" ? githubSignInURL(attempt.url) : undefined;
  const unsafeLink = attempt?.provider === "github-copilot" && !!attempt.url && !link;
  const expired = attempt && (attempt.status === "expired" || authorizationPending(attempt.status) && (!Number.isFinite(Date.parse(attempt.expires_at)) || Date.parse(attempt.expires_at) <= Date.now()));
  const active = busy || !!attemptID || pending.current !== null;
  return <div className="max-w-xl space-y-4 rounded-lg border border-border p-5">
    <div><h3 className="font-medium">Personal Account</h3><p className="mt-2 text-sm text-foreground-muted">Use your own provider subscription. This connection is available only to you. Each provider applies its own plan and usage limits.</p></div>
    {error && <p ref={notice} tabIndex={-1} role="alert" className="text-sm">{error}</p>}
    {unsafeLink && <p role="alert" className="text-sm">The sign-in link is unavailable. Cancel this request and try again.</p>}
    {message && <p role="status" className="text-sm">{message}</p>}
    {methods.isPending ? <p role="status">Loading sign-in options…</p> : methods.isError ? <div className="space-y-2"><p className="text-sm">Account sign-in options are unavailable.</p><Button variant="secondary" onClick={() => void methods.refetch()}>Retry Sign-In Options</Button></div> : !providers.length ? <p className="text-sm text-foreground-muted">Your operator has not enabled personal account sign-in.</p> : !attemptID && attempt?.status !== "complete" ? <form onSubmit={event => void start(event)} className="space-y-4">
      <div><label htmlFor="personal-provider" className={fieldLabelClass}>Account Provider</label><select id="personal-provider" className={fieldClass} value={selected} disabled={active} onChange={event => setProvider(event.target.value as PersonalProvider)}>{providers.map(item => <option key={item} value={item}>{item === "openai" ? "ChatGPT" : "GitHub Copilot"}</option>)}</select></div>
      <div><label htmlFor="personal-label" className={fieldLabelClass}>Account Connection Name</label><input id="personal-label" className={fieldClass} required maxLength={256} value={label} disabled={active} onChange={event => setLabel(event.target.value)} /></div>
      <Button type="submit" disabled={busy || !label.trim()}>{busy ? "Starting…" : pending.current ? "Retry Same Sign-In" : selected === "openai" ? "Connect ChatGPT" : "Sign In with GitHub Copilot"}</Button>
    </form> : null}
    {attemptID && <div className="space-y-3">
      {expired ? <p className="text-sm">This sign-in has expired. Cancel it and start a new sign-in.</p> : attempt?.status === "failed" || attempt?.status === "interrupted" ? <p className="text-sm">Sign-in did not complete. Start a new sign-in to try again.</p> : attempt?.status === "canceled" ? <p role="status" className="text-sm">Sign-in was cancelled.</p> : <>
        <p role="status" className="text-sm">{busy ? "Checking sign-in…" : attempt?.status === "canceling" ? "Canceling sign-in…" : attempt && !authorizationPending(attempt.status) ? "Finishing sign-in…" : "Waiting for sign-in to complete."}</p>
        {attempt?.status === "pending" && effectiveProvider === "github-copilot" && <>{attempt?.instructions && <p className="whitespace-pre-wrap text-sm">{attempt.instructions}</p>}{link && <a href={link} target="_blank" rel="noopener noreferrer" className="inline-block text-sm underline">Open GitHub Sign-In</a>}</>}
        {attempt?.status === "pending" && effectiveProvider === "openai" && (attempt?.pairing_code ? <div className="space-y-3 text-sm"><p>On a computer with a browser, run the command below. Enter the pairing code when the helper asks for it.</p><code className="block select-all break-all rounded bg-surface px-3 py-2">kd chatgpt attach --service {window.location.origin}</code><div><p className={fieldLabelClass}>Pairing Code</p><output className="block select-all break-all font-mono">{attempt.pairing_code}</output></div><Button variant="secondary" onClick={() => void copyCode()}>Copy Pairing Code</Button><p className="text-foreground-muted">Keep this code private. Kingdom shows it only in this tab.</p></div> : <p className="text-sm">The pairing code is no longer available in this tab. If you did not start the helper, cancel this sign-in and start again.</p>)}
      </>}
      <div className="flex flex-wrap gap-2">{!expired && (!attempt || pendingStatus(attempt.status)) && <Button variant="secondary" disabled={busy} onClick={() => void check(attemptID)}>{error ? "Retry Sign-In Status" : "Check Sign-In"}</Button>}{attempt?.status !== "canceled" && <Button variant="ghost" disabled={busy} onClick={() => void cancel()}>Cancel Sign-In</Button>}{attempt && !pendingStatus(attempt.status) && <Button variant="secondary" disabled={busy} onClick={reset}>Start New Sign-In</Button>}</div>
    </div>}
    {attempt?.status === "complete" && <div className="space-y-3"><p role="status" className="text-sm">Your personal account is connected. Select a project and refresh its models before you start new work.</p><Button variant="secondary" onClick={reset}>Connect Another Account</Button></div>}
  </div>;
}
