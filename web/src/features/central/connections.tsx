import { useCallback, useEffect, useRef, useState, type FormEvent } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { ApiError, centralFetch } from "@/api/client";
import { Button } from "@/components/ui/button";
import { fieldClass, fieldLabelClass } from "@/components/ui/field-styles";
import { PersonalAccounts } from "./personal-accounts";
import { useCentralSession } from "./session-context";
import { catalogPath, type ConnectionCatalog, type ModelConnection } from "./connections-client";

type Pending = { path: string; method: string; body?: string; key: string; accountDisconnect?: boolean };
export function CentralConnections({ projects }: { projects: { id: string; display_name: string }[] }) {
  const session = useCentralSession();
  const queryClient = useQueryClient();
  const queryKey = ["central-connections", session.user.id];
  const connections = useQuery({ queryKey, queryFn: () => centralFetch<ModelConnection[]>("/connections") });
  const [connectionType, setConnectionType] = useState<"api_key" | "account">("api_key");
  const [personalShown, setPersonalShown] = useState(false);
  const accountConnected = useCallback(() => { void queryClient.invalidateQueries({ queryKey: ["central-connections", session.user.id] }); }, [queryClient, session.user.id]);
  const [editing, setEditing] = useState<ModelConnection | null>(null);
  const [label, setLabel] = useState("");
  const [provider, setProvider] = useState("openai");
  const [endpoint, setEndpoint] = useState("");
  const [scope, setScope] = useState<"personal" | "team">("personal");
  const [credential, setCredential] = useState("");
  const [project, setProject] = useState("");
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState("");
  const [failed, setFailed] = useState(false);
  const notice = useRef<HTMLParagraphElement>(null);
  useEffect(() => { if (failed && message) notice.current?.focus(); }, [failed, message]);
  const [uncertain, setUncertain] = useState(false);
  const [catalogs, setCatalogs] = useState<Record<string, ConnectionCatalog>>({});
  // Keys and request bodies stay in memory only. Ambiguous retries reuse exact bytes.
  const pending = useRef<Pending | null>(null);
  async function send(request: Pending) {
    if (busy) return;
    pending.current = request;
    setBusy(true); setMessage(""); setFailed(false);
    try {
      const result = await centralFetch<{ remote_revocation_confirmed: boolean } | undefined>(request.path, { method: request.method, headers: { "Idempotency-Key": request.key }, ...(request.body ? { body: request.body } : {}) });
      pending.current = null; setCredential(""); setEditing(null); setLabel(""); setUncertain(false); setCatalogs({});
      await queryClient.invalidateQueries({ queryKey });
      setMessage(request.accountDisconnect ? result?.remote_revocation_confirmed ? "The account was disconnected and remote revocation was confirmed. Existing work cannot use this connection. Sign in again to create a new connection." : "The account was disconnected from Kingdom. Remote revocation is not confirmed. Revoke access in your provider account settings. Sign in again to create a new connection." : "The connection was updated.");
    } catch (error) {
      setCredential(""); setFailed(true);
      if (!uncertain && error instanceof ApiError && [400, 403, 409].includes(error.status)) {
        pending.current = null; setUncertain(false);
        setMessage(request.accountDisconnect ? "The account change was rejected. Check your access and try again." : "The connection change was rejected. Check your settings and access, then enter the key again.");
      } else {
        setUncertain(true);
        setMessage("The result is not confirmed. Retry the same request before making another change.");
      }
    } finally { setBusy(false); }
  }
  function submit(event: FormEvent) {
    event.preventDefault();
    if (busy || uncertain || !credential) return;
    const definition = { label: label.trim(), provider: provider.trim(), ...(endpoint.trim() ? { endpoint: endpoint.trim() } : {}), auth_kind: "api_key" };
    const body = editing ? { expected_revision: editing.revision, definition, credential } : { scope, definition, credential };
    void send({ path: editing ? `/connections/${editing.id}` : "/connections", method: editing ? "PUT" : "POST", body: JSON.stringify(body), key: crypto.randomUUID() });
  }
  async function readCatalog(connection: ModelConnection, refresh: boolean) {
    if (!project || busy || uncertain) return;
    setBusy(true); setMessage(""); setFailed(false);
    try {
      const result = await centralFetch<ConnectionCatalog>(catalogPath(project, connection) + (refresh ? "/refresh" : ""), { method: refresh ? "POST" : "GET" });
      setCatalogs(previous => ({ ...previous, [connection.id]: result }));
    } catch { setFailed(true); setMessage("Models are unavailable. Check your project access or contact your operator."); }
    finally { setBusy(false); }
  }
  const locked = busy || uncertain;
  return <section aria-labelledby="connections-heading" className="space-y-5">
    <div><h2 id="connections-heading" className="text-xl font-medium">Model Connections</h2><p className="mt-2 text-sm text-foreground-muted">Add an API key or sign in with your personal provider account. Team owners can also share an API-key connection.</p></div>
    {message && <p ref={notice} tabIndex={failed ? -1 : undefined} role={failed ? "alert" : "status"} className="rounded-lg border border-border p-4 text-sm">{message}</p>}
    {uncertain && <Button disabled={busy} onClick={() => { if (pending.current) void send(pending.current); }}>Retry Same Request</Button>}
    <div className="max-w-xl"><label htmlFor="connection-project" className={fieldLabelClass}>Project for Model Discovery</label><select id="connection-project" className={fieldClass} value={project} disabled={locked} onChange={event => { setProject(event.target.value); setCatalogs({}); }}><option value="">Select a project</option>{projects.map(item => <option key={item.id} value={item.id}>{item.display_name}</option>)}</select></div>
    {connections.isPending ? <p role="status">Loading connections…</p> : connections.isError ? <Button onClick={() => { void connections.refetch(); }}>Retry Connections</Button> : <ul className="space-y-3">{connections.data.map(connection => <li key={connection.id} className="rounded-lg border border-border bg-surface p-4">
      <h3 className="font-medium">{connection.definition.label}</h3><p className="mt-1 text-sm text-foreground-muted">{connection.scope === "team" ? "Team" : "Personal"} · Revision {connection.revision} · {connection.disabled ? "Disabled" : "Active"}</p>
      {!connection.disabled && <div className="mt-3 flex flex-wrap gap-2">
        <Button variant="secondary" disabled={locked || !project} onClick={() => { void readCatalog(connection, false); }}>View Models</Button><Button variant="secondary" disabled={locked || !project} onClick={() => { void readCatalog(connection, true); }}>Refresh Models</Button>
        {(connection.owner_id === session.user.id || connection.scope === "team" && session.user.team_owner) && <>{connection.definition.auth_kind === "api_key" && <Button variant="ghost" disabled={locked} onClick={() => { setConnectionType("api_key"); setEditing(connection); setLabel(connection.definition.label); setProvider(connection.definition.provider); setEndpoint(connection.definition.endpoint ?? ""); setScope(connection.scope); setCredential(""); }}>Replace Key</Button>}<Button variant="ghost" disabled={locked} onClick={() => { void send({ path: `/connections/${connection.id}`, method: "DELETE", key: crypto.randomUUID(), accountDisconnect: connection.definition.auth_kind !== "api_key" }); }}>{connection.definition.auth_kind === "api_key" ? "Disable" : "Disconnect"}</Button></>}
      </div>}
      {connection.disabled && connection.definition.auth_kind !== "api_key" && connection.owner_id === session.user.id && <div className="mt-3 space-y-2"><p className="text-sm text-foreground-muted">Sign in again to create a new personal connection. Existing work keeps its original connection.</p><Button variant="secondary" disabled={locked} onClick={() => { setConnectionType("account"); setPersonalShown(true); }}>Reconnect</Button></div>}
      {catalogs[connection.id] && <div className="mt-4 text-sm"><p>Available Models</p><ul className="mt-2 space-y-1">{catalogs[connection.id].models.map(model => <li key={model.id}>{model.name}{model.efforts?.length ? ` · ${model.efforts.join(", ")}` : ""}</li>)}</ul>{catalogs[connection.id].models.length === 0 && <p>No supported models are available.</p>}</div>}
    </li>)}</ul>}
    <fieldset className="flex flex-wrap gap-4 text-sm" disabled={locked || !!editing}>
      <legend className={fieldLabelClass}>Connection Type</legend>
      <label className="flex items-center gap-2"><input type="radio" name="connection-type" value="api_key" checked={connectionType === "api_key"} onChange={() => setConnectionType("api_key")} />API Key Connection</label>
      <label className="flex items-center gap-2"><input type="radio" name="connection-type" value="account" checked={connectionType === "account"} onChange={() => { setConnectionType("account"); setPersonalShown(true); }} />Personal Account</label>
    </fieldset>
    {personalShown && <div hidden={connectionType !== "account"}><PersonalAccounts key={session.user.id} ownerID={session.user.id} onConnected={accountConnected} /></div>}
    {connectionType === "api_key" && <form onSubmit={submit} className="max-w-xl space-y-4 rounded-lg border border-border p-5">
      <h3 className="font-medium">{editing ? "Replace Key" : "New Model Connection"}</h3>
      <div><label htmlFor="connection-label" className={fieldLabelClass}>Connection Name</label><input id="connection-label" className={fieldClass} required maxLength={256} value={label} disabled={locked} onChange={e => setLabel(e.target.value)} /></div>
      <div><label htmlFor="connection-provider" className={fieldLabelClass}>Provider ID</label><input id="connection-provider" className={fieldClass} required value={provider} disabled={locked} onChange={e => setProvider(e.target.value)} /></div>
      <div><label htmlFor="connection-endpoint" className={fieldLabelClass}>Custom HTTPS Endpoint</label><input id="connection-endpoint" type="url" className={fieldClass} value={endpoint} disabled={locked} placeholder="https://models.example.com/v1" onChange={e => setEndpoint(e.target.value)} /><p className="mt-1 text-xs text-foreground-muted">Leave empty for the default OpenAI endpoint.</p></div>
      <div><label htmlFor="connection-scope" className={fieldLabelClass}>Access</label><select id="connection-scope" className={fieldClass} value={scope} disabled={locked || !!editing} onChange={e => setScope(e.target.value as "personal" | "team")}><option value="personal">Personal</option>{session.user.team_owner && <option value="team">Team</option>}</select></div>
      <div><label htmlFor="connection-key" className={fieldLabelClass}>API Key</label><input id="connection-key" type="password" autoComplete="new-password" className={fieldClass} required value={credential} disabled={locked} onChange={e => setCredential(e.target.value)} /><p className="mt-1 text-xs text-foreground-muted">Keys are not shown after submission. Existing work keeps its original connection settings.</p></div>
      <div className="flex gap-2"><Button type="submit" disabled={locked}>{busy ? "Saving…" : editing ? "Save Replacement Key" : "Add Connection"}</Button>{editing && <Button variant="ghost" disabled={locked} onClick={() => { setEditing(null); setCredential(""); setLabel(""); }}>Cancel</Button>}</div>
    </form>}
  </section>;
}
