import { useRef, useState, type FormEvent } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Button } from "@/components/ui/button";
import { fieldClass, fieldLabelClass } from "@/components/ui/field-styles";
import { accessAPI, accessKeys } from "./access-client";
import { useAccessMutation } from "./use-access-mutation";
import { useCentralSession } from "./session-context";

export function TeamMembers() {
  const session = useCentralSession(); const client = useQueryClient();
  const [open, setOpen] = useState(false); const [name, setName] = useState(""); const [providerID, setProviderID] = useState(""); const [accountID, setAccountID] = useState("");
  const notice = useRef<HTMLParagraphElement>(null);
  const key = accessKeys.users(session.user.id, session.session_id);
  const users = useQuery({ queryKey: key, enabled: open && session.user.team_owner, queryFn: ({ signal }) => accessAPI.users(signal), retry: false });
  const providers = useQuery({ queryKey: accessKeys.providers(session.user.id, session.session_id), enabled: open && session.user.team_owner, queryFn: ({ signal }) => accessAPI.providers(signal), retry: false });
  const provider = providers.data?.find(item => item.id === (providerID || providers.data[0]?.id));
  const change = useAccessMutation(`${session.user.id}:${session.session_id}:team`, () => client.invalidateQueries({ queryKey: key }), () => { client.removeQueries({ queryKey: key }); client.removeQueries({ queryKey: accessKeys.providers(session.user.id, session.session_id) }); }, "The account authorization was rejected. Check the provider account ID and your team access.");
  if (!session.user.team_owner) return null;
  async function submit(event: FormEvent) {
    event.preventDefault(); if (!provider || !name.trim() || !accountID) return;
    const saved = await change.submit({ path: "/users", method: "POST", body: { provider_id: provider.id, account_id: accountID, display_name: name.trim() } }, "The account is authorized to sign in. Add the person to a project to share work.");
    if (saved) { setName(""); setAccountID(""); }
    else requestAnimationFrame(() => notice.current?.focus());
  }
  return <section aria-labelledby="team-members-heading" className="space-y-4 rounded-lg border border-border p-5">
    <div><h2 id="team-members-heading" className="text-xl font-medium">Team Members</h2><p className="mt-2 text-sm text-foreground-muted">Authorize a specific provider account before the person signs in. Project access is assigned separately.</p></div>
    <Button variant="outline" onClick={() => setOpen(value => !value)} disabled={change.locked}>{open ? "Hide Team Members" : "Manage Team Members"}</Button>
    {open && <div className="max-w-xl space-y-4">
      {change.error && <p role="alert" tabIndex={-1} ref={notice}>{change.error}</p>}{change.success && <p role="status">{change.success}</p>}
      {(change.refreshFailed || change.denied) && <div><p className="text-sm">Reload the access list before making another change.</p><Button onClick={() => { void change.reload(); }}>Reload Access List</Button></div>}
      {change.uncertain && !change.denied && <Button disabled={change.busy} onClick={() => { void change.retry("The account authorization is confirmed."); }}>Retry Same Request</Button>}
      {change.denied ? null : users.isPending || providers.isPending ? <p role="status">Loading team access…</p> : users.isError || providers.isError ? <div><p role="alert">Team access is unavailable.</p><Button onClick={() => { void users.refetch(); void providers.refetch(); }}>Try Again</Button></div> : <>
        <ul className="space-y-2">{users.data.map(user => <li key={user.id}><span className="font-medium">{user.display_name}</span><span className="ml-2 text-sm text-foreground-muted">{user.team_owner ? "Team Owner" : "Team Member"}</span>{user.accounts.map(account => <p className="text-sm text-foreground-muted" key={`${account.provider_id}:${account.account_id}`}>{account.provider_id === "github" ? "GitHub" : "Organization Sign-In"} · {account.account_id}</p>)}</li>)}</ul>
        {providers.data.length === 0 ? <p role="status">No sign-in provider is configured. Contact your operator.</p> : <form className="space-y-4" onSubmit={event => { void submit(event); }}>
          <h3 className="font-medium">Authorize an Account</h3>
          <div><label htmlFor="team-user-name" className={fieldLabelClass}>Display Name</label><input id="team-user-name" className={fieldClass} required maxLength={512} value={name} disabled={change.locked} onChange={event => setName(event.target.value)} /></div>
          <div><label htmlFor="team-user-provider" className={fieldLabelClass}>Sign-In Provider</label><select id="team-user-provider" className={fieldClass} value={provider?.id ?? ""} disabled={change.locked} onChange={event => { setProviderID(event.target.value); setAccountID(""); }}>{providers.data.map(item => <option key={item.id} value={item.id}>{item.label}</option>)}</select></div>
          <div><label htmlFor="team-user-account" className={fieldLabelClass}>{provider?.account_id_label ?? "Account ID"}</label><input id="team-user-account" autoCapitalize="none" autoCorrect="off" spellCheck={false} autoComplete="off" inputMode={provider?.id === "github" ? "numeric" : "text"} className={fieldClass} required maxLength={provider?.id === "github" ? 19 : 512} pattern={provider?.id === "github" ? "[1-9][0-9]*" : undefined} value={accountID} disabled={change.locked} onChange={event => setAccountID(event.target.value)} aria-describedby="team-account-help" /><p id="team-account-help" className="mt-1 text-xs text-foreground-muted">{provider?.help}</p></div>
          <Button type="submit" disabled={change.locked || !provider}>{change.busy ? "Authorizing…" : "Authorize Account"}</Button>
        </form>}
      </>}
    </div>}
  </section>;
}
