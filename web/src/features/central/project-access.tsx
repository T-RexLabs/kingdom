import { useRef, useState, type FormEvent } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Button } from "@/components/ui/button";
import { fieldClass, fieldLabelClass } from "@/components/ui/field-styles";
import { Modal, ModalFooter } from "@/components/ui/modal";
import { accessAPI, accessKeys, membershipPath, roleLabel, type CentralProject, type ProjectMembership, type ProjectRole } from "./access-client";
import { useAccessMutation } from "./use-access-mutation";
import { useCentralSession } from "./session-context";

type Action = { kind: "role" | "remove"; membership: ProjectMembership };
export function ProjectAccess({ projects }: { projects: CentralProject[] }) {
  const session = useCentralSession(); const client = useQueryClient();
  const [open, setOpen] = useState(false); const [selected, setSelected] = useState(""); const [userID, setUserID] = useState(""); const [role, setRole] = useState<ProjectRole>("member");
  const [action, setAction] = useState<Action | null>(null); const [editedRole, setEditedRole] = useState<ProjectRole>("member");
  const notice = useRef<HTMLParagraphElement>(null);
  const owned = projects.filter(item => item.role === "owner");
  const project = owned.find(item => item.id === selected); const projectID = project?.id ?? "";
  const key = accessKeys.memberships(session.user.id, session.session_id, projectID);
  const members = useQuery({ queryKey: key, enabled: open && !!projectID, queryFn: ({ signal }) => accessAPI.memberships(projectID, signal), retry: false });
  const users = useQuery({ queryKey: accessKeys.users(session.user.id, session.session_id), enabled: open && !!projectID && session.user.team_owner, queryFn: ({ signal }) => accessAPI.users(signal), retry: false });
  const change = useAccessMutation(`${session.user.id}:${session.session_id}:${projectID}`, async () => {
    await Promise.all([client.invalidateQueries({ queryKey: key }), client.invalidateQueries({ queryKey: ["central-projects", session.user.id] })]);
  }, () => { client.removeQueries({ queryKey: key }); client.removeQueries({ queryKey: accessKeys.users(session.user.id, session.session_id) }); client.removeQueries({ queryKey: accessKeys.providers(session.user.id, session.session_id) }); });
  const candidates = users.data?.filter(user => !members.data?.some(item => item.user.id === user.id)) ?? [];
  const feedback = change.error ? <p role="alert" ref={notice} tabIndex={-1}>{change.error}</p> : change.success ? <p role="status">{change.success}</p> : null;
  async function add(event: FormEvent) {
    event.preventDefault(); if (!projectID || !session.user.team_owner || !candidates.some(user => user.id === userID)) return;
    const saved = await change.submit({ path: membershipPath(projectID, userID), method: "PUT", body: { role } }, "Project access was added.");
    if (saved) { setUserID(""); setRole("member"); } else requestAnimationFrame(() => notice.current?.focus());
  }
  async function apply() {
    if (!projectID || !action) return;
    const saved = await change.submit({ path: membershipPath(projectID, action.membership.user.id), method: action.kind === "remove" ? "DELETE" : "PUT", ...(action.kind === "role" ? { body: { role: editedRole } } : {}) }, "Project access was updated.");
    if (saved) setAction(null); else requestAnimationFrame(() => notice.current?.focus());
  }
  if (owned.length === 0) return null;
  return <section aria-labelledby="project-access-heading" className="space-y-4 rounded-lg border border-border p-5">
    <div><h2 id="project-access-heading" className="text-xl font-medium">Project Access</h2><p className="mt-2 text-sm text-foreground-muted">Project owners can change roles and remove access. Each project must keep at least one owner.</p></div>
    <Button variant="outline" disabled={change.locked} onClick={() => setOpen(value => !value)}>{open ? "Hide Project Access" : "Manage Project Access"}</Button>
    {open && <div className="max-w-xl space-y-4">
      <div><label htmlFor="access-project" className={fieldLabelClass}>Project</label><select id="access-project" className={fieldClass} value={projectID} disabled={change.locked} onChange={event => { setSelected(event.target.value); setUserID(""); setAction(null); }}><option value="">Select a project</option>{owned.map(item => <option key={item.id} value={item.id}>{item.display_name}</option>)}</select></div>
      {!action && feedback}
      {(change.refreshFailed || change.denied) && !action && <div><p className="text-sm">Reload the access list before making another change.</p><Button onClick={() => { void change.reload(); }}>Reload Access List</Button></div>}
      {change.uncertain && !change.denied && !action && <Button disabled={change.busy} onClick={() => { void change.retry("Project access is confirmed."); }}>Retry Same Request</Button>}
      {change.denied ? null : !projectID ? <p className="text-sm text-foreground-muted">Select a project you own to manage access.</p> : members.isPending ? <p role="status">Loading project access…</p> : members.isError ? <div><p role="alert">Project access is unavailable. Your role may have changed.</p><Button onClick={() => { void members.refetch(); void client.invalidateQueries({ queryKey: ["central-projects", session.user.id] }); }}>Try Again</Button></div> : <>
        <ul className="space-y-3">{members.data.map(membership => <li key={membership.user.id} className="rounded-md border border-border p-3"><div className="flex flex-wrap items-center justify-between gap-2"><span className="font-medium">{membership.user.display_name}{membership.user.id === session.user.id ? " (You)" : ""}</span><span className="text-sm">{roleLabel(membership.role)}</span></div><div className="mt-2 flex flex-wrap gap-2"><Button variant="ghost" disabled={change.locked} onClick={() => { setAction({ kind: "role", membership }); setEditedRole(membership.role); }}>Change Role<span className="sr-only"> for {membership.user.display_name}</span></Button><Button variant="ghost" disabled={change.locked} onClick={() => setAction({ kind: "remove", membership })}>Remove Access<span className="sr-only"> for {membership.user.display_name}</span></Button></div></li>)}</ul>
        {!session.user.team_owner ? <p className="text-sm text-foreground-muted">Ask a team owner to add another person. You can manage the people shown here.</p> : users.isPending ? <p role="status">Loading authorized accounts…</p> : users.isError ? <div><p role="alert">Authorized accounts are unavailable.</p><Button onClick={() => { void users.refetch(); }}>Retry Accounts</Button></div> : <form className="space-y-4" onSubmit={event => { void add(event); }}>
          <h3 className="font-medium">Add Project Member</h3>
          <div><label htmlFor="access-person" className={fieldLabelClass}>Authorized Account</label><select id="access-person" className={fieldClass} required value={userID} disabled={change.locked} onChange={event => setUserID(event.target.value)}><option value="">Select an account</option>{candidates.map(user => <option key={user.id} value={user.id}>{user.display_name}{user.accounts.length ? ` · ${user.accounts.map(account => `${account.provider_id === "github" ? "GitHub" : "Organization"} ${account.account_id}`).join("; ")}` : " · Sign-in provider unavailable"}</option>)}</select><p className="mt-1 text-xs text-foreground-muted">Authorize a provider account in Team Members before adding it here.</p></div>
          <div><label htmlFor="access-role" className={fieldLabelClass}>Project Role</label><select id="access-role" className={fieldClass} value={role} disabled={change.locked} onChange={event => setRole(event.target.value as ProjectRole)}><option value="member">Member</option><option value="owner">Owner</option></select></div>
          <Button type="submit" disabled={change.locked || !userID}>{change.busy ? "Adding…" : "Add Member"}</Button>
        </form>}
      </>}
    </div>}
    <Modal open={!!action && !!projectID} onClose={() => { if (!change.busy) setAction(null); }} title={action?.kind === "remove" ? "Remove Project Access" : "Change Project Role"} description={action?.kind === "remove" ? `Remove ${action.membership.user.display_name} from ${project?.display_name}. Active project requests will stop.` : `Update ${action?.membership.user.display_name ?? "the account"} in ${project?.display_name ?? "this project"}.`} size="sm" footer={<ModalFooter><Button variant="ghost" disabled={change.busy} onClick={() => setAction(null)}>Cancel</Button><Button disabled={change.busy || change.denied || action?.kind === "role" && editedRole === action.membership.role && !change.uncertain} onClick={() => { if (change.uncertain) { void change.retry("Project access is confirmed.").then(saved => { if (saved) setAction(null); }); } else { void apply(); } }}>{change.busy ? "Saving…" : change.uncertain ? "Retry Same Request" : action?.kind === "remove" ? "Remove Access" : "Save Role"}</Button></ModalFooter>}>
      <div className="space-y-4">{action && feedback}{change.denied && <Button onClick={() => { void change.reload(); }}>Reload Access List</Button>}{action?.kind === "role" && !change.denied && <div><label htmlFor="edited-project-role" className={fieldLabelClass}>Project Role</label><select id="edited-project-role" className={fieldClass} value={editedRole} disabled={change.locked} onChange={event => setEditedRole(event.target.value as ProjectRole)}><option value="member">Member</option><option value="owner">Owner</option></select></div>}</div>
    </Modal>
  </section>;
}
