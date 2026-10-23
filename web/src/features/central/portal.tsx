import { useEffect, useRef, useState, type FormEvent } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { centralFetch } from "@/api/client";
import { Button } from "@/components/ui/button";
import { fieldClass, fieldLabelClass } from "@/components/ui/field-styles";
import { CentralConnections } from "./connections";
import { useCentralSession } from "./session-context";

type Project = { id: string; display_name: string };

export function CentralPortal() {
  const session = useCentralSession();
  const client = useQueryClient();
  const projects = useQuery({ queryKey: ["central-projects", session.user.id], queryFn: () => centralFetch<Project[]>("/projects") });
  const [name, setName] = useState("");
  const [pending, setPending] = useState(false);
  const [message, setMessage] = useState("");
  const [uncertain, setUncertain] = useState(false);
  const request = useRef<{ name: string; key: string } | null>(null);
  const errorHeading = useRef<HTMLParagraphElement>(null);
  useEffect(() => { if (message) errorHeading.current?.focus(); }, [message]);
  async function create(event: FormEvent) {
    event.preventDefault();
    if (pending) return;
    const displayName = request.current?.name ?? name.trim();
    if (!displayName) return;
    request.current ??= { name: displayName, key: crypto.randomUUID() };
    setPending(true); setMessage("");
    try {
      const project = await centralFetch<Project>("/projects", { method: "POST", headers: { "Idempotency-Key": request.current.key }, body: JSON.stringify({ display_name: displayName }) });
      client.setQueryData<Project[]>(["central-projects", session.user.id], previous => [...(previous ?? []).filter(item => item.id !== project.id), project]);
      request.current = null; setName(""); setUncertain(false);
    } catch {
      setUncertain(true);
      setMessage("The result is not confirmed. Retry to check the same request.");
    } finally { setPending(false); }
  }
  async function logout() {
    if (pending) return;
    setPending(true); setMessage("");
    try {
      await centralFetch<void>("/logout", { method: "POST" });
      client.clear(); window.location.assign("/");
    } catch { setMessage("Kingdom could not confirm sign-out. Try again."); setPending(false); }
  }
  return <main className="min-h-dvh bg-background text-foreground">
    <header className="border-b border-border bg-background-secondary">
      <div className="mx-auto flex max-w-5xl flex-wrap items-center justify-between gap-4 px-5 py-5">
        <a href="/" className="font-display text-2xl text-accent">Kingdom</a>
        <div className="flex items-center gap-4"><span className="text-sm text-foreground-muted">{session.user.display_name}</span><Button variant="ghost" disabled={pending} onClick={() => { void logout(); }}>Sign Out</Button></div>
      </div>
    </header>
    <div className="mx-auto max-w-5xl space-y-8 px-5 py-10">
      <section aria-labelledby="projects-heading">
        <h1 id="projects-heading" className="text-2xl font-medium">Projects</h1>
        <p className="mb-6 mt-2 text-sm text-foreground-muted">Open a project to plan work, follow runs, and review results.</p>
        {projects.isPending ? <p role="status" className="text-sm text-foreground-muted">Loading projects…</p> : projects.isError ? <div role="alert" className="rounded-lg border border-border bg-surface p-5"><p className="mb-3 text-sm">Kingdom could not load your projects.</p><Button variant="secondary" onClick={() => { void projects.refetch(); }}>Try Again</Button></div> : projects.data.length === 0 ? <p className="rounded-lg border border-dashed border-border p-6 text-sm text-foreground-muted">{session.user.team_owner ? "Create your first project below." : "You do not have access to a project yet. Contact your team owner."}</p> : <ul className="grid gap-4 sm:grid-cols-2">
          {projects.data.map(project => <li key={project.id}><a href={`/projects/${project.id}/`} className="ui-card block rounded-lg border border-border bg-surface p-5 transition-colors hover:border-accent focus-visible:outline-2 focus-visible:outline-accent"><h2 className="font-medium">{project.display_name}</h2><p className="mt-2 text-sm text-foreground-muted">Open Project</p></a></li>)}
        </ul>}
      </section>
      <CentralConnections projects={projects.data ?? []} />
      {message && <p ref={errorHeading} tabIndex={-1} role="alert" className="rounded-lg border border-border bg-surface p-4 text-sm">{message}</p>}
      {session.user.team_owner && <section aria-labelledby="new-project-heading" className="ui-card max-w-xl rounded-lg border border-border bg-surface p-6">
        <h2 id="new-project-heading" className="text-lg font-medium">New Project</h2>
        <form onSubmit={event => { void create(event); }} className="mt-4 space-y-4">
          <div><label htmlFor="project-name" className={fieldLabelClass}>Project Name</label><input id="project-name" className={fieldClass} value={name} onChange={event => setName(event.target.value)} maxLength={512} required disabled={pending || uncertain} placeholder="Project name" /></div>
          <Button type="submit" disabled={pending}>{pending ? "Creating…" : uncertain ? "Retry Same Request" : "Create Project"}</Button>
        </form>
      </section>}
    </div>
  </main>;
}
