import { useEffect, useId, useRef } from "react";
import { Button } from "@/components/ui/button";
import { fieldClass, fieldLabelClass } from "@/components/ui/field-styles";
import type { InputKind, ProjectInputController } from "./use-project-input-controller";

export function ProjectInput({ controller: input }: { controller: ProjectInputController }) {
  const id = useId(); const notice = useRef<HTMLParagraphElement>(null);
  useEffect(() => { if (input.error) notice.current?.focus(); }, [input.error]);
  const status = input.status; const source = status.data?.source;
  return <div className="space-y-4">
    {input.error && <p ref={notice} tabIndex={-1} role="alert" className="text-sm">{input.error}</p>}
    {input.uncertain && <Button disabled={input.busy} onClick={input.retry}>Retry Same Input</Button>}
    {status.isPending || status.isFetching ? <p role="status">Checking project input…</p> : status.isError ? <div className="space-y-3"><p role="alert">Project input is unavailable. Check it again before opening the project.</p><Button variant="secondary" onClick={() => { void input.checkStatus(); }}>Check Project Input</Button></div> : <>
      {source && <div className="rounded-lg border border-border bg-background-secondary p-4 text-sm"><p className="font-medium">{source.kind === "github" ? "GitHub Repository" : source.kind === "zip" ? "ZIP Archive" : source.kind === "file" ? "Single File" : "Imported Files"}</p><p className="mt-1">{source.files} {source.files === 1 ? "file was" : "files were"} imported.</p>{source.repository && <p className="mt-2 break-all"><span className="font-medium">Commit: </span>{source.repository.commit_sha}</p>}</div>}
      {status.data?.state === "started" ? <p>This project has started. Initial uploads are closed.</p> : status.data?.state === "imported" ? <p>Initial files are ready. Open the project to start work.</p> : !input.canConfigure ? <p>Ask a project owner to add initial files or start this project empty.</p> : <>
        <p className="text-sm text-foreground-muted">Add initial files before starting {input.project.display_name}. You cannot replace these files or add a different initial source after the project starts.</p>
        {input.repository.isPending ? <p role="status" className="text-sm">Checking GitHub import access…</p> : input.repository.isError ? <div className="space-y-2 text-sm"><p role="alert">GitHub import access is unavailable. You can still upload a ZIP archive or one file.</p><Button variant="secondary" disabled={input.busy} onClick={() => { void input.repository.refetch(); }}>Check GitHub Access</Button></div> : <p className="text-sm text-foreground-muted">{input.repository.data?.available ? "A GitHub repository is configured for this project." : "GitHub import is not configured for this project."}</p>}
        <form onSubmit={event => { event.preventDefault(); input.submit(); }} className="space-y-4">
          <div><label htmlFor={`${id}-kind`} className={fieldLabelClass}>Input Type</label><select id={`${id}-kind`} className={fieldClass} value={input.kind} disabled={input.locked} onChange={event => input.setKind(event.target.value as InputKind)}><option value="zip">ZIP Archive</option><option value="file">Single File</option>{input.repository.data?.available && !input.repository.isError && <option value="github">GitHub Repository</option>}</select></div>
          {input.kind === "github" ? <div><label htmlFor={`${id}-ref`} className={fieldLabelClass}>Branch, Tag, or Commit</label><input id={`${id}-ref`} className={fieldClass} value={input.ref} disabled={input.locked} maxLength={200} onChange={event => input.setRef(event.target.value)} /><p className="mt-1 text-xs text-foreground-muted">Use the repository your operator enabled for this project. A retry keeps the same saved commit.</p></div> : <div><label htmlFor={`${id}-file`} className={fieldLabelClass}>File</label><input key={input.kind} id={`${id}-file`} type="file" accept={input.kind === "zip" ? ".zip,application/zip" : undefined} disabled={input.locked} onChange={event => input.setFile(event.target.files?.[0] ?? null)} />{input.file && <p className="mt-1 text-sm">Selected file: {input.file.name}</p>}<p className="mt-1 text-xs text-foreground-muted">ZIP uploads support up to 128 MiB. Individual files support up to 64 MiB. Links and project control files are rejected.</p></div>}
          <Button type="submit" disabled={input.locked || (input.kind === "github" ? !input.ref.trim() || !input.repository.data?.available || input.repository.isError : !input.file)}>{input.busy ? "Adding files…" : "Add Initial Files"}</Button>
        </form>
      </>}
    </>}
  </div>;
}
