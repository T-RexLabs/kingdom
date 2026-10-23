import { useRef, useState, type FormEvent } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { ApiError, centralFetch } from "@/api/client";
import { Button } from "@/components/ui/button";
import { fieldClass, fieldLabelClass } from "@/components/ui/field-styles";
import { useCentralSession } from "./session-context";

type InputStatus = { can_upload: boolean; state: "empty" | "imported" | "started"; source?: { id: string; kind: string; files: number; bytes: number; sha256: string } };
type Upload = { project: string; file: File; kind: "zip" | "file"; key: string };
export function ProjectInput({ projects }: { projects: { id: string; display_name: string }[] }) {
 const session=useCentralSession(); const client=useQueryClient();
 const [project,setProject]=useState("");const [kind,setKind]=useState<"zip"|"file">("zip");const [file,setFile]=useState<File|null>(null);
 const [busy,setBusy]=useState(false);const [uncertain,setUncertain]=useState(false);const [error,setError]=useState("");
 const pending=useRef<Upload|null>(null);const notice=useRef<HTMLParagraphElement>(null);
 const key=["central-project-input",session.user.id,project];
 const status=useQuery({queryKey:key,enabled:!!project,queryFn:()=>centralFetch<InputStatus>(`/projects/${project}/source`)});
 async function upload(request:Upload){
  if(busy)return;pending.current=request;setBusy(true);setError("");
  try{
   await centralFetch(`/projects/${request.project}/source/${request.kind}${request.kind==="file"?`?name=${encodeURIComponent(request.file.name)}`:""}`,{method:"POST",headers:{"Content-Type":request.kind==="zip"?"application/zip":"application/octet-stream","Idempotency-Key":request.key},body:request.file});
   pending.current=null;setUncertain(false);setFile(null);await client.invalidateQueries({queryKey:key});
  }catch(cause){
   if(!uncertain&&cause instanceof ApiError&&[400,403,409,413].includes(cause.status)){pending.current=null;setError("The upload was rejected. Check project access, file size, and whether the project has already started.");void status.refetch();}
   else{setUncertain(true);setError("The upload result is not confirmed. Retry the same file before making another change.");}
   requestAnimationFrame(()=>notice.current?.focus());
  }finally{setBusy(false)}
 }
 function submit(event:FormEvent){event.preventDefault();if(project&&file&&!uncertain&&status.data?.state==="empty")void upload({project,file,kind,key:crypto.randomUUID()})}
 const locked=busy||uncertain;
 return <section aria-labelledby="project-input-heading" className="max-w-xl space-y-4 rounded-lg border border-border p-5">
  <h2 id="project-input-heading" className="text-lg font-medium">Initial Project Files</h2>
  <p className="text-sm text-foreground-muted">Add a ZIP archive or one file before opening a new project. Initial files cannot be replaced after upload or after the project starts.</p>
  <div><label htmlFor="input-project" className={fieldLabelClass}>Project</label><select id="input-project" className={fieldClass} value={project} disabled={locked} onChange={e=>{setProject(e.target.value);setError("");setFile(null)}}><option value="">Select a project</option>{projects.map(p=><option key={p.id} value={p.id}>{p.display_name}</option>)}</select></div>
  {error&&<p ref={notice} tabIndex={-1} role="alert" className="text-sm">{error}</p>}
  {uncertain&&<Button disabled={busy} onClick={()=>{if(pending.current)void upload(pending.current)}}>Retry Same Upload</Button>}
  {project&&(status.isPending?<p role="status">Checking project input…</p>:status.isError?<div><p role="alert">Project input is unavailable.</p><Button onClick={()=>{void status.refetch()}}>Try Again</Button></div>:!status.data.can_upload?<p role="status" className="text-sm">{status.data.state==="started"?"This project has started. Initial uploads are closed.":status.data.state==="imported"?"Initial files are ready. Open the project to start work.":"Only a project owner can add initial files."}{status.data.source?` ${status.data.source.files} files were imported.`:""}</p>:<form onSubmit={submit} className="space-y-4">
   <div><label htmlFor="input-kind" className={fieldLabelClass}>Input Type</label><select id="input-kind" className={fieldClass} value={kind} disabled={locked} onChange={e=>{setKind(e.target.value as "zip"|"file");setFile(null)}}><option value="zip">ZIP Archive</option><option value="file">Single File</option></select></div>
   <div><label htmlFor="project-file" className={fieldLabelClass}>File</label><input key={`${project}-${kind}`} id="project-file" type="file" accept={kind==="zip"?".zip,application/zip":undefined} disabled={locked} onChange={e=>setFile(e.target.files?.[0]??null)} /><p className="mt-1 text-xs text-foreground-muted">ZIP uploads support up to 128 MiB. Individual files support up to 64 MiB. Links and project control files are rejected.</p></div>
   <Button type="submit" disabled={locked||!file}>{busy?"Uploading…":"Add Initial Files"}</Button>
  </form>)}
 </section>;
}
