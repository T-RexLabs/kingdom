import { useRef, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { ApiError, centralFetch } from "@/api/client";
import { Button } from "@/components/ui/button";
import { Modal, ModalFooter } from "@/components/ui/modal";
import { fieldClass, fieldLabelClass } from "@/components/ui/field-styles";
import { appBase } from "@/lib/runtime-mode";

type Publication = { id: string; state: "pending" | "published" | "unchanged"; branch: string; pull_request?: number; url?: string };
type Request = { run_id: string; base_branch: string; title: string; body: string };
export function PublishResult({ runID }: { runID: string }) {
 const project=appBase().split("/")[2];const client=useQueryClient();const base=`/projects/${project}/repository`;
 const grant=useQuery({queryKey:["repository-grant",project],queryFn:()=>centralFetch<{available:boolean;can_publish:boolean}>(base),retry:false});
 const queryKey=["repository-publications",project,runID];
 const history=useQuery({queryKey,enabled:grant.data?.can_publish===true,queryFn:()=>centralFetch<Publication[]>(`${base}/publications?run_id=${encodeURIComponent(runID)}`)});
 const [open,setOpen]=useState(false);const [branch,setBranch]=useState("main");const [title,setTitle]=useState("Publish Kingdom Result");const [body,setBody]=useState("");const [busy,setBusy]=useState(false);const [error,setError]=useState("");const [result,setResult]=useState<Publication|null>(null);
 const retained=useRef<{key:string;body:Request}|null>(null);const alert=useRef<HTMLParagraphElement>(null);
 async function publish(id?:string){if(busy)return;setBusy(true);setError("");const retry=retained.current!==null;
 try{let response:Publication;if(id){response=await centralFetch<Publication>(`${base}/publications/${id}/reconcile`,{method:"POST"})}else{const request=retained.current??{key:crypto.randomUUID(),body:{run_id:runID,base_branch:branch.trim(),title:title.trim(),body}};retained.current=request;response=await centralFetch<Publication>(`${base}/publish`,{method:"POST",headers:{"Content-Type":"application/json","Idempotency-Key":request.key},body:JSON.stringify(request.body)})}
 retained.current=null;setResult(response);await client.invalidateQueries({queryKey});
 }catch(cause){if(!retry&&cause instanceof ApiError&&[400,403,409].includes(cause.status))retained.current=null;setError("Publication is not confirmed. Check the saved request before starting another publication.");requestAnimationFrame(()=>alert.current?.focus());void client.invalidateQueries({queryKey})}finally{setBusy(false)}}
 if(!grant.data?.can_publish)return null;
 const pending=history.data?.find(item=>item.state==="pending");const published=history.data?.find(item=>item.state==="published");const locked=busy||retained.current!==null;
 return <div className="space-y-2">
 <Button variant="outline" onClick={()=>setOpen(true)}>Publish to GitHub</Button>
 <Modal open={open} onClose={()=>{if(!busy)setOpen(false)}} title="Publish Result to GitHub" description="Create a separate branch and pull request from this run’s verified result. The base branch is not changed." size="sm" footer={<ModalFooter><Button variant="ghost" disabled={busy} onClick={()=>setOpen(false)}>Close</Button><Button disabled={busy||history.isPending||history.isError||(!retained.current&&!!pending)||!branch.trim()||!title.trim()} onClick={()=>void publish()}>{busy?"Checking…":retained.current?"Retry Same Publication":"Create Pull Request"}</Button></ModalFooter>}>
 <div className="space-y-4">
 {error&&<p role="alert" tabIndex={-1} ref={alert}>{error}</p>}
 {result&&<p role="status">{result.state==="unchanged"?"This result has no file changes to publish.":result.state==="published"?"The pull request is ready.":"The submission is still pending. Check it again before creating another pull request."}{result.url&&<a className="ml-2 underline" href={result.url} target="_blank" rel="noreferrer">Open Pull Request</a>}</p>}
 {history.isError&&<p role="alert">Saved publications are unavailable. Try again before creating another publication.</p>}
 {published&&<div className="space-y-2"><p>A pull request was already created for this run.</p><Button disabled={busy} onClick={()=>void publish(published.id)}>Show Pull Request</Button></div>}
 {pending&&<div className="space-y-2"><p>A saved publication still needs confirmation.</p><Button disabled={busy} onClick={()=>void publish(pending.id)}>Check Saved Publication</Button></div>}
 <div><label className={fieldLabelClass} htmlFor="publish-base">Base Branch</label><input id="publish-base" className={fieldClass} value={branch} disabled={locked||!!pending} onChange={e=>setBranch(e.target.value)} maxLength={200}/></div>
 <div><label className={fieldLabelClass} htmlFor="publish-title">Title</label><input id="publish-title" className={fieldClass} value={title} disabled={locked||!!pending} onChange={e=>setTitle(e.target.value)} maxLength={200}/></div>
 <div><label className={fieldLabelClass} htmlFor="publish-body">Description</label><textarea id="publish-body" className={fieldClass} value={body} disabled={locked||!!pending} onChange={e=>setBody(e.target.value)} maxLength={8192}/></div>
 </div></Modal></div>;
}
