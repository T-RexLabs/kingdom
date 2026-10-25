import { useEffect, useRef, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { ApiError, centralFetch } from "@/api/client";
import type { CentralProject } from "./access-client";
import { useCentralSession } from "./session-context";

export type InputKind = "zip" | "file" | "github";
export type InputStatus = {
  can_upload: boolean; state: "empty" | "imported" | "started";
  source?: { id: string; kind: string; files: number; bytes: number; sha256: string; repository?: { repository_id: number; commit_sha: string; tree_sha: string } };
};
type Pending = { binding: string; path: string; key: string; contentType: string; body: File | string };
type State = { binding: string; kind: InputKind; file: File | null; ref: string; busy: boolean; uncertain: boolean; error: string };
const initial = (binding: string): State => ({ binding, kind: "zip", file: null, ref: "main", busy: false, uncertain: false, error: "" });
function checkedStatus(value: InputStatus): InputStatus {
  if (!value || typeof value.can_upload !== "boolean" || !["empty", "imported", "started"].includes(value.state)) throw new Error("Project input status is invalid.");
  return value;
}
// The launcher owns this hook. Closing its dialog retains the File, exact
// request body, retry key, and uncertainty about an admitted input operation.
export function useProjectInputController(project: CentralProject, enabled: boolean) {
  const session = useCentralSession(); const client = useQueryClient();
  const binding = JSON.stringify([session.user.id, session.session_id, project.id]);
  const current = useRef<string | null>(binding); current.current = binding;
  const visibleDialog = useRef(enabled); visibleDialog.current = enabled;
  const [state, setState] = useState(() => initial(binding));
  const visible = state.binding === binding ? state : initial(binding);
  const pending = useRef<Pending | null>(null); const active = useRef<AbortController | null>(null);
  useEffect(() => { current.current = binding; return () => { if (current.current === binding) current.current = null; active.current?.abort(); }; }, [binding]);
  const sourceKey = ["central-project-input", session.user.id, session.session_id, project.id] as const;
  const repositoryKey = ["central-project-repository", session.user.id, session.session_id, project.id] as const;
  const base = `/projects/${encodeURIComponent(project.id)}`;
  const status = useQuery({ queryKey: sourceKey, enabled, retry: false, queryFn: async ({ signal }) => checkedStatus(await centralFetch<InputStatus>(`${base}/source`, { signal })) });
  const canConfigure = project.role === "owner" && status.data?.state === "empty" && status.data.can_upload;
  const repository = useQuery({ queryKey: repositoryKey, enabled: enabled && canConfigure, retry: false, queryFn: ({ signal }) => centralFetch<{ available: boolean }>(`${base}/repository`, { signal }) });
  const patch = (value: Partial<State>) => setState(previous => ({ ...(previous.binding === binding ? previous : initial(binding)), ...value }));
  async function checkStatus(): Promise<InputStatus | null> {
    try { const result = await status.refetch(); return current.current === binding && !result.isError ? result.data ?? null : null; }
    catch { return null; }
  }
  async function send(request: Pending) {
    if (request.binding !== current.current || active.current && !active.current.signal.aborted) return;
    const wasUncertain = pending.current?.binding === binding && visible.uncertain;
    pending.current = request; const controller = new AbortController(); active.current = controller; patch({ busy: true, error: "" });
    try {
      await centralFetch(request.path, { method: "POST", headers: { "Content-Type": request.contentType, "Idempotency-Key": request.key }, body: request.body, signal: controller.signal });
      if (controller.signal.aborted || current.current !== binding) return;
      pending.current = null; patch({ uncertain: false, file: null });
      await client.invalidateQueries({ queryKey: sourceKey, refetchType: "none" });
      if (visibleDialog.current) await checkStatus();
    } catch (cause) {
      if (controller.signal.aborted || current.current !== binding) return;
      const rejected = !wasUncertain && cause instanceof ApiError && [400, 403, 404, 409, 413].includes(cause.status);
      if (rejected) pending.current = null;
      patch({ uncertain: !rejected, error: rejected ? "The input was rejected. Check project access, the selected source, and whether the project has already started." : "The input result is not confirmed. Retry the same request before making another change." });
      if (rejected && visibleDialog.current) void checkStatus();
    } finally { if (active.current === controller) { active.current = null; if (current.current === binding) patch({ busy: false }); } }
  }
  function submit() {
    if (!canConfigure || visible.busy || visible.uncertain || pending.current?.binding === binding || status.isFetching) return;
    const key = crypto.randomUUID();
    if (visible.kind === "github") {
      if (repository.isError || !repository.data?.available || !visible.ref.trim()) return;
      void send({ binding, path: `${base}/repository/import`, key, contentType: "application/json", body: JSON.stringify({ ref: visible.ref.trim() }) });
    } else if (visible.file) void send({ binding, path: `${base}/source/${visible.kind}${visible.kind === "file" ? `?name=${encodeURIComponent(visible.file.name)}` : ""}`, key, contentType: visible.kind === "zip" ? "application/zip" : "application/octet-stream", body: visible.file });
  }
  return {
    project, ...visible, status, repository, canConfigure, locked: visible.busy || visible.uncertain, checkStatus, submit,
    retry: () => { if (pending.current?.binding === binding) void send(pending.current); },
    hasPendingRequest: () => !!active.current || pending.current?.binding === binding,
    isCurrent: () => current.current === binding,
    setKind: (kind: InputKind) => { if (!visible.busy && !visible.uncertain) patch({ kind, file: null, error: "" }); },
    setFile: (file: File | null) => { if (!visible.busy && !visible.uncertain) patch({ file, error: "" }); },
    setRef: (ref: string) => { if (!visible.busy && !visible.uncertain) patch({ ref, error: "" }); },
  };
}
export type ProjectInputController = ReturnType<typeof useProjectInputController>;
