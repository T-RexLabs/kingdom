import { useEffect, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { ApiError, centralFetch } from "@/api/client";
import type { AccessRequest } from "./access-client";

type Pending = { binding: string; path: string; method: string; key: string; body?: string };
type State = { binding: string; busy: boolean; uncertain: boolean; error: string; success: string; refreshFailed: boolean; denied: boolean };
const initial = (binding: string): State => ({ binding, busy: false, uncertain: false, error: "", success: "", refreshFailed: false, denied: false });

// Exact pending bytes are retained only for their actor/session/project binding.
// An unconfirmed result locks other changes until that request is checked.
export function useAccessMutation(binding: string, refresh: () => Promise<unknown>, accessLost: () => void, rejectedMessage = "The access change was rejected. Check current access and keep at least one project owner.") {
  const client = useQueryClient();
  const [state, setState] = useState<State>(() => initial(binding));
  const current = useRef(binding); current.current = binding;
  const pending = useRef<Pending | null>(null);
  const active = useRef<AbortController | null>(null);
  const visible = state.binding === binding ? state : initial(binding);
  useEffect(() => () => { active.current?.abort(); }, [binding]);
  async function send(request: Pending, message: string) {
    if (active.current && !active.current.signal.aborted) return false;
    if (request.binding !== current.current) return false;
    pending.current = request;
    const controller = new AbortController(); active.current = controller;
    setState({ ...initial(binding), busy: true, uncertain: visible.uncertain });
    try {
      await centralFetch(request.path, { method: request.method, headers: { "Idempotency-Key": request.key }, ...(request.body ? { body: request.body } : {}), signal: controller.signal });
      if (controller.signal.aborted || current.current !== request.binding) return false;
      pending.current = null;
      // The server acknowledgment is final. A subsequent read failure cannot
      // turn a confirmed mutation into an unknown request or cause a resend.
      let refreshFailed = false;
      try { await refresh(); } catch { refreshFailed = true; }
      if (!controller.signal.aborted && current.current === request.binding) {
        setState({ ...initial(binding), success: message, refreshFailed });
      }
      return true;
    } catch (error) {
      if (controller.signal.aborted || current.current !== request.binding) return false;
      const rejected = !visible.uncertain && error instanceof ApiError && [400, 403, 404, 409].includes(error.status);
      if (rejected) pending.current = null;
      if (error instanceof ApiError && error.status === 403) {
        accessLost();
        void client.invalidateQueries({ queryKey: ["central-projects"] });
      }
      setState({ ...initial(binding), uncertain: !rejected, denied: error instanceof ApiError && error.status === 403, error: rejected ? rejectedMessage : "The result is not confirmed. Retry the same request before making another change." });
      return false;
    } finally { if (active.current === controller) active.current = null; }
  }
  return {
    ...visible,
    locked: visible.busy || visible.uncertain || visible.denied,
    clearFeedback: () => {
      if (visible.busy || visible.uncertain || visible.denied || pending.current?.binding === binding) return;
      setState(previous => previous.binding === binding && !previous.busy && !previous.uncertain && !previous.denied ? { ...previous, error: "", success: "" } : previous);
    },
    reload: async () => {
      try {
        await refresh();
        if (current.current === binding) setState(previous => previous.binding === binding ? { ...previous, refreshFailed: false, denied: false } : previous);
      } catch {
        if (current.current === binding) setState(previous => previous.binding === binding ? { ...previous, refreshFailed: true } : previous);
      }
    },
    submit: (request: AccessRequest, message: string) => {
      if (visible.busy || pending.current?.binding === binding) return Promise.resolve(false);
      return send({ binding, path: request.path, method: request.method, key: crypto.randomUUID(), ...(request.body ? { body: JSON.stringify(request.body) } : {}) }, message);
    },
    retry: (message: string) => pending.current?.binding === binding ? send(pending.current, message) : Promise.resolve(false),
  };
}
