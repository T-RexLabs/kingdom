import { useEffect, useRef, useState } from "react";
import { API_BASE, currentAuthGeneration, signalUnauthorized } from "./client";

/** Full bodies are intentionally absent from query caches and list fetching. */
export function useMailboxMessageContent(runId: string, messageId: string) {
  const [content, setContent] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const request = useRef<AbortController | null>(null);
  useEffect(() => () => request.current?.abort(), []);

  async function load() {
    request.current?.abort();
    const controller = new AbortController();
    request.current = controller;
    const generation = currentAuthGeneration();
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`${API_BASE}/runs/${encodeURIComponent(runId)}/mailbox/messages/${encodeURIComponent(messageId)}/content`, {
        credentials: "same-origin", cache: "no-store", signal: controller.signal,
      });
      if (response.status === 401) signalUnauthorized("read", generation);
      if (!response.ok) throw new Error("Full content could not be loaded. Retry or download the content.");
      const text = await response.text();
      if (!controller.signal.aborted) setContent(text);
    } catch (error) {
      if (!controller.signal.aborted) setError(error instanceof Error ? error.message : "Full content could not be loaded.");
    } finally {
      if (!controller.signal.aborted) setLoading(false);
    }
  }

  return { content, loading, error, load };
}
