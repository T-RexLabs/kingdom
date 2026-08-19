import { useCallback, useEffect, useRef, useState } from "react";
import { Loader2 } from "lucide-react";
import { apiFetch } from "@/api/client";
import type { components } from "@/api/schema";
import { Button } from "@/components/ui/button";
import { fieldClass } from "@/components/ui/field-styles";
import { newIdempotencyKey as idem } from "@/lib/idempotency";
import { cn } from "@/lib/utils";

type Connection = components["schemas"]["Connection"];
type Repository = components["schemas"]["Repository"];

type RepositoryPage = { items: Repository[]; next_cursor?: string };

/**
 * Which of a connection's granted repositories Kingdom reads from.
 *
 * GitHub decides what an installation grants; this decides how much of that
 * grant Kingdom uses. The two are different questions, so this never offers a
 * repository GitHub has not granted — the server proves that again before it
 * stores anything.
 *
 * The same editor serves the end of setup and the saved connection card, so
 * there is only one way to answer the question.
 */
export function IncludedRepositories({
  connection,
  onSaved,
  onDone,
  doneLabel = "Done",
}: {
  connection: Connection;
  onSaved: () => void | Promise<void>;
  onDone?: () => void;
  doneLabel?: string;
}) {
  const connectionId = connection.connection_id;
  const [mode, setMode] = useState<"all" | "selected">(connection.included_repository_mode === "selected" ? "selected" : "all");
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [repositories, setRepositories] = useState<Repository[]>([]);
  const [cursor, setCursor] = useState<string | undefined>();
  const [query, setQuery] = useState("");
  const [debouncedQuery, setDebouncedQuery] = useState("");
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string>();
  const [attempt, setAttempt] = useState(0);

  // A late page must never append onto a list the filter has already replaced,
  // so every request carries the generation it was issued under.
  const generation = useRef(0);
  const pagination = useRef<AbortController>(undefined);

  useEffect(() => {
    const timer = setTimeout(() => setDebouncedQuery(query.trim()), 300);
    return () => clearTimeout(timer);
  }, [query]);

  useEffect(() => {
    generation.current += 1;
    pagination.current?.abort();
    pagination.current = undefined;
    setCursor(undefined);
    setRepositories([]);
  }, [debouncedQuery, attempt, connectionId]);

  useEffect(() => {
    const current = generation.current;
    const controller = new AbortController();
    setLoading(true);
    setError(undefined);
    void (async () => {
      try {
        const params = new URLSearchParams({ scope: "granted" });
        if (debouncedQuery) params.set("search", debouncedQuery);
        const page = await apiFetch<RepositoryPage>(`/connections/${connectionId}/repositories?${params}`, { signal: controller.signal });
        if (controller.signal.aborted || current !== generation.current) return;
        setRepositories(page.items);
        setCursor(page.next_cursor);
        // The server is the source of truth for what is currently included, so
        // the boxes reflect it rather than anything remembered locally.
        setSelected((existing) => {
          const next = new Set(existing);
          for (const item of page.items) {
            if (item.included) next.add(item.id);
          }
          return next;
        });
      } catch (cause) {
        if (controller.signal.aborted || current !== generation.current) return;
        setError(cause instanceof Error ? cause.message : "Repositories could not be loaded");
      } finally {
        if (!controller.signal.aborted && current === generation.current) setLoading(false);
      }
    })();
    return () => controller.abort();
  }, [connectionId, debouncedQuery, attempt]);

  const loadMore = useCallback(async () => {
    if (!cursor || loading) return;
    const current = generation.current;
    const controller = new AbortController();
    pagination.current = controller;
    setLoading(true);
    try {
      const params = new URLSearchParams({ scope: "granted", cursor });
      if (debouncedQuery) params.set("search", debouncedQuery);
      const page = await apiFetch<RepositoryPage>(`/connections/${connectionId}/repositories?${params}`, { signal: controller.signal });
      if (controller.signal.aborted || current !== generation.current) return;
      setRepositories((existing) => [...existing, ...page.items]);
      setCursor(page.next_cursor);
      setSelected((existing) => {
        const next = new Set(existing);
        for (const item of page.items) {
          if (item.included) next.add(item.id);
        }
        return next;
      });
    } catch (cause) {
      if (controller.signal.aborted || current !== generation.current) return;
      setError(cause instanceof Error ? cause.message : "More repositories could not be loaded");
    } finally {
      if (!controller.signal.aborted && current === generation.current) setLoading(false);
    }
  }, [connectionId, cursor, debouncedQuery, loading]);

  const save = async () => {
    setSaving(true);
    setError(undefined);
    try {
      await apiFetch(`/connections/${connectionId}/included-repositories`, {
        method: "PUT",
        headers: { "Idempotency-Key": idem("included-repositories") },
        body: JSON.stringify(mode === "all" ? { mode } : { mode, repository_ids: [...selected] }),
      });
      await onSaved();
      onDone?.();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Repositories could not be saved");
    } finally {
      setSaving(false);
    }
  };

  const toggle = (id: string) => {
    setSelected((existing) => {
      const next = new Set(existing);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const choice = (value: "all" | "selected", label: string, help: string) => (
    <label
      className={cn(
        "flex cursor-pointer gap-3 rounded-xl border p-3 transition",
        mode === value ? "border-gold bg-gold/10" : "border-border bg-background/40 hover:border-gold/40",
      )}
    >
      <input className="sr-only" type="radio" name={`included-mode-${connectionId}`} value={value} checked={mode === value} onChange={() => setMode(value)} />
      <span className="block">
        <span className="block text-sm font-medium">{label}</span>
        <span className="mt-0.5 block text-xs text-foreground-muted">{help}</span>
      </span>
    </label>
  );

  return (
    <div className="space-y-4">
      <div className="grid gap-2 sm:grid-cols-2">
        {choice("all", "All Repositories It Grants", "Kingdom follows the installation, including anything you add on GitHub later.")}
        {choice("selected", "Only Selected Repositories", "Kingdom reads from the repositories you tick below.")}
      </div>

      {mode === "selected" && (
        <div className="space-y-3">
          <input
            className={fieldClass}
            type="search"
            aria-label="Filter repositories"
            placeholder="Filter owner/name"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
          />
          <div className="max-h-64 space-y-2 overflow-y-auto rounded-xl border border-border bg-background/40 p-2">
            {repositories.length === 0 ? (
              <p className="rounded-lg border border-dashed border-border p-6 text-center text-sm text-foreground-muted">
                {loading ? "Loading repositories..." : "No repositories match."}
              </p>
            ) : (
              repositories.map((repository) => (
                <label
                  key={repository.id}
                  className={cn(
                    "flex cursor-pointer items-center gap-3 rounded-lg border p-2.5 transition",
                    selected.has(repository.id) ? "border-gold bg-gold/10" : "border-border bg-background/40 hover:border-gold/40",
                  )}
                >
                  <input className="sr-only" type="checkbox" checked={selected.has(repository.id)} onChange={() => toggle(repository.id)} />
                  <span
                    aria-hidden
                    className={cn(
                      "flex h-4 w-4 flex-none items-center justify-center rounded border text-[10px] font-bold",
                      selected.has(repository.id) ? "border-gold bg-gold text-background" : "border-border",
                    )}
                  >
                    {selected.has(repository.id) ? "✓" : ""}
                  </span>
                  <span className="truncate text-sm">{repository.locator}</span>
                </label>
              ))
            )}
          </div>
          {cursor && (
            <Button type="button" variant="ghost" size="sm" className="w-full" disabled={loading} onClick={() => void loadMore()}>
              {loading && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
              Load More Repositories
            </Button>
          )}
          <p className="text-xs text-foreground-muted" aria-live="polite">
            {selected.size} selected
          </p>
        </div>
      )}

      {error && (
        <div role="alert" className="rounded-lg border border-status-failed/30 bg-status-failed/10 p-3 text-xs text-status-failed">
          <p>{error}</p>
          <Button type="button" variant="ghost" size="sm" className="mt-2" onClick={() => setAttempt((value) => value + 1)}>
            Try Again
          </Button>
        </div>
      )}

      <div className="flex justify-end gap-2">
        {onDone && (
          <Button type="button" variant="ghost" onClick={onDone} disabled={saving}>
            Cancel
          </Button>
        )}
        <Button type="button" onClick={() => void save()} disabled={saving || (mode === "selected" && selected.size === 0)}>
          {saving && <Loader2 className="h-4 w-4 animate-spin" />}
          {doneLabel}
        </Button>
      </div>
    </div>
  );
}
