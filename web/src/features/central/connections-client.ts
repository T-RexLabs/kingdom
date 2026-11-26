import { centralFetch } from "@/api/client";
import type { ProviderModelsResponse } from "@/api/hooks-types";
import { appBase } from "@/lib/runtime-mode";
import { modelSelectionKey } from "@/features/work-orders/provider-model-selection";

export type ModelConnection = {
  id: string; owner_id: string; scope: "personal" | "team"; revision: number; disabled: boolean;
  definition: { label: string; provider: string; endpoint?: string; auth_kind: string };
};
export type ConnectionCatalog = {
  connection_id: string; revision: number; discovered_at: string; expires_at: string;
  models: { id: string; name: string; efforts: string[] | null }[];
};
export function catalogPath(project: string, connection: ModelConnection): string {
  return `/projects/${encodeURIComponent(project)}/connections/${encodeURIComponent(connection.id)}/revisions/${connection.revision}/models`;
}
export async function hostedProviderModels(refresh: boolean, signal?: AbortSignal): Promise<ProviderModelsResponse> {
  const project = appBase().split("/")[2];
  const connections = await centralFetch<ModelConnection[]>("/connections", { signal });
  const results = await Promise.all(connections.filter(c => !c.disabled).map(async connection => {
    try {
      const catalog = await centralFetch<ConnectionCatalog>(catalogPath(project, connection) + (refresh ? "/refresh" : ""), { method: refresh ? "POST" : "GET", signal });
      if (catalog.connection_id !== connection.id || catalog.revision !== connection.revision || (!Number.isFinite(Date.parse(catalog.expires_at)) || Date.parse(catalog.expires_at) <= Date.now())) return [];
      return catalog.models.map(model => {
        const canonical = `${connection.definition.provider}/${model.id}`;
        const reference = { id: connection.id, revision: connection.revision };
        return {
          id: modelSelectionKey({ model: canonical, connection: reference }), canonical_model: canonical, connection: reference,
          provider_id: connection.definition.provider, provider_name: `${connection.definition.label} · Revision ${connection.revision}`,
          model_name: model.name,
          thinking_effort_options: (model.efforts ?? []).map(value => ({ value, label: value[0].toUpperCase() + value.slice(1) })),
        };
      });
    } catch (error) { if (signal?.aborted) throw error; return []; }
  }));
  const items = results.flat();
  return { items, catalog_status: items.length ? "fresh" : "unavailable", ...(items.length ? {} : { failure_message: "Refresh an authorized model connection to choose a model." }) };
}
