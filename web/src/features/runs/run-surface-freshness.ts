import { skipToken, useQuery, type QueryClient } from "@tanstack/react-query";
import type { RunQuerySurface } from "@/api/hooks-runs";

export const runSurfaceFreshnessQueryKey = (runId: string) => ["runs", runId, "detail", "surface-freshness"] as const;

export interface RunSurfaceFreshnessState {
	dirtySurfaces: RunQuerySurface[];
}

const RUN_SURFACE_DIRTY_ORDER: readonly RunQuerySurface[] = [
	"detail",
	"plan",
	"governanceSummary",
	"governanceHistory",
	"graph",
	"executions",
	"timeline",
	"agents",
	"mailbox",
];

function initialRunSurfaceFreshnessState(): RunSurfaceFreshnessState {
	return { dirtySurfaces: [] };
}

export function useRunSurfaceFreshness(runId: string) {
	return useQuery({
		queryKey: runSurfaceFreshnessQueryKey(runId),
		queryFn: skipToken,
		enabled: false,
		initialData: initialRunSurfaceFreshnessState,
		staleTime: Number.POSITIVE_INFINITY,
	});
}

export function markRunQuerySurfacesDirty(
	qc: QueryClient,
	runId: string,
	surfaces: readonly RunQuerySurface[],
): void {
	const nextSurfaces = uniqueRunQuerySurfaces(surfaces);
	if (nextSurfaces.length === 0) {
		return;
	}
	qc.setQueryData<RunSurfaceFreshnessState>(runSurfaceFreshnessQueryKey(runId), (current) => ({
		dirtySurfaces: uniqueRunQuerySurfaces([...(current?.dirtySurfaces ?? []), ...nextSurfaces]),
	}));
}

export function clearRunQuerySurfacesDirty(
	qc: QueryClient,
	runId: string,
	surfaces: readonly RunQuerySurface[],
): void {
	const clearedSurfaces = new Set(uniqueRunQuerySurfaces(surfaces));
	if (clearedSurfaces.size === 0) {
		return;
	}
	qc.setQueryData<RunSurfaceFreshnessState>(runSurfaceFreshnessQueryKey(runId), (current) => ({
		dirtySurfaces: uniqueRunQuerySurfaces((current?.dirtySurfaces ?? []).filter((surface) => !clearedSurfaces.has(surface))),
	}));
}

function uniqueRunQuerySurfaces(surfaces: readonly RunQuerySurface[]): RunQuerySurface[] {
	const seen = new Set<RunQuerySurface>();
	const ordered: RunQuerySurface[] = [];
	for (const surface of RUN_SURFACE_DIRTY_ORDER) {
		if (!surfaces.includes(surface) || seen.has(surface)) {
			continue;
		}
		seen.add(surface);
		ordered.push(surface);
	}
	return ordered;
}
