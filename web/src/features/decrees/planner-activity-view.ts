import type { PlannerActivityItem } from "./planner-operation-controller";

export type PlannerCurrentEvent = {
  id: string;
  label: string;
};

export type PlannerThoughtEntry = {
  id: string;
  label: string;
  detail: string;
  occurredAt?: string;
};

export type PlannerSourceEntry = {
  id: string;
  label: string;
  url: string;
};

export type PlannerMessageActivityState = {
  currentEvent: PlannerCurrentEvent | null;
  liveThoughtText: string;
  thoughts: PlannerThoughtEntry[];
  sources: PlannerSourceEntry[];
};

export function emptyPlannerMessageActivityState(): PlannerMessageActivityState {
  return {
    currentEvent: null,
    liveThoughtText: "",
    thoughts: [],
    sources: [],
  };
}

export function buildPlannerMessageActivityState({
  durableItems,
  liveItems,
}: {
  durableItems?: PlannerActivityItem[];
  liveItems?: PlannerActivityItem[];
}): PlannerMessageActivityState {
  const mergedItems = mergePlannerActivityTimeline(durableItems ?? [], liveItems ?? []);
  const thoughts = buildThoughtEntries(mergedItems);
  const liveThoughtText = thoughts.map((thought) => thought.detail).join("\n\n");
  return {
    currentEvent: buildCurrentEvent(mergedItems, liveThoughtText),
    liveThoughtText,
    thoughts,
    sources: buildSourceEntries(mergedItems),
  };
}

export function mergePlannerActivityTimeline(
  durableItems: PlannerActivityItem[],
  liveItems: PlannerActivityItem[],
): PlannerActivityItem[] {
  const merged = new Map<string, PlannerActivityItem>();
  const orderedIDs: string[] = [];

  const consume = (item: PlannerActivityItem) => {
    const existing = merged.get(item.id);
    if (!existing) {
      merged.set(item.id, item);
      orderedIDs.push(item.id);
      return;
    }
    merged.set(item.id, {
      ...existing,
      ...item,
      detail: item.detail ?? existing.detail,
      input: item.input ?? existing.input,
      output: item.output ?? existing.output,
      occurredAt: item.occurredAt ?? existing.occurredAt,
    });
  };

  for (const item of durableItems) {
    consume(item);
  }
  for (const item of liveItems) {
    consume(item);
  }

  return orderedIDs.map((id) => merged.get(id)).filter((item): item is PlannerActivityItem => Boolean(item));
}

export function buildCurrentEvent(items: PlannerActivityItem[], liveThoughtText: string) {
  let currentEvent: PlannerCurrentEvent | null = null;
  let latestProcessingEvent: PlannerCurrentEvent | null = null;
  let webfetchBurst = new Set<string>();
  let webfetchBurstState: "fetching" | "fetched" = "fetched";

  for (const item of items) {
    if (!item.label.trim()) {
      continue;
    }
    if (item.kind === "thinking") {
      webfetchBurst = new Set();
      webfetchBurstState = "fetched";
      currentEvent = { id: item.id, label: item.label.trim() || "Thinking" };
      continue;
    }
    if (item.kind === "progress") {
      if (item.label === "Processing") {
        latestProcessingEvent = { id: item.id, label: item.label.trim() };
        continue;
      }
      webfetchBurst = new Set();
      webfetchBurstState = "fetched";
      currentEvent = { id: item.id, label: item.label.trim() };
      continue;
    }

    const webfetchURL = extractWebfetchURL(item);
    if (webfetchURL) {
      webfetchBurst.add(webfetchURL.toLowerCase());
      webfetchBurstState = item.label.startsWith("Fetching ") ? "fetching" : "fetched";
      currentEvent = {
        id: item.id,
        label: webfetchBurst.size > 1
          ? webfetchBurstState === "fetching"
            ? `Web fetching ${webfetchBurst.size} sources`
            : `Fetched ${webfetchBurst.size} web sources`
          : item.label.trim(),
      };
      continue;
    }

    webfetchBurst = new Set();
    webfetchBurstState = "fetched";
    currentEvent = { id: item.id, label: item.label.trim() };
  }

  if (!currentEvent && latestProcessingEvent) {
    return latestProcessingEvent;
  }
  if (!currentEvent && liveThoughtText.trim()) {
    return { id: "live-thinking", label: "Thinking" };
  }
  return currentEvent;
}

export function buildThoughtEntries(
  items: PlannerActivityItem[],
): PlannerThoughtEntry[] {
  return items.reduce<PlannerThoughtEntry[]>((result, item) => {
    if (item.kind !== "thinking") {
      return result;
    }
    const detail = item.detail?.trim() ?? "";
    if (!detail) {
      return result;
    }
    result.push({
      id: item.id,
      label: item.label,
      detail,
      occurredAt: item.occurredAt,
    });
    return result;
  }, []);
}

export function buildSourceEntries(items: PlannerActivityItem[]): PlannerSourceEntry[] {
  const result: PlannerSourceEntry[] = [];
  const seen = new Set<string>();
  for (const item of items) {
    if (item.kind !== "tool") {
      continue;
    }
    const url = extractWebfetchURL(item);
    if (!url) {
      continue;
    }
    const key = url.toLowerCase();
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    result.push({
      id: item.id,
      label: item.label,
      url,
    });
  }
  return result;
}

function extractWebfetchURL(item: PlannerActivityItem): string | null {
  if (!item.label.startsWith("Fetching ") && !item.label.startsWith("Fetched ")) {
    return extractURLFromLabel(item.label);
  }
  if (!item.input) {
    return extractURLFromLabel(item.label);
  }
  try {
    const parsed = JSON.parse(item.input) as { url?: unknown };
    return typeof parsed.url === "string" && parsed.url.trim() ? parsed.url.trim() : extractURLFromLabel(item.label);
  } catch {
    return extractURLFromLabel(item.label);
  }
}

function extractURLFromLabel(label: string): string | null {
  const match = label.match(/https?:\/\/\S+/);
  return match?.[0] ?? null;
}
