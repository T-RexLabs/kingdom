import type { AcpRunActivityItem } from "@/api/hooks";

// ActivityItem is the canonical, source-agnostic shape for one unit of agent
// activity. Both the planner (planning operations) and decree runs (node attempts)
// emit the same shape, so a single view-model builder and renderer serve both.
export type ActivityKind = "thinking" | "tool" | "progress";

export type ActivityItem = {
  id: string;
  kind: ActivityKind;
  label: string;
  detail?: string;
  input?: string;
  output?: string;
  occurredAt?: string;
};

export type ActivityCurrentEvent = {
  id: string;
  label: string;
};

export type ActivityThoughtEntry = {
  id: string;
  label: string;
  detail: string;
  occurredAt?: string;
};

export type ActivitySourceEntry = {
  id: string;
  label: string;
  url: string;
};

export type ActivityState = {
  currentEvent: ActivityCurrentEvent | null;
  liveThoughtText: string;
  thoughts: ActivityThoughtEntry[];
  sources: ActivitySourceEntry[];
};

export function emptyActivityState(): ActivityState {
  return {
    currentEvent: null,
    liveThoughtText: "",
    thoughts: [],
    sources: [],
  };
}

export function activityItemFromApi(item: AcpRunActivityItem): ActivityItem {
  return {
    id: item.id,
    kind: item.kind,
    label: item.label,
    detail: item.detail,
    input: item.input,
    output: item.output,
    occurredAt: item.occurred_at,
  };
}

// parseActivityEvent maps a raw activity event payload (carrying a kingdom.* kind)
// into an ActivityItem. It is shared by the planner SSE controller and the run
// event stream, since both deliver the same payload shape. Returns null for events
// that are not renderable activity.
export function parseActivityEvent(args: {
  payload: Record<string, unknown>;
  eventId: string;
  createdAt?: string;
}): ActivityItem | null {
  const { payload, eventId, createdAt } = args;
  const kind = readString(payload.kind);
  const id = readString(payload.activity_id) ?? eventId;
  if (!kind || !id) {
    return null;
  }
  switch (kind) {
    case "kingdom.phase":
    case "kingdom.progress": {
      const label = readString(payload.label);
      if (!label) {
        return null;
      }
      return { id, kind: "progress", label, occurredAt: createdAt };
    }
    case "kingdom.tool": {
      const label = readString(payload.label);
      if (!label) {
        return null;
      }
      return {
        id,
        kind: "tool",
        label,
        detail: readString(payload.detail),
        input: readString(payload.tool_input),
        occurredAt: createdAt,
      };
    }
    case "kingdom.reasoning": {
      return {
        id,
        kind: "thinking",
        label: readString(payload.label) ?? "Thinking",
        detail: readString(payload.detail) ?? readString(payload.summary),
        occurredAt: createdAt,
      };
    }
    default:
      return null;
  }
}

export function buildActivityState({
  durableItems,
  liveItems,
}: {
  durableItems?: ActivityItem[];
  liveItems?: ActivityItem[];
}): ActivityState {
  const mergedItems = mergeActivityTimeline(durableItems ?? [], liveItems ?? []);
  const thoughts = buildThoughtEntries(mergedItems);
  const liveThoughtText = thoughts.map((thought) => thought.detail).join("\n\n");
  return {
    currentEvent: buildCurrentEvent(mergedItems, liveThoughtText),
    liveThoughtText,
    thoughts,
    sources: buildSourceEntries(mergedItems),
  };
}

export function mergeActivityTimeline(
  durableItems: ActivityItem[],
  liveItems: ActivityItem[],
): ActivityItem[] {
  const merged = new Map<string, ActivityItem>();
  const orderedIDs: string[] = [];

  const consume = (item: ActivityItem) => {
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

  return orderedIDs.map((id) => merged.get(id)).filter((item): item is ActivityItem => Boolean(item));
}

export function buildCurrentEvent(items: ActivityItem[], liveThoughtText: string) {
  let currentEvent: ActivityCurrentEvent | null = null;
  let latestProcessingEvent: ActivityCurrentEvent | null = null;
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

export function buildThoughtEntries(items: ActivityItem[]): ActivityThoughtEntry[] {
  return items.reduce<ActivityThoughtEntry[]>((result, item) => {
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

export function buildSourceEntries(items: ActivityItem[]): ActivitySourceEntry[] {
  const result: ActivitySourceEntry[] = [];
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

function extractWebfetchURL(item: ActivityItem): string | null {
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

function readString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed === "" ? undefined : trimmed;
}
