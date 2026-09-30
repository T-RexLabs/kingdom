import type { AcpRunActivityItem } from "@/api/hooks";
export type ActivityContent = AcpRunActivityItem["input"];

// ActivityItem is the canonical, source-agnostic shape for one unit of agent
// activity. Both the planner (planning operations) and work order runs (node attempts)
// emit the same shape, so a single view-model builder and renderer serve both.
export type ActivityKind = "thinking" | "tool" | "progress" | "message";

export type ActivityItem = {
  id: string;
  eventId: string;
  sequence: number;
  invocationId?: string;
  providerPartId?: string;
  toolCallId?: string;
  toolName?: string;
  toolStatus?: string;
  toolTitle?: string;
  recordedAt?: string;
  inputContent?: ActivityContent;
  outputContent?: ActivityContent;
  reasoningContent?: ActivityContent;
  messageContent?: ActivityContent;
  kind: ActivityKind;
  label: string;
  detail?: string;
  input?: string;
  output?: string;
  occurredAt?: string;
};

// Conversation history contains work, while canonical response readers own the
// reply. Retain message observations in the model for storage and diagnostics.
export function isConversationActivity(item: ActivityItem): boolean {
  return item.kind === "thinking" || item.kind === "tool";
}

export type ActivitySourceEntry = {
  id: string;
  label: string;
  url: string;
};

export type ActivityState = {
  sources: ActivitySourceEntry[];
  items: ActivityItem[];
};

export function emptyActivityState(): ActivityState {
  return {
    sources: [],
    items: [],
  };
}

export function activityItemFromApi(item: AcpRunActivityItem): ActivityItem {
  return {
    id: item.id, eventId: item.event_id, sequence: item.sequence,
    invocationId: item.invocation_id, providerPartId: item.provider_part_id,
    toolCallId: item.tool_call_id, toolName: item.tool_name, toolStatus: item.tool_status, toolTitle: item.tool_title,
    kind: item.kind, label: item.label,
    detail: item.kind === "thinking" ? item.reasoning.preview : item.detail,
    input: item.input.preview, output: item.output.preview,
    inputContent: item.input, outputContent: item.output, reasoningContent: item.reasoning, messageContent: item.message,
    occurredAt: item.occurred_at, recordedAt: item.recorded_at,
  };
}

// Metadata is already bounded by the canonical event writer. Exact content is
// read separately through its scoped descriptor; previews preserve whitespace.
export function parseActivityEvent(args: {
  payload: Record<string, unknown>;
  eventId: string;
  createdAt?: string;
}): ActivityItem | null {
  const { payload, eventId, createdAt } = args;
  const rawKind = readString(payload.kind);
  // Runtime ACP phases have event identity but deliberately no provider part ID.
  const id = readString(payload.activity_id) ?? (rawKind === "kingdom.phase" ? eventId : undefined);
  const sequence = payload.sequence;
  if (!id || !eventId || typeof sequence !== "number" || !Number.isSafeInteger(sequence) || sequence < 1) return null;
  const kind = rawKind === "kingdom.message" ? "message" : rawKind === "kingdom.reasoning" ? "thinking"
    : rawKind === "kingdom.tool" ? "tool"
      : rawKind === "kingdom.phase" || rawKind === "kingdom.progress" ? "progress" : null;
  if (!kind) return null;
  const label = readString(payload.label) ?? (kind === "thinking" ? "Thinking" : undefined);
  if (!label) return null;
  const input = readContent(payload.input);
  const output = readContent(payload.output);
  const reasoning = readContent(payload.reasoning);
  const message = readContent(payload.message);
  if (!input || !output || !reasoning || !message) return null;
  return {
    id, eventId, sequence, kind, label,
    invocationId: readString(payload.invocation_id), providerPartId: readString(payload.provider_part_id),
    toolCallId: readString(payload.tool_call_id), toolName: readString(payload.tool_name), toolStatus: readString(payload.tool_status), toolTitle: readString(payload.tool_title),
    inputContent: input, outputContent: output, reasoningContent: reasoning, messageContent: message,
    detail: kind === "thinking" ? reasoning.preview : readString(payload.detail),
    input: input.preview, output: output.preview,
    occurredAt: readString(payload.occurred_at) ?? createdAt,
    recordedAt: readString(payload.recorded_at) ?? createdAt,
  };
}

function readContent(raw: unknown): ActivityContent | null {
  if (!raw || typeof raw !== "object") return null;
  const value = raw as Record<string, unknown>;
  if (value.availability === "not_provided" || value.availability === "unavailable") {
    return { availability: value.availability };
  }
  if (value.availability !== "available" || typeof value.preview !== "string"
    || typeof value.byte_length !== "number" || !Number.isSafeInteger(value.byte_length) || value.byte_length < 0
    || typeof value.sha256 !== "string" || !/^[a-f0-9]{64}$/.test(value.sha256)
    || typeof value.content_url !== "string" || !value.content_url.startsWith("/")) return null;
  return {availability:"available",preview:value.preview,byte_length:value.byte_length,sha256:value.sha256,content_url:value.content_url};
}

export function buildActivityState({
  durableItems,
  liveItems,
}: {
  durableItems?: ActivityItem[];
  liveItems?: ActivityItem[];
}): ActivityState {
  const mergedItems = mergeActivityTimeline(durableItems ?? [], liveItems ?? []);
  return {
    sources: buildSourceEntries(mergedItems),
    items: mergedItems,
  };
}

export function mergeActivityTimeline(
  durableItems: ActivityItem[],
  liveItems: ActivityItem[],
): ActivityItem[] {
  const merged = new Map<string, ActivityItem>();
  for (const item of [...durableItems, ...liveItems]) {
    const key = JSON.stringify([item.invocationId ?? "",item.id]);
    const existing = merged.get(key);
    // A later-arriving response or SSE replay may carry an older snapshot.
    // Replace the complete descriptor set only when the durable sequence wins.
    if (!existing || item.sequence > existing.sequence) merged.set(key,item);
  }
  return [...merged.values()].sort((left,right) => left.sequence-right.sequence);
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
