import type { MailboxMessage } from "@/api/hooks";

export interface LaneConversation {
  laneKey: string;
  laneLabel: string;
  messages: MailboxMessage[];
  latestAt: string;
  awaitingResponseCount: number;
}

export interface MailboxPresentation {
  lanes: LaneConversation[];
  visibleMessageCount: number;
}

function agentDisplayName(name: string | undefined, id: string | undefined): string {
  if (name && name.trim()) {
    return name;
  }
  return id ?? "Unknown Agent";
}

function laneKeyOf(message: MailboxMessage): string {
  const left = (message.from_agent_id ?? "").trim();
  const right = (message.to_agent_id ?? "").trim();
  return [left, right].sort().join("::");
}

function laneLabelOf(messages: MailboxMessage[]): string {
  if (messages.length === 0) {
    return "Unknown Lane";
  }
  const first = messages[0];
  return `${agentDisplayName(first.from_agent_name, first.from_agent_id)} -> ${agentDisplayName(first.to_agent_name, first.to_agent_id)}`;
}

function computeAwaitingResponse(messages: MailboxMessage[]): number {
  const byThread = new Map<string, MailboxMessage[]>();
  for (const message of messages) {
    const key = message.thread_id || "_no_thread";
    const existing = byThread.get(key);
    if (existing) {
      existing.push(message);
    } else {
      byThread.set(key, [message]);
    }
  }

  let awaiting = 0;
  for (const list of byThread.values()) {
    list.sort((left, right) => new Date(left.created_at).getTime() - new Date(right.created_at).getTime());
    const last = list[list.length - 1];
    if (last?.communication_type === "instruction_dispatched") {
      awaiting += 1;
    }
  }
  return awaiting;
}

export function buildMailboxPresentation(messages: MailboxMessage[]): MailboxPresentation {
  const byLane = new Map<string, MailboxMessage[]>();
  for (const message of messages) {
    const key = laneKeyOf(message);
    const existing = byLane.get(key);
    if (existing) {
      existing.push(message);
    } else {
      byLane.set(key, [message]);
    }
  }

  const lanes: LaneConversation[] = [];
  for (const [laneKey, list] of byLane) {
    list.sort((left, right) => new Date(left.created_at).getTime() - new Date(right.created_at).getTime());
    const latest = list[list.length - 1];
    lanes.push({
      laneKey,
      laneLabel: laneLabelOf(list),
      messages: list,
      latestAt: latest?.created_at ?? "",
      awaitingResponseCount: computeAwaitingResponse(list),
    });
  }

  lanes.sort((left, right) => new Date(right.latestAt).getTime() - new Date(left.latestAt).getTime());
  return { lanes, visibleMessageCount: messages.length };
}
