import type { RunExecution, RunGraphNode, TimelineEvent } from "@/api/hooks";
import type { RunStatusTone } from "./run-detail-tones";

export interface Milestone {
  id: string;
  kind: "run_event" | "node_completed" | "node_failed" | "node_running" | "node_queued" | "node_blocked" | "node_cancelled" | "fan_out";
  label: string;
  detail?: string;
  status: RunStatusTone;
  timestamp: string;
  endTimestamp?: string;
  events: TimelineEvent[];
  children?: Milestone[];
  badge?: string;
}

export interface TimelineLookups {
  nridToNodeId: Map<string, string>;
  nodeStateMap: Map<string, RunGraphNode>;
  currentNodeRunIDMap: Map<string, string>;
  executionStateMap: Map<string, RunExecution>;
}

export interface RawEventLabelResult {
  label: string;
  detail?: string;
}
