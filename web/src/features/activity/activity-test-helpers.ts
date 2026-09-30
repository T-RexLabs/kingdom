import type { ActivityContent } from "./activity-view";

export function availableActivityContent(text: string): ActivityContent {
  return {availability:"available",preview:text,byte_length:new TextEncoder().encode(text).length,sha256:"a".repeat(64),content_url:"/acp/runs/run/activity/event/content/output"};
}

export function activityPayload(overrides: Record<string,unknown> = {}): Record<string,unknown> {
  return {sequence:1,activity_id:"part",input:{availability:"not_provided"},output:{availability:"not_provided"},reasoning:{availability:"not_provided"},message:{availability:"not_provided"},...overrides};
}
