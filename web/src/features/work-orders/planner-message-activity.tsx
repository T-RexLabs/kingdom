import { ActivityDisclosure } from "@/features/activity/activity-disclosure";
import type { PlannerMessageActivityState } from "./use-planner-message-activity";

/** The persisted message tree owns the reply; activity holds thinking and tools. */
export function PlannerMessageActivity({
  scopeKey,
  activity,
  isLive,
}: {
  scopeKey: string;
  activity: PlannerMessageActivityState;
  isLive: boolean;
}) {
  return (
    <section aria-label="Planner activity">
      <ActivityDisclosure scopeKey={scopeKey} {...activity} isLive={isLive} />
    </section>
  );
}
