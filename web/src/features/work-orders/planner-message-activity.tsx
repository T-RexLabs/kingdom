import { ActivityDisclosure } from "@/features/activity/activity-disclosure";
import type { PlannerMessageActivityState } from "./use-planner-message-activity";

/** The persisted message tree owns the reply; provider messages are optional inspection. */
export function PlannerMessageActivity({
  scopeKey,
  activity,
}: {
  scopeKey: string;
  activity: PlannerMessageActivityState;
}) {
  return (
    <section aria-label="Planner activity">
      <ActivityDisclosure scopeKey={scopeKey} {...activity} />
    </section>
  );
}
