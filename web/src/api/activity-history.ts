import type { AcpRunActivityItem, AcpRunActivityResponse } from "./hooks-types";

export type ActivityRange = { after: number; through: number };
export type ActivityGap = ActivityRange & {
  cursor: string;
  count: number;
  direction: "earlier" | "later";
};
export type ActivityHistory = {
  items: AcpRunActivityItem[];
  ranges: ActivityRange[];
  gaps: ActivityGap[];
  resumeCursor: string;
  snapshotThrough: number;
  consumedPageCursors: string[];
};

export function mergeActivityPage(
  current: ActivityHistory | undefined,
  page: AcpRunActivityResponse,
  requestedCursor?: string,
): ActivityHistory {
  // Pinned continuation cursors are immutable. Ignore duplicate responses that
  // arrive after their covered gap was already consumed; dynamic resume cursors
  // remain reusable after an empty poll.
  if (requestedCursor && current?.consumedPageCursors.includes(requestedCursor))
    return current;
  const consumed =
    current?.gaps.some((gap) => gap.cursor === requestedCursor) ?? false;
  const events = new Map(
    (current?.items ?? []).map((item) => [item.event_id, item]),
  );
  for (const item of page.items) events.set(item.event_id, item);
  const gaps = (current?.gaps ?? []).filter(
    (gap) => gap.cursor !== requestedCursor && gap.cursor !== page.next_cursor,
  );
  if (page.next_cursor && page.remaining_count > 0) {
    gaps.push({
      cursor: page.next_cursor,
      count: page.remaining_count,
      direction: page.direction,
      after: page.direction === "earlier" ? 0 : page.range_through_sequence,
      through:
        page.direction === "earlier"
          ? page.range_after_sequence
          : page.snapshot_through_sequence,
    });
  }
  const ranges = [
    ...(current?.ranges ?? []),
    { after: page.range_after_sequence, through: page.range_through_sequence },
  ].sort((left, right) => left.after - right.after);
  const mergedRanges: ActivityRange[] = [];
  for (const range of ranges) {
    const previous = mergedRanges.at(-1);
    if (previous && range.after <= previous.through)
      previous.through = Math.max(previous.through, range.through);
    else mergedRanges.push({ ...range });
  }
  const advances =
    !current || page.snapshot_through_sequence >= current.snapshotThrough;
  return {
    items: [...events.values()].sort(
      (left, right) => left.sequence - right.sequence,
    ),
    ranges: mergedRanges,
    gaps: gaps
      .filter(
        (gap) =>
          !mergedRanges.some(
            (range) => range.after <= gap.after && range.through >= gap.through,
          ),
      )
      .sort((left, right) => left.after - right.after),
    resumeCursor: advances ? page.resume_cursor : current.resumeCursor,
    snapshotThrough: Math.max(
      current?.snapshotThrough ?? 0,
      page.snapshot_through_sequence,
    ),
    consumedPageCursors:
      consumed && requestedCursor
        ? [...(current?.consumedPageCursors ?? []), requestedCursor]
        : (current?.consumedPageCursors ?? []),
  };
}
