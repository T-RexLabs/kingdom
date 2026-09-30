export type ActivityVirtualRow = { key: string };

export type ActivityVirtualPosition<Row extends ActivityVirtualRow> = {
  rows: Row[];
  firstItemIndex: number;
  anchorKey: string | null;
};

// Virtuoso's inverse list uses positive coordinates for prepends. These skipped
// indices allocate no rows and are independent of provider sequence numbers.
const INITIAL_INDEX = 1_000_000_000;

export function initialActivityPosition<Row extends ActivityVirtualRow>(
  rows: Row[],
): ActivityVirtualPosition<Row> {
  return { rows, firstItemIndex: INITIAL_INDEX, anchorKey: null };
}

export function reconcileActivityPosition<Row extends ActivityVirtualRow>(
  previous: ActivityVirtualPosition<Row>,
  rows: Row[],
): ActivityVirtualPosition<Row> {
  const indices = new Map(rows.map((row, index) => [row.key, index]));
  // Preserve the visible immutable event across inserted history and changing
  // gap controls. If that revision was superseded, keep the next retained event.
  let oldIndex = previous.rows.findIndex(
    (row) => row.key === previous.anchorKey,
  );
  if (oldIndex < 0 || !indices.has(previous.rows[oldIndex].key)) {
    oldIndex = previous.rows.findIndex((row) => indices.has(row.key));
  }
  if (oldIndex < 0) return { ...previous, rows, anchorKey: null };
  const anchorKey = previous.rows[oldIndex].key;
  return {
    rows,
    anchorKey,
    firstItemIndex:
      previous.firstItemIndex + oldIndex - indices.get(anchorKey)!,
  };
}
