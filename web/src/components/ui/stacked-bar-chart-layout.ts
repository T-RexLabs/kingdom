interface LegendLayoutInput {
  keys: string[];
  chartLeft: number;
  chartWidth: number;
  markerWidth: number;
  markerToTextGap: number;
  itemGap: number;
  measureLabel: (label: string) => number;
}

interface LegendLayout {
  totalWidth: number;
  items: Array<{ key: string; x: number }>;
}

export function computeLegendLayout({
  keys,
  chartLeft,
  chartWidth,
  markerWidth,
  markerToTextGap,
  itemGap,
  measureLabel,
}: LegendLayoutInput): LegendLayout {
  const itemWidths = keys.map((key) => markerWidth + markerToTextGap + Math.max(0, measureLabel(key)));
  const totalWidth = itemWidths.reduce((sum, width) => sum + width, 0) + Math.max(0, keys.length - 1) * itemGap;
  const startX = chartLeft + Math.max(0, (chartWidth - totalWidth) / 2);

  const items: Array<{ key: string; x: number }> = [];
  let cursor = startX;
  keys.forEach((key, index) => {
    items.push({ key, x: cursor });
    cursor += itemWidths[index] + itemGap;
  });

  return { totalWidth, items };
}
