import type { InfiniteData } from "@tanstack/react-query";

/** Keep first-seen order and identity when live updates move rows between pages. */
export function historyItems<T>(pages: Array<{ items: T[] }>, id: (item: T) => string): T[] {
  const seen = new Set<string>();
  return pages.flatMap((page) => page.items.filter((item) => {
    const key = id(item);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  }));
}

export function mapHistoryPages<T>(
  current: InfiniteData<T> | undefined,
  update: (page: T) => T,
): InfiniteData<T> | undefined {
  if (!current) return current;
  const pages = current.pages.map(update);
  return pages.some((page, index) => page !== current.pages[index]) ? { ...current, pages } : current;
}
