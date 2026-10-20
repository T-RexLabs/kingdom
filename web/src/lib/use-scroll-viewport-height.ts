import { useLayoutEffect, useState } from "react";

/**
 * The visible height of a scroll viewport, less its bottom padding: the height
 * a final section needs to fill the viewport exactly once scrolled to the end.
 * Pass the element from a callback ref so a late-mounting viewport is measured.
 */
export function useScrollViewportHeight(
  element: HTMLElement | null,
  enabled: boolean,
): number | null {
  const [height, setHeight] = useState<number | null>(null);
  useLayoutEffect(() => {
    if (!enabled || !element) return;
    const measure = () => {
      const paddingBottom = parseFloat(getComputedStyle(element).paddingBottom) || 0;
      setHeight(Math.max(0, element.clientHeight - paddingBottom));
    };
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(element);
    return () => observer.disconnect();
  }, [element, enabled]);
  return enabled ? height : null;
}
