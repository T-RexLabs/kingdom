import { useCallback, useEffect, useLayoutEffect, useState, type RefObject } from "react";

export function useAutosizeTextarea({
  ref,
  value,
  minHeight,
  maxHeight,
}: {
  ref: RefObject<HTMLTextAreaElement | null>;
  value: string;
  minHeight: number;
  maxHeight: number;
}) {
  const [isOverflowing, setIsOverflowing] = useState(false);

  const measure = useCallback(() => {
    const textarea = ref.current;
    if (!textarea) {
      return;
    }

    textarea.style.height = "auto";
    const nextHeight = Math.max(minHeight, Math.min(textarea.scrollHeight, maxHeight));
    textarea.style.height = `${nextHeight}px`;
    const nextOverflowing = textarea.scrollHeight > maxHeight;
    setIsOverflowing((current) => (current === nextOverflowing ? current : nextOverflowing));
  }, [maxHeight, minHeight, ref]);

  useLayoutEffect(() => {
    measure();
  }, [measure, value]);

  useEffect(() => {
    const textarea = ref.current;
    if (!textarea) {
      return;
    }

    if (typeof ResizeObserver === "undefined") {
      const handleResize = () => {
        measure();
      };

      window.addEventListener("resize", handleResize);
      return () => {
        window.removeEventListener("resize", handleResize);
      };
    }

    const observer = new ResizeObserver(() => {
      measure();
    });

    observer.observe(textarea);
    return () => {
      observer.disconnect();
    };
  }, [measure, ref]);

  useEffect(() => {
    const fontFaceSet = document.fonts;
    if (!fontFaceSet) {
      return;
    }

    let cancelled = false;
    void fontFaceSet.ready.then(() => {
      if (!cancelled) {
        measure();
      }
    });

    const handleLoadingDone = () => {
      measure();
    };

    fontFaceSet.addEventListener?.("loadingdone", handleLoadingDone);
    return () => {
      cancelled = true;
      fontFaceSet.removeEventListener?.("loadingdone", handleLoadingDone);
    };
  }, [measure]);

  return { isOverflowing };
}
