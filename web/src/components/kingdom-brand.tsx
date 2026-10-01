import { Link } from "@tanstack/react-router";
import { useCallback, useEffect, useRef, useState, type PointerEvent } from "react";
import { ThemeIcon } from "@/theme";
import "./kingdom-brand.css";

type BrandState = "idle" | "rex";

export function KingdomBrand({ collapsed = false }: { collapsed?: boolean }) {
  const [state, setState] = useState<BrandState>("idle");
  const revealTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const reset = useCallback(() => {
    if (revealTimer.current !== null) clearTimeout(revealTimer.current);
    revealTimer.current = null;
    setState("idle");
  }, []);

  useEffect(() => {
    function onVisibilityChange() {
      if (document.hidden) reset();
    }
    window.addEventListener("blur", reset);
    document.addEventListener("visibilitychange", onVisibilityChange);
    return () => {
      if (revealTimer.current !== null) clearTimeout(revealTimer.current);
      window.removeEventListener("blur", reset);
      document.removeEventListener("visibilitychange", onVisibilityChange);
    };
  }, [reset]);

  function startHover(event: PointerEvent<HTMLAnchorElement>) {
    if (event.pointerType !== "mouse" || event.buttons !== 0 || document.hidden) return;
    if (revealTimer.current !== null) clearTimeout(revealTimer.current);
    revealTimer.current = setTimeout(() => {
      revealTimer.current = null;
      setState("rex");
    }, 3000);
  }

  return (
    <Link
      to="/"
      aria-label="Kingdom"
      title={collapsed ? "Kingdom" : undefined}
      className="ui-brand kingdom-brand flex w-fit items-center rounded-md text-accent"
      data-brand-state={state}
      data-collapsed={collapsed ? "true" : undefined}
      onPointerEnter={startHover}
      onPointerLeave={reset}
      onPointerCancel={reset}
      onPointerDown={reset}
      onClick={reset}
    >
      <span className="kingdom-brand-scene">
        <span className="kingdom-brand-icon"><ThemeIcon slot="brand" size="var(--brand-size)" /></span>
        <span className={`kingdom-brand-title ${collapsed ? "sr-only" : "font-display text-2xl"}`} aria-hidden="true">
          {Array.from("Kingdom", (letter) => <span className="kingdom-brand-letter" key={letter}>{letter}</span>)}
        </span>
        <span className="kingdom-brand-rex-stage" aria-hidden="true">
          <span className="kingdom-brand-rex" />
          <span className="kingdom-brand-roar">
            <svg viewBox="0 0 112 52" preserveAspectRatio="none">
              <path d="m3 16 20-2L14 3l28 6 13-8 13 8 28-6-6 13 20 3-12 9 11 12-27-2-9 12-15-8-19 7-7-10-26 6 7-14-12-4 12-4Z" />
            </svg>
            <span>RAAWR!</span>
          </span>
        </span>
      </span>
    </Link>
  );
}
