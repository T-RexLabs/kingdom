import { useEffect, useState, type CSSProperties } from "react";
import { useTheme } from "@/theme";
import { REXIE_ROAR_EVENT, isTypingTarget, registerKey } from "./rexie";
import { playSound } from "./rexie-sound";

const PARADE_MS = 9000;
const PARADE_SIZE = 5;

const GREETING = String.raw`
        __
       / _)   Ahoy from Rexie, captain of the Kingdom.
  .-^^^-/ /   The crew is busy. You may poke me in the header.
 /       /
<__.|_|-|_|   (Some say there is a code. Arrows are involved.)
`;

let greeted = false;

/** Headless listener for the ludicrous easter eggs. Mounted once at the root;
 * inert unless the active theme asks for ludicrous ornamentation, and it
 * ignores keys typed into fields so nobody's brief gets eaten by a dinosaur. */
export function RexieMischief() {
  const ludicrous = useTheme().theme.surfaces.ornamentation === "ludicrous";
  const [parade, setParade] = useState(0);

  useEffect(() => {
    if (!ludicrous) return;
    if (!greeted) {
      greeted = true;
      console.info(GREETING);
    }
    let trail: string[] = [];
    const onKey = (event: KeyboardEvent) => {
      if (event.metaKey || event.ctrlKey || event.altKey || isTypingTarget(event.target)) return;
      const result = registerKey(trail, event.key);
      trail = result.trail;
      if (result.egg === "parade") {
        playSound("bell");
        playSound("gulls-flyby");
        setParade((count) => count + 1);
      }
      if (result.egg === "roar") {
        playSound("foghorn");
        window.dispatchEvent(new Event(REXIE_ROAR_EVENT));
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [ludicrous]);

  useEffect(() => {
    if (parade === 0) return;
    const timer = window.setTimeout(() => setParade(0), PARADE_MS);
    return () => window.clearTimeout(timer);
  }, [parade]);

  if (!ludicrous || parade === 0) return null;
  return <div className="rexie-parade" key={parade} aria-hidden="true">
    <span className="rexie-parade-banner">All hands on deck!</span>
    <span className="rexie-parade-ship"><img src="/ship/ship-back.avif" alt="" /><img src="/ship/ship-front.avif" alt="" /></span>
    {Array.from({ length: PARADE_SIZE }, (_, index) =>
      <span key={index} className="rexie-parade-rex" style={{ "--rexie-index": index } as CSSProperties}><span /></span>)}
  </div>;
}
