import { useCallback, useEffect, useRef, useState } from "react";
import { REXIE_QUIPS, REXIE_ROAR_EVENT, ROAR_TEXT, nextQuip, registerPoke, skyAt } from "./rexie";

const QUIP_MS = 3200;
const ROAR_MS = 1600;

/** Decorative, application-owned chrome; visibility belongs to the nearest
 * ornamentation scope so draft previews never inherit the live App's level.
 * Rexie answers pokes, and roars at five quick ones or a typed "rawr". */
export function LudicrousRegalia() {
  const [sky] = useState(() => skyAt(new Date().getHours()));
  const [speech, setSpeech] = useState<string | null>(null);
  const [mood, setMood] = useState<"idle" | "hop" | "roar">("idle");
  const quip = useRef(-1);
  const clicks = useRef<number[]>([]);
  const timer = useRef<number | undefined>(undefined);

  const say = useCallback((text: string, nextMood: "hop" | "roar", duration: number) => {
    window.clearTimeout(timer.current);
    setSpeech(text);
    setMood(nextMood);
    timer.current = window.setTimeout(() => { setSpeech(null); setMood("idle"); }, duration);
  }, []);

  const roar = useCallback(() => say(ROAR_TEXT, "roar", ROAR_MS), [say]);

  const poke = () => {
    const result = registerPoke(clicks.current, Date.now());
    clicks.current = result.clicks;
    if (result.roar) {
      roar();
      return;
    }
    quip.current = nextQuip(quip.current);
    say(REXIE_QUIPS[quip.current], "hop", QUIP_MS);
  };

  useEffect(() => {
    window.addEventListener(REXIE_ROAR_EVENT, roar);
    return () => {
      window.removeEventListener(REXIE_ROAR_EVENT, roar);
      window.clearTimeout(timer.current);
    };
  }, [roar]);

  return <div className="ludicrous-regalia" data-sky={sky} data-mood={mood} aria-hidden="true">
    <svg className="ludicrous-sky" viewBox="0 0 1200 112" preserveAspectRatio="xMaxYMax slice" fill="none">
      {sky === "night"
        ? <>
            <path className="ludicrous-moon" d="M1126 22a20 20 0 1 0 20 30 16 16 0 1 1-20-30Z" />
            <g className="ludicrous-stars" fill="currentColor">
              <circle cx="880" cy="20" r="1.6" /><circle cx="960" cy="44" r="1.2" /><circle cx="1120" cy="18" r="1.8" />
              <circle cx="760" cy="34" r="1.2" /><circle cx="1160" cy="50" r="1.1" /><circle cx="320" cy="22" r="1.5" />
              <circle cx="180" cy="40" r="1.1" /><circle cx="560" cy="16" r="1.3" />
            </g>
          </>
        : <circle className="ludicrous-sun" cx="1130" cy={sky === "day" ? 34 : 70} r={sky === "day" ? 18 : 26} />}
    </svg>
    <svg className="ludicrous-landscape" viewBox="0 0 1200 112" preserveAspectRatio="xMidYMax slice" fill="none">
      <path className="ludicrous-range-far" d="M0 91 72 70 120 80 216 15 300 84 370 56 441 88 519 42 608 93 807 93 869 51 951 83 1066 12 1151 77 1200 59V112H0Z" />
      <path className="ludicrous-range-near" d="M0 103 65 86 121 99 232 52 290 94 363 73 422 96 506 84 582 103 894 101 956 78 1018 95 1112 64 1200 98V112H0Z" />
      <path className="ludicrous-ridge-detail" d="m186 35 30 12 18-17M497 59l22 8 13-12m511-25 23 11 17-13" />
      <path className="ludicrous-ground" d="M0 102Q160 88 340 101T660 100Q785 94 890 101T1200 101V112H0Z" />
      <g className="ludicrous-fern" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round">
        <path d="M410 101q-1-19-12-30m9 18q-14 0-20-10m20 8q9-12 7-20m-11 16q-12-3-15-13m14 10q4-14-1-20M1010 101q3-15 17-24m-13 14q-1-9 4-14m-3 12q12-2 18-10" />
      </g>
      <g className="ludicrous-dust" fill="currentColor">
        <circle cx="664" cy="99" r="2" /><circle cx="655" cy="96" r="1.3" /><circle cx="645" cy="100" r="1" />
      </g>
    </svg>
    <span className="ludicrous-regalia-rex" onClick={poke}><span /></span>
    {speech && <span className="ludicrous-speech" key={speech}>{speech}</span>}
  </div>;
}
