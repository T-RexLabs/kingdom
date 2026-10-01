/** Decorative, application-owned chrome; visibility belongs to the nearest
 * ornamentation scope so draft previews never inherit the live App's level. */
export function LudicrousRegalia() {
  return <div className="ludicrous-regalia" aria-hidden="true">
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
    <span className="ludicrous-regalia-rex"><span /></span>
  </div>;
}
