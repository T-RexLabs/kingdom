import { FloatingNode, useFloatingNodeId, useInteractions } from "@floating-ui/react";
import { CalendarDays, ChevronLeft, ChevronRight } from "lucide-react";
import { useId, useLayoutEffect, useRef, useState, type KeyboardEvent, type MutableRefObject } from "react";
import { usePopoverFloating } from "@/lib/use-popover-floating";
import { cn } from "@/lib/utils";
import { Button } from "./button";
import { PopoverSurface } from "./popover-surface";

const DAY = 86_400_000;
const FIRST_DAY = Date.parse("0000-01-01T00:00:00Z");
const LAST_DAY = Date.parse("9999-12-31T00:00:00Z");

// Calendar arithmetic uses UTC dates, independently of the time zone used to
// interpret the endpoint text. DST changes must never skip or repeat a cell.
function calendarDate(year: number, month: number, day: number): Date {
  const date = new Date(0);
  date.setUTCFullYear(year, month, day);
  return date;
}

function dateKey(date: Date): string {
  return `${String(date.getUTCFullYear()).padStart(4, "0")}-${String(date.getUTCMonth() + 1).padStart(2, "0")}-${String(date.getUTCDate()).padStart(2, "0")}`;
}

function parseDay(value: string): Date | null {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return null;
  const date = new Date(`${value}T00:00:00Z`);
  return Number.isFinite(date.getTime()) && dateKey(date) === value ? date : null;
}

function clampDay(date: Date): Date {
  return new Date(Math.max(FIRST_DAY, Math.min(LAST_DAY, date.getTime())));
}

function shiftMonth(date: Date, amount: number): Date {
  const month = Math.max(0, Math.min(9999 * 12 + 11, date.getUTCFullYear() * 12 + date.getUTCMonth() + amount));
  const year = Math.floor(month / 12);
  const lastDay = calendarDate(year, month % 12 + 1, 0).getUTCDate();
  return calendarDate(year, month % 12, Math.min(date.getUTCDate(), lastDay));
}

export function CalendarPicker({ value, onChange, label, className }: {
  value: string;
  onChange: (day: string) => void;
  label: string;
  className?: string;
}) {
  const id = useId();
  const nodeId = useFloatingNodeId();
  const [open, setOpen] = useState(false);
  const initialFocus = useRef<HTMLButtonElement>(null);
  const { context, setReference, setPanel, modalOwned, panelStyle, baseInteractions } = usePopoverFloating({
    nodeId,
    open,
    onOpenChange: setOpen,
    role: "dialog",
    placement: "bottom-end",
    allowFlip: true,
    widthMode: "css",
    maxHeight: 400,
  });
  const { getReferenceProps, getFloatingProps } = useInteractions(baseInteractions);

  return (
    <FloatingNode id={nodeId}>
      <Button
        ref={setReference}
        {...getReferenceProps({ type: "button", "aria-label": `Choose ${label} date`, "aria-expanded": open })}
        variant="ghost"
        className={cn("h-auto min-h-11 w-11 shrink-0 rounded-none border-l border-border bg-surface-raised px-0", className)}
      >
        <CalendarDays className="h-4 w-4" aria-hidden="true" />
      </Button>
      {open && (
        <PopoverSurface
          context={context}
          panelRef={setPanel}
          modalOwned={modalOwned}
          style={panelStyle}
          floatingProps={getFloatingProps({ "aria-label": `${label} Calendar` })}
          initialFocus={initialFocus}
          className="w-[20rem] max-w-[calc(100vw-1rem)]"
        >
          <CalendarMonth
            id={id}
            value={value}
            initialFocus={initialFocus}
            onChange={(day) => {
              onChange(day);
              setOpen(false);
            }}
          />
        </PopoverSurface>
      )}
    </FloatingNode>
  );
}

function CalendarMonth({ id, value, initialFocus, onChange }: {
  id: string;
  value: string;
  initialFocus: MutableRefObject<HTMLButtonElement | null>;
  onChange: (day: string) => void;
}) {
  const [today] = useState(() => {
    const now = new Date();
    return calendarDate(now.getFullYear(), now.getMonth(), now.getDate());
  });
  const [active, setActive] = useState(() => parseDay(value) ?? today);
  const focusAfterNavigation = useRef(false);
  const activeKey = dateKey(active);
  const todayKey = dateKey(today);
  const monthStart = calendarDate(active.getUTCFullYear(), active.getUTCMonth(), 1);
  const firstCell = monthStart.getTime() - ((monthStart.getUTCDay() + 6) % 7) * DAY;
  const monthFormatter = new Intl.DateTimeFormat(undefined, { month: "long", year: "numeric", timeZone: "UTC", calendar: "gregory" });
  const dayFormatter = new Intl.DateTimeFormat(undefined, { dateStyle: "full", timeZone: "UTC", calendar: "gregory" });
  const weekdayFormatter = new Intl.DateTimeFormat(undefined, { weekday: "short", timeZone: "UTC" });
  const weekdayLongFormatter = new Intl.DateTimeFormat(undefined, { weekday: "long", timeZone: "UTC" });

  useLayoutEffect(() => {
    if (focusAfterNavigation.current) {
      initialFocus.current?.focus();
      focusAfterNavigation.current = false;
    }
  }, [activeKey, initialFocus]);

  function handleDayKeyDown(event: KeyboardEvent<HTMLButtonElement>) {
    let next: Date;
    switch (event.key) {
      case "ArrowLeft": next = new Date(active.getTime() - DAY); break;
      case "ArrowRight": next = new Date(active.getTime() + DAY); break;
      case "ArrowUp": next = new Date(active.getTime() - 7 * DAY); break;
      case "ArrowDown": next = new Date(active.getTime() + 7 * DAY); break;
      case "Home": next = new Date(active.getTime() - ((active.getUTCDay() + 6) % 7) * DAY); break;
      case "End": next = new Date(active.getTime() + (6 - (active.getUTCDay() + 6) % 7) * DAY); break;
      case "PageUp": next = shiftMonth(active, event.shiftKey ? -12 : -1); break;
      case "PageDown": next = shiftMonth(active, event.shiftKey ? 12 : 1); break;
      default: return;
    }
    event.preventDefault();
    const bounded = clampDay(next);
    if (dateKey(bounded) !== activeKey) {
      focusAfterNavigation.current = true;
      setActive(bounded);
    }
  }

  return (
    <div className="min-h-0 overflow-y-auto p-3">
      <div className="mb-3 flex items-center justify-between gap-2">
        <Button type="button" variant="ghost" size="icon" aria-label="Previous month"
          disabled={active.getUTCFullYear() === 0 && active.getUTCMonth() === 0}
          onClick={() => setActive(shiftMonth(active, -1))}
          className="h-9 w-9 shrink-0"
        ><ChevronLeft className="h-4 w-4" aria-hidden="true" /></Button>
        <h3 id={`${id}-month`} aria-live="polite" className="text-center text-sm font-semibold">
          {monthFormatter.format(active)}
        </h3>
        <Button type="button" variant="ghost" size="icon" aria-label="Next month"
          disabled={active.getUTCFullYear() === 9999 && active.getUTCMonth() === 11}
          onClick={() => setActive(shiftMonth(active, 1))}
          className="h-9 w-9 shrink-0"
        ><ChevronRight className="h-4 w-4" aria-hidden="true" /></Button>
      </div>
      <table role="grid" aria-labelledby={`${id}-month`} className="w-full table-fixed border-separate border-spacing-1">
        <thead>
          <tr>{Array.from({ length: 7 }, (_, index) => {
            const weekday = calendarDate(2024, 0, index + 1);
            return <th key={index} scope="col" abbr={weekdayLongFormatter.format(weekday)} className="pb-2 text-center text-xs font-medium text-foreground-muted">
              {weekdayFormatter.format(weekday)}
            </th>;
          })}</tr>
        </thead>
        <tbody>
          {Array.from({ length: 6 }, (_, week) => (
            <tr key={week}>
              {Array.from({ length: 7 }, (_, weekday) => {
                const instant = firstCell + (week * 7 + weekday) * DAY;
                if (instant < FIRST_DAY || instant > LAST_DAY) return <td key={weekday} />;
                const date = new Date(instant);
                const key = dateKey(date);
                const selected = key === value;
                const isToday = key === todayKey;
                return (
                  <td key={weekday} aria-selected={selected} className="p-0 text-center">
                    <Button
                      ref={key === activeKey ? initialFocus : undefined}
                      type="button"
                      variant={selected ? "default" : "ghost"}
                      aria-label={dayFormatter.format(date)}
                      aria-current={isToday ? "date" : undefined}
                      aria-pressed={selected}
                      data-date={key}
                      tabIndex={key === activeKey ? 0 : -1}
                      onKeyDown={handleDayKeyDown}
                      onClick={() => onChange(key)}
                      className={cn(
                        "h-9 w-full min-w-0 px-0 text-xs tabular-nums",
                        !selected && date.getUTCMonth() !== active.getUTCMonth() && "text-foreground-subtle",
                        !selected && isToday && "ring-1 ring-inset ring-accent font-semibold text-accent",
                      )}
                    >{date.getUTCDate()}</Button>
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
