import { useInteractions, useTypeahead } from "@floating-ui/react";
import { useCallback, useEffect, useId, useMemo, useRef, useState } from "react";
import { Check } from "lucide-react";
import { usePopoverFloating, usePopoverOpenState } from "@/lib/use-popover-floating";
import { PopoverSurface } from "./popover-surface";

/**
 * The option-list popover: a trigger, a scrollable list of options, and
 * optional search, grouping, and end-of-list loading.
 *
 * Floating chrome — positioning, portalling, dismissal, focus containment,
 * z-index — comes from `PopoverSurface`, which every Kingdom popover shares.
 * What lives here is the part that makes it a listbox: keyboard navigation,
 * typeahead, selection, and the option rows themselves.
 */

export interface PopoverListboxItem {
  id: string;
  label: string;
  /** Optional: the value passed back via onSelect; defaults to id. */
  value?: string;
  disabled?: boolean;
  /** Optional group heading; consecutive items with the same group sit together. */
  group?: string;
}

export interface PopoverListboxSearchProps {
  value: string;
  onChange(next: string): void;
  placeholder?: string;
}

export interface PopoverListboxProps {
  /** DOM id of the trigger; the listbox aria-labelledby points here. */
  triggerId: string;
  /** Use "combobox" when there is a search input; "button" for a plain select. */
  triggerRole: "combobox" | "button";
  /** Visual content rendered inside the trigger. */
  triggerContent: React.ReactNode;
  /** Accessible name; if absent the trigger's content provides it. */
  triggerAriaLabel?: string;
  triggerClassName?: string;
  triggerDisabled?: boolean;
  onOpenChange?: (open: boolean) => void;

  items: PopoverListboxItem[];
  selectedId: string | null;
  onSelect(id: string): void;

  /** Provided when the panel should render a search input above the list. */
  search?: PopoverListboxSearchProps;
  /** Render group headings between consecutive items with the same `group`. */
  groupBy?: boolean;

  emptyState?: React.ReactNode;
  footer?: React.ReactNode;

  /**
   * Called when the option list is scrolled to its end. Lets a consumer whose
   * options arrive a page at a time keep filling as the operator scrolls,
   * rather than making them ask for each page.
   */
  onReachEnd?: () => void;

  /** Lower bound on the panel width; defaults to 160px. */
  minWidth?: number;
  /** Upper bound on panel height (px); defaults to 320 (with autoflip). */
  maxHeight?: number;

  /** Visual className applied to the panel root. */
  panelClassName?: string;

  /** Override the rendered row for advanced cases. */
  renderItem?: (
    item: PopoverListboxItem,
    state: { active: boolean; selected: boolean },
  ) => React.ReactNode;
}

export function PopoverListbox({
  triggerId,
  triggerRole,
  triggerContent,
  triggerAriaLabel,
  triggerClassName,
  triggerDisabled,
  onOpenChange,
  items,
  selectedId,
  onSelect,
  search,
  groupBy = false,
  emptyState,
  footer,
  onReachEnd,
  minWidth = 160,
  maxHeight = 320,
  panelClassName,
  renderItem,
}: PopoverListboxProps) {
  const [activeIndex, setActiveIndex] = useState<number | null>(null);
  const activeIndexRef = useRef<number | null>(null);

  const listboxId = useId();
  const triggerLabelId = useId();
  const triggerContentId = useId();
  const listRef = useRef<Array<HTMLElement | null>>([]);
  const selectedIndex = items.findIndex((item) => item.id === selectedId && !item.disabled);

  const setActiveItemIndex = useCallback((nextActiveIndex: number | null) => {
    activeIndexRef.current = nextActiveIndex;
    setActiveIndex(nextActiveIndex);
  }, []);
  const clearActiveIndex = useCallback(() => setActiveItemIndex(null), [setActiveItemIndex]);
  const { open, setOpen: resetOpen, handleOpenChange: setOpen } = usePopoverOpenState(onOpenChange, clearActiveIndex);
  // Reset during render so a disabled owner cannot commit an interactive
  // portalled menu or reopen it implicitly when the lock is released.
  if (triggerDisabled && open) resetOpen(false);
  const handleOpenChange = useCallback((nextOpen: boolean) => {
    if (nextOpen && triggerRole === "button") {
      setActiveItemIndex(resolveInitialActiveIndex(items, selectedId));
    }
    setOpen(nextOpen);
  }, [items, selectedId, setActiveItemIndex, setOpen, triggerRole]);

  const {
    context: floatingContext,
    setReference,
    setPanel,
    modalOwned,
    panelStyle,
    baseInteractions,
  } = usePopoverFloating({
    open,
    onOpenChange: handleOpenChange,
    role: "listbox",
    allowFlip: true,
    minWidth,
    maxHeight,
    triggerDisabled,
  });

  // Typeahead is only useful when there is no search field; otherwise the
  // search input handles "type to filter" itself.
  const typeahead = useTypeahead(floatingContext, {
    listRef: { current: items.map((item) => item.label) },
    activeIndex,
    selectedIndex,
    onMatch: setActiveItemIndex,
    enabled: open && !search,
  });

  const { getReferenceProps, getFloatingProps, getItemProps } = useInteractions([
    ...baseInteractions,
    typeahead,
  ]);

  const handleSelect = useCallback(
    (item: PopoverListboxItem) => {
      if (item.disabled || triggerDisabled) return;
      onSelect(item.value ?? item.id);
	      handleOpenChange(false);
    },
	    [handleOpenChange, onSelect, triggerDisabled],
  );

  const handleKeyboardSelect = useCallback(() => {
    const selection = resolveKeyboardSelectionItem(items, activeIndexRef.current, selectedId);
    if (!selection) {
      return false;
    }
    handleSelect(selection);
    return true;
  }, [handleSelect, items, selectedId]);

  const handleListboxKeyDown = useCallback((event: React.KeyboardEvent<HTMLElement>, moveFocus: boolean) => {
    if (!open) return;
    const activeIndex = activeIndexRef.current;
    const navigationStartIndex = activeIndex ?? (selectedIndex >= 0 ? selectedIndex : null);
    let nextActiveIndex: number | null = null;
    switch (event.key) {
      case "Home":
        nextActiveIndex = firstEnabledIndex(items);
        break;
      case "End":
        nextActiveIndex = lastEnabledIndex(items);
        break;
      case "ArrowDown":
        nextActiveIndex = adjacentEnabledIndex(items, navigationStartIndex, 1);
        break;
      case "ArrowUp":
        nextActiveIndex = adjacentEnabledIndex(items, navigationStartIndex, -1);
        break;
      case "Enter":
        if (!handleKeyboardSelect()) return;
        event.preventDefault();
        event.stopPropagation();
        return;
      case " ":
        if (!moveFocus || !handleKeyboardSelect()) return;
        event.preventDefault();
        event.stopPropagation();
        return;
      default:
        return;
    }
    if (nextActiveIndex === null) return;
    setActiveItemIndex(nextActiveIndex);
    if (moveFocus) {
      const nextOption = listRef.current[nextActiveIndex];
      nextOption?.focus();
      nextOption?.scrollIntoView?.({ block: "nearest" });
    }
    event.preventDefault();
    event.stopPropagation();
  }, [handleKeyboardSelect, items, open, selectedIndex, setActiveItemIndex]);

  const groupedItems = useMemo(() => buildGroupedItems(items, groupBy), [items, groupBy]);

  const [listRoot, setListRoot] = useState<HTMLDivElement | null>(null);
  const [endSentinel, setEndSentinel] = useState<HTMLDivElement | null>(null);
  // `onReachEnd` fires from an observer rather than a scroll handler so it also
  // fires when the loaded options do not fill the panel — the case where the
  // operator has nothing to scroll and would otherwise be stuck looking at a
  // short list with more behind it.
  const reachEndRef = useRef(onReachEnd);
  useEffect(() => {
    reachEndRef.current = onReachEnd;
  }, [onReachEnd]);
  useEffect(() => {
    if (!listRoot || !endSentinel) return;
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries.some((entry) => entry.isIntersecting)) reachEndRef.current?.();
      },
      { root: listRoot },
    );
    observer.observe(endSentinel);
    return () => observer.disconnect();
    // Re-observing on every change in length matters: an observer only reports
    // changes in intersection, so a sentinel that is still visible after a page
    // loads would never fire again and the list would stall one page in. A
    // fresh observer re-reports the current state, and the consumer's own
    // guard — no cursor, or a load already in flight — is what ends the chain.
  }, [listRoot, endSentinel, items.length]);

  const triggerProps = getReferenceProps({
    id: triggerId,
    type: "button",
    role: triggerRole,
    "aria-haspopup": "listbox",
    "aria-expanded": open,
    "aria-controls": open ? listboxId : undefined,
    "aria-labelledby": triggerAriaLabel ? triggerLabelId : undefined,
    "aria-describedby": triggerAriaLabel ? triggerContentId : undefined,
    disabled: triggerDisabled,
  });

  return (
    <>
      <button ref={setReference} {...triggerProps} onKeyDownCapture={(event) => handleListboxKeyDown(event, false)} className={triggerClassName}>
				{triggerAriaLabel ? <span id={triggerLabelId} className="sr-only">{triggerAriaLabel}</span> : null}
				<span id={triggerContentId} className="contents">{triggerContent}</span>
      </button>
      {open ? (
        <PopoverSurface
			context={floatingContext}
			panelRef={setPanel}
			style={panelStyle}
			modalOwned={modalOwned}
			floatingProps={getFloatingProps()}
			onKeyDownCapture={(event) => {
				if (triggerRole === "combobox" && !search && event.target === event.currentTarget) {
					handleListboxKeyDown(event, false);
				}
			}}
			initialFocus={search ? -1 : 0}
          className={panelClassName}
        >
              {search ? (
                <input
                  type="text"
                  value={search.value}
                  onChange={(event) => search.onChange(event.target.value)}
                  onKeyDown={(event) => {
                    handleListboxKeyDown(event, false);
                  }}
                  placeholder={search.placeholder ?? "Search"}
                  // Without an explicit accessible name screen readers
                  // would announce this as an unnamed text field. Derive
                  // it from the trigger so e.g. "Model" yields
                  // "Search Model".
                  aria-label={search.placeholder ?? (triggerAriaLabel ? `Search ${triggerAriaLabel}` : "Search")}
                  autoFocus
                  className="ui-field m-2 shrink-0 rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground placeholder:text-foreground-subtle/50 focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent"
                />
              ) : null}
              <div
                ref={setListRoot}
                id={listboxId}
                role="listbox"
                aria-labelledby={triggerId}
                className="min-h-0 flex-1 overflow-auto p-1.5"
              >
                {items.length === 0
                  ? (emptyState ?? <p className="px-3 py-2 text-xs text-foreground-subtle">No options available.</p>)
                  : groupedItems.map((row) =>
                      row.kind === "group"
                        ? (
                          <p
                            key={`group-${row.label}`}
                            role="presentation"
                            className="px-3 pb-1.5 pt-3 text-xs font-medium text-foreground-muted"
                          >
                            {row.label}
                          </p>
                        )
                        : (
                          <PopoverOption
                            key={row.item.id}
                            ref={(node) => { listRef.current[row.flatIndex] = node; }}
                            item={row.item}
                            active={activeIndex === row.flatIndex}
                            selected={row.item.id === selectedId}
                            getItemProps={getItemProps}
                            onSelect={handleSelect}
                            onFocus={() => setActiveItemIndex(row.flatIndex)}
                            onKeyDown={(event) => handleListboxKeyDown(event, true)}
                            focusable={triggerRole === "button"}
                            renderItem={renderItem}
                          />
                        ),
                    )}
                {onReachEnd ? <div ref={setEndSentinel} aria-hidden="true" className="h-px" /> : null}
              </div>
              {footer ? <div className="border-t border-border p-2">{footer}</div> : null}
        </PopoverSurface>
      ) : null}
    </>
  );
}

interface PopoverOptionProps {
  item: PopoverListboxItem;
  active: boolean;
  selected: boolean;
  getItemProps: ReturnType<typeof useInteractions>["getItemProps"];
  onSelect: (item: PopoverListboxItem) => void;
  onFocus(): void;
  onKeyDown(event: React.KeyboardEvent<HTMLButtonElement>): void;
  focusable: boolean;
  renderItem?: PopoverListboxProps["renderItem"];
}

function PopoverOption({
  item,
  active,
  selected,
  getItemProps,
  onSelect,
  onFocus,
  onKeyDown,
  focusable,
  renderItem,
  ref,
}: PopoverOptionProps & { ref: React.Ref<HTMLButtonElement> }) {
  const className = optionClassName(active, selected, Boolean(item.disabled));
  return (
    <button
      ref={ref}
      type="button"
      role="option"
      aria-selected={selected}
      data-active={active || undefined}
      disabled={item.disabled}
      tabIndex={focusable && active ? 0 : -1}
      className={className}
      {...getItemProps({
        onClick: () => onSelect(item),
      })}
      onFocus={onFocus}
      onKeyDown={onKeyDown}
    >
      {renderItem ? renderItem(item, { active, selected }) : <>
        <span className="min-w-0 flex-1 text-wrap-anywhere">{item.label}</span>
        <Check size={15} aria-hidden="true" className={selected ? "shrink-0" : "invisible shrink-0"} />
      </>}
    </button>
  );
}

function optionClassName(active: boolean, selected: boolean, disabled: boolean): string {
  const base = "ui-menu-option flex min-h-9 w-full items-center gap-3 rounded-md px-3 py-2 text-left text-sm transition-colors";
  if (disabled) return `${base} cursor-not-allowed text-foreground-subtle opacity-50`;
  if (selected) return `${base} bg-accent-subtle font-medium text-accent`;
  if (active) return `${base} bg-surface-raised/60 text-foreground`;
  return `${base} text-foreground hover:bg-surface-raised/60`;
}

type GroupedRow =
  | { kind: "group"; label: string }
  | { kind: "item"; item: PopoverListboxItem; flatIndex: number };

function buildGroupedItems(items: PopoverListboxItem[], groupBy: boolean): GroupedRow[] {
  if (!groupBy) {
    return items.map((item, flatIndex) => ({ kind: "item", item, flatIndex }));
  }
  const result: GroupedRow[] = [];
  let lastGroup: string | undefined;
  items.forEach((item, flatIndex) => {
    const group = item.group?.trim() || "";
    if (group && group !== lastGroup) {
      result.push({ kind: "group", label: group });
      lastGroup = group;
    } else if (!group) {
      lastGroup = undefined;
    }
    result.push({ kind: "item", item, flatIndex });
  });
  return result;
}

function resolveInitialActiveIndex(items: PopoverListboxItem[], selectedId: string | null): number | null {
  const selectedIndex = items.findIndex((item) => item.id === selectedId && !item.disabled);
  if (selectedIndex >= 0) {
    return selectedIndex;
  }

  return firstEnabledIndex(items);
}

function firstEnabledIndex(items: PopoverListboxItem[]): number | null {
  const index = items.findIndex((item) => !item.disabled);
  return index >= 0 ? index : null;
}

function lastEnabledIndex(items: PopoverListboxItem[]): number | null {
  for (let index = items.length - 1; index >= 0; index -= 1) {
    if (!items[index]?.disabled) return index;
  }
  return null;
}

function adjacentEnabledIndex(items: PopoverListboxItem[], activeIndex: number | null, direction: -1 | 1): number | null {
  const enabledIndices = items.flatMap((item, index) => (item.disabled ? [] : [index]));
  if (enabledIndices.length === 0) return null;
  const activePosition = activeIndex === null ? -1 : enabledIndices.indexOf(activeIndex);
  if (activePosition < 0) return direction === 1 ? enabledIndices[0] : enabledIndices.at(-1) ?? null;
  return enabledIndices[(activePosition + direction + enabledIndices.length) % enabledIndices.length] ?? null;
}

function resolveKeyboardSelectionItem(
  items: PopoverListboxItem[],
  activeIndex: number | null,
  selectedId: string | null,
): PopoverListboxItem | null {
  if (activeIndex !== null) {
    const activeItem = items[activeIndex];
    if (activeItem && !activeItem.disabled) {
      return activeItem;
    }
  }

  if (selectedId !== null) {
    const selectedItem = items.find((item) => item.id === selectedId && !item.disabled);
    if (selectedItem) {
      return selectedItem;
    }
  }

  const enabledItems = items.filter((item) => !item.disabled);
  return enabledItems.length === 1 ? enabledItems[0] : null;
}
