import {
  FloatingFocusManager,
  FloatingPortal,
  autoUpdate,
  offset,
  shift,
  size,
  useClick,
  useDismiss,
  useFloating,
  useInteractions,
  useListNavigation,
  useRole,
  useTypeahead,
} from "@floating-ui/react";
import { useCallback, useId, useMemo, useRef, useState } from "react";
import { isElementInsideModal, popoverZIndexForOwner, resolveOverlayContainer } from "@/lib/overlay-system";

/**
 * Generic popover-listbox primitive built on @floating-ui/react.
 *
 * Replaces the hand-rolled `getBoundingClientRect()`-once + manual outside
 * click handling that drifted when the parent column scrolled and lacked
 * keyboard support entirely. Floating UI handles:
 *   - autoUpdate (live reposition on scroll, resize, ResizeObserver)
 *   - flip / shift / size middleware (keep panel on screen, match width)
 *   - useListNavigation (Up/Down/Home/End/Enter/Space)
 *   - useTypeahead (jump-to-letter for non-search variants)
 *   - useDismiss (outside click incl. touch, Escape, focus loss)
 *   - FloatingFocusManager (focus trap inside panel; restore on close)
 *
 * Consumers stay thin: SelectField and ModelSelectField are now wrappers
 * around this primitive that supply the trigger label and item shape.
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
  minWidth = 160,
  maxHeight = 320,
  panelClassName,
  renderItem,
}: PopoverListboxProps) {
  const [open, setOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState<number | null>(null);

  const listboxId = useId();
  const triggerLabelId = useId();
  const triggerContentId = useId();
  const listRef = useRef<Array<HTMLElement | null>>([]);
  const selectedIndex = items.findIndex((item) => item.id === selectedId);

  const handleOpenChange = useCallback((nextOpen: boolean) => {
    setOpen(nextOpen);
    if (!nextOpen) {
      setActiveIndex(null);
    }
    onOpenChange?.(nextOpen);
  }, [onOpenChange]);

  const { refs, floatingStyles, context } = useFloating({
    open,
	    onOpenChange: handleOpenChange,
    placement: "bottom-start",
    whileElementsMounted: autoUpdate,
    middleware: [
      offset(4),
      shift({ padding: 8 }),
      // No `flip`. Predictability matters more than maximum panel size:
      // the listbox always opens below the trigger and `size` shrinks it
      // to fit the available viewport space (with internal scrolling
      // when the option list is longer). Flipping above the trigger was
      // jarring when triggers landed in the lower half of the viewport.
      size({
        padding: 8,
        apply({ rects, elements, availableHeight }) {
          Object.assign(elements.floating.style, {
            minWidth: `${Math.max(minWidth, rects.reference.width)}px`,
            maxHeight: `${Math.min(maxHeight, availableHeight)}px`,
          });
        },
      }),
    ],
  });

  const click = useClick(context, { enabled: !triggerDisabled });
  const dismiss = useDismiss(context, { outsidePressEvent: "pointerdown" });
  const role = useRole(context, { role: "listbox" });
  // List navigation drives Up/Down/Home/End and selection focus.
  const navigation = useListNavigation(context, {
    listRef,
    activeIndex,
    selectedIndex: selectedIndex >= 0 ? selectedIndex : null,
    onNavigate: setActiveIndex,
    loop: true,
    virtual: triggerRole === "combobox", // search input keeps real focus.
  });
  // Typeahead is only useful when there is no search field; otherwise the
  // search input handles "type to filter" itself.
  const typeahead = useTypeahead(context, {
    listRef: { current: items.map((item) => item.label) },
    activeIndex,
    selectedIndex,
    onMatch: (index) => setActiveIndex(index),
    enabled: open && !search,
  });

  const { getReferenceProps, getFloatingProps, getItemProps } = useInteractions([
    click,
    dismiss,
    role,
    navigation,
    typeahead,
  ]);

  const handleSelect = useCallback(
    (item: PopoverListboxItem) => {
      if (item.disabled) return;
      onSelect(item.value ?? item.id);
	      handleOpenChange(false);
    },
	    [handleOpenChange, onSelect],
  );

  const handleKeyboardSelect = useCallback(() => {
    const selection = resolveKeyboardSelectionItem(items, activeIndex, selectedId);
    if (!selection) {
      return false;
    }
    handleSelect(selection);
    return true;
  }, [activeIndex, handleSelect, items, selectedId]);

  const groupedItems = useMemo(() => buildGroupedItems(items, groupBy), [items, groupBy]);

  const setReference = useCallback((node: HTMLButtonElement | null) => {
    refs.setReference(node);
  }, [refs]);

  const setFloating = useCallback((node: HTMLDivElement | null) => {
    refs.setFloating(node);
  }, [refs]);

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
  const popoverOwner = refs.reference.current instanceof Element ? refs.reference.current : null;
  const modalOwned = isElementInsideModal(popoverOwner);

  return (
    <>
      <button ref={setReference} {...triggerProps} className={triggerClassName}>
				{triggerAriaLabel ? <span id={triggerLabelId} className="sr-only">{triggerAriaLabel}</span> : null}
				<span id={triggerContentId} className="contents">{triggerContent}</span>
      </button>
      {open ? (
        <FloatingPortal root={resolveOverlayContainer()}>
	          <FloatingFocusManager context={context} modal={modalOwned} initialFocus={search ? -1 : 0} returnFocus>
            <div
              ref={setFloating}
              style={{ ...floatingStyles, zIndex: popoverZIndexForOwner(popoverOwner) }}
              className={panelClassName ?? "flex flex-col overflow-hidden rounded-lg border border-border bg-background shadow-xl"}
              {...getFloatingProps()}
            >
              {search ? (
                <input
                  type="text"
                  value={search.value}
                  onChange={(event) => search.onChange(event.target.value)}
                  onKeyDown={(event) => {
                    if (event.key !== "Enter") {
                      return;
                    }
                    if (handleKeyboardSelect()) {
                      event.preventDefault();
                    }
                  }}
                  placeholder={search.placeholder ?? "Search"}
                  // Without an explicit accessible name screen readers
                  // would announce this as an unnamed text field. Derive
                  // it from the trigger so e.g. "Model" yields
                  // "Search Model".
                  aria-label={search.placeholder ?? (triggerAriaLabel ? `Search ${triggerAriaLabel}` : "Search")}
                  autoFocus
                  className="m-2 shrink-0 rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground placeholder:text-foreground-subtle/50 focus:border-gold focus:outline-none focus:ring-1 focus:ring-gold"
                />
              ) : null}
              <div
                id={listboxId}
                role="listbox"
                aria-labelledby={triggerId}
                className="min-h-0 flex-1 overflow-auto p-1"
              >
                {items.length === 0
                  ? (emptyState ?? <p className="px-3 py-2 text-xs text-foreground-subtle">No options available.</p>)
                  : groupedItems.map((row) =>
                      row.kind === "group"
                        ? (
                          <p
                            key={`group-${row.label}`}
                            role="presentation"
                            className="px-3 py-1.5 text-[10px] font-semibold uppercase tracking-wider text-foreground-subtle/70"
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
                            renderItem={renderItem}
                          />
                        ),
                    )}
              </div>
              {footer ? <div className="border-t border-border p-2">{footer}</div> : null}
            </div>
          </FloatingFocusManager>
        </FloatingPortal>
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
  renderItem?: PopoverListboxProps["renderItem"];
}

function PopoverOption({
  item,
  active,
  selected,
  getItemProps,
  onSelect,
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
      disabled={item.disabled}
      tabIndex={active ? 0 : -1}
      className={className}
      {...getItemProps({
        onClick: () => onSelect(item),
        onKeyDown: (event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            onSelect(item);
          }
        },
      })}
    >
      {renderItem ? renderItem(item, { active, selected }) : item.label}
    </button>
  );
}

function optionClassName(active: boolean, selected: boolean, disabled: boolean): string {
  const base = "block w-full rounded-md px-3 py-2 text-left text-sm transition-colors";
  if (disabled) return `${base} cursor-not-allowed text-foreground-subtle opacity-50`;
  if (selected) return `${base} bg-gold/15 font-medium text-gold`;
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
