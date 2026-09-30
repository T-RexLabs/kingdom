import {
  FloatingOverlay,
  FloatingPortal,
} from "@floating-ui/react";
import {
  type KeyboardEvent as ReactKeyboardEvent,
  type ReactNode,
  useCallback,
  useEffect,
  useEffectEvent,
  useRef,
} from "react";
import { cn } from "@/lib/utils";
import { MODAL_PANEL_ATTRIBUTE, OVERLAY_Z_INDEX, resolveOverlayContainer } from "@/lib/overlay-system";
import { setModalPresence } from "@/lib/modal-state";

interface ModalDialogProps {
  open: boolean;
  onClose: () => void;
  children: ReactNode;
  ariaLabel?: string;
  ariaLabelledBy?: string;
  overlayClassName?: string;
  panelClassName?: string;
  getFinalFocus?: () => HTMLElement | null;
}

/**
 * Shared modal primitive with proper focus containment and restore semantics.
 *
 * The Phase 4 remediation batch replaces hand-rolled overlays that visually
 * looked modal but left keyboard focus behind the backdrop. Floating UI keeps
 * the behavior consistent across work order, planning, and run-detail dialogs.
 */
export function ModalDialog({
  open,
  onClose,
  children,
  ariaLabel,
  ariaLabelledBy,
  overlayClassName,
  panelClassName,
  getFinalFocus,
}: ModalDialogProps) {
  const panelRef = useRef<HTMLDivElement | null>(null);
  const previousFocusRef = useRef<HTMLElement | null>(null);
  const presenceRegisteredRef = useRef(false);
  const resolveFinalFocus = useEffectEvent(() => getFinalFocus?.());

  const registerPanel = useCallback((panel: HTMLDivElement | null) => {
    panelRef.current = panel;
    if (!panel || !open || presenceRegisteredRef.current) {
      return;
    }

    previousFocusRef.current = document.activeElement instanceof HTMLElement
      ? document.activeElement
      : null;
    const focusables = listFocusable(panel);
    if (focusables.length > 0) {
      focusables[0]?.focus();
    } else {
      panel.focus();
    }
    presenceRegisteredRef.current = true;
    setModalPresence(true);
  }, [open]);

  useEffect(() => {
    if (!open) {
      return;
    }

    return () => {
      if (presenceRegisteredRef.current) {
        presenceRegisteredRef.current = false;
        setModalPresence(false);
      }
      const previousFocus = previousFocusRef.current;
      previousFocusRef.current = null;
      window.requestAnimationFrame(() => {
        // A setup flow may replace this dialog in the same render. Its newly
        // focused control owns the handoff; a delayed restore must not send
        // typing back into the obscured page.
        if (document.activeElement?.closest(`[${MODAL_PANEL_ATTRIBUTE}]`)) return;
        const finalFocus = resolveFinalFocus();
        if (finalFocus && finalFocus.isConnected && !finalFocus.matches(":disabled") && !finalFocus.closest("[inert]")) {
          finalFocus.focus();
          return;
        }
        if (previousFocus?.isConnected && !previousFocus.closest("[inert]")) {
          previousFocus.focus();
        }
      });
    };
  }, [open]);

  function handleKeyDown(event: ReactKeyboardEvent<HTMLDivElement>) {
    if (event.key === "Escape") {
      event.preventDefault();
      onClose();
      return;
    }
    if (event.key !== "Tab") {
      return;
    }

    const focusables = listFocusable(panelRef.current);
    if (focusables.length === 0) {
      event.preventDefault();
      panelRef.current?.focus();
      return;
    }

    const first = focusables[0];
    const last = focusables[focusables.length - 1];
    const active = document.activeElement;

    if (event.shiftKey && active === first) {
      event.preventDefault();
      last?.focus();
      return;
    }

    if (!event.shiftKey && active === last) {
      event.preventDefault();
      first?.focus();
    }
  }

  if (!open) {
    return null;
  }

  return (
    <FloatingPortal root={resolveOverlayContainer()}>
      <FloatingOverlay
        lockScroll
        className={cn(
          "fixed inset-0 flex items-center justify-center bg-black/55 p-4 backdrop-blur-sm",
          overlayClassName,
        )}
        style={{ zIndex: OVERLAY_Z_INDEX.modal }}
        onMouseDown={(event) => {
          if (event.target === event.currentTarget) {
            onClose();
          }
        }}
      >
        <div
          ref={registerPanel}
          role="dialog"
          tabIndex={-1}
          aria-label={ariaLabel}
          aria-labelledby={ariaLabelledBy}
          aria-modal="true"
          {...{ [MODAL_PANEL_ATTRIBUTE]: "true" }}
          className={panelClassName}
          onKeyDown={handleKeyDown}
        >
          {children}
        </div>
      </FloatingOverlay>
    </FloatingPortal>
  );
}

function listFocusable(container: HTMLElement | null): HTMLElement[] {
  if (!container) {
    return [];
  }

  return Array.from(
    container.querySelectorAll<HTMLElement>(
      'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])',
    ),
  ).filter((element) => !element.hasAttribute("disabled") && element.getAttribute("aria-hidden") !== "true");
}
