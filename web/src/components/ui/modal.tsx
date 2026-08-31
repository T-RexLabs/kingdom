import { type ReactNode, useId } from "react";
import { Button } from "@/components/ui/button";
import { ModalDialog } from "@/components/ui/modal-dialog";
import { ModalShell } from "@/components/ui/modal-shell-components";
import { modalShellSizeClass, type ModalShellHeight, type ModalShellSize } from "@/components/ui/modal-shell-styles";
import { cn } from "@/lib/utils";

/**
 * The one way to put a dialog on screen.
 *
 * `ModalDialog` owns behavior — portal, backdrop, focus containment, scroll
 * lock — and `ModalShell` owns chrome. Composing them here means no call site
 * derives its own panel width, backdrop padding, or accessible name, which is
 * what let thirteen dialogs that all use the same base still drift apart. Size
 * is a prop precisely because modals legitimately differ in size; everything
 * else about them should not.
 */
export function Modal({
  open,
  onClose,
  title,
  caption,
  description,
  footer,
  children,
  size = "md",
  height = "auto",
  bodyClassName,
  overlayClassName,
  getFinalFocus,
}: {
  open: boolean;
  onClose: () => void;
  title: ReactNode;
  caption?: ReactNode;
  description?: ReactNode;
  footer?: ReactNode;
  children: ReactNode;
  size?: ModalShellSize;
  height?: ModalShellHeight;
  bodyClassName?: string;
  overlayClassName?: string;
  getFinalFocus?: () => HTMLElement | null;
}) {
  // Named by the heading the operator can actually see. Call sites used to pass
  // a separate `ariaLabel` string, so screen readers heard a label that no
  // longer matched the title beside it.
  const titleId = useId();

  return (
    <ModalDialog
      open={open}
      onClose={onClose}
      ariaLabelledBy={titleId}
      panelClassName={cn("w-full min-w-0", modalShellSizeClass(size))}
      overlayClassName={overlayClassName}
      getFinalFocus={getFinalFocus}
    >
      <ModalShell
        title={title}
        titleId={titleId}
        caption={caption}
        description={description}
        onClose={onClose}
        footer={footer}
        height={height}
        bodyClassName={bodyClassName}
      >
        {children}
      </ModalShell>
    </ModalDialog>
  );
}

/** The canonical modal action row: trailing, wrapping, evenly spaced. */
export function ModalFooter({ children }: { children: ReactNode }) {
  return <div className="flex flex-wrap items-center justify-end gap-2">{children}</div>;
}

/**
 * A modal that asks one question and takes one answer.
 *
 * Confirmations are the dialogs most likely to diverge, because each one is
 * written where its action lives. Routing them through a single component
 * fixes the order (dismiss, then confirm), the sizing, and the shape of the
 * ask, and leaves each caller only the words that are actually specific to it.
 */
export function ConfirmDialog({
  open,
  onClose,
  onConfirm,
  title,
  caption,
  description,
  confirmLabel,
  dismissLabel = "Cancel",
  tone = "destructive",
  confirmDisabled = false,
  dismissDisabled = false,
  children,
}: {
  open: boolean;
  onClose: () => void;
  onConfirm: () => void;
  title: ReactNode;
  caption?: ReactNode;
  description?: ReactNode;
  confirmLabel: ReactNode;
  dismissLabel?: ReactNode;
  tone?: "destructive" | "default";
  confirmDisabled?: boolean;
  dismissDisabled?: boolean;
  children?: ReactNode;
}) {
  return (
    <Modal
      open={open}
      onClose={onClose}
      title={title}
      caption={caption}
      description={description}
      size="sm"
      footer={
        <ModalFooter>
          <Button type="button" variant="ghost" onClick={onClose} disabled={dismissDisabled}>
            {dismissLabel}
          </Button>
          <Button type="button" variant={tone} onClick={onConfirm} disabled={confirmDisabled}>
            {confirmLabel}
          </Button>
        </ModalFooter>
      }
    >
      {children}
    </Modal>
  );
}
