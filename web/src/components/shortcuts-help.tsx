import { Keyboard } from "lucide-react";
import { MODAL_BODY_TEXT_CLASS, Modal } from "@/components/ui/modal-shell";

interface Shortcut {
  keys: string;
  label: string;
  description: string;
}

interface ShortcutsHelpProps {
  open: boolean;
  onClose: () => void;
  shortcuts: Shortcut[];
  pendingG: boolean;
}

/**
 * Modal overlay showing available keyboard shortcuts.
 */
export function ShortcutsHelp({ open, onClose, shortcuts, pendingG }: ShortcutsHelpProps) {
  if (!open && !pendingG) return null;

  // Just the "g" pending indicator (small toast)
  if (!open && pendingG) {
    return (
      <div className="fixed bottom-4 right-4 z-[60] rounded-lg border border-gold/30 bg-surface px-3 py-2 shadow-lg">
        <span className="font-mono text-sm text-gold">g</span>
        <span className="ml-2 text-xs text-foreground-muted">waiting for key...</span>
      </div>
    );
  }

  return (
		<Modal
			open={open}
			onClose={onClose}
			size="sm"
			title={<span className="flex items-center gap-2"><Keyboard className="h-5 w-5 text-gold" /> Keyboard Shortcuts</span>}
			footer={<p className="text-xs text-foreground-subtle">Press <Kbd>?</Kbd> to toggle this overlay</p>}
		>
	          <div className="mb-3">
	            <h4 className="mb-2 text-xs font-medium uppercase tracking-wider text-foreground-subtle">
	              Navigation
            </h4>
            <div className="space-y-2">
              {shortcuts
                .filter((s) => s.keys.startsWith("g"))
                .map((s) => (
                  <ShortcutRow key={s.keys} shortcut={s} />
                ))}
            </div>
          </div>
          <div>
            <h4 className="mb-2 text-xs font-medium uppercase tracking-wider text-foreground-subtle">
              General
            </h4>
            <div className="space-y-2">
              {shortcuts
                .filter((s) => !s.keys.startsWith("g"))
                .map((s) => (
                  <ShortcutRow key={s.keys} shortcut={s} />
                ))}
	              <ShortcutRow shortcut={{ keys: "Esc", label: "Esc", description: "Close overlay / cancel" }} />
	            </div>
	          </div>
		</Modal>
  );
}

function ShortcutRow({ shortcut }: { shortcut: Shortcut }) {
  const keys = shortcut.label.split(" ");
  return (
    <div className="flex items-center justify-between">
      <span className={MODAL_BODY_TEXT_CLASS}>{shortcut.description}</span>
      <div className="flex items-center gap-1">
        {keys.map((k, i) => (
          <Kbd key={i}>{k}</Kbd>
        ))}
      </div>
    </div>
  );
}

function Kbd({ children }: { children: React.ReactNode }) {
  return (
    <kbd className="inline-flex min-w-[1.5rem] items-center justify-center rounded border border-border bg-background px-1.5 py-0.5 font-mono text-xs text-foreground">
      {children}
    </kbd>
  );
}
