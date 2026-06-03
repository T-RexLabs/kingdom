import { useEffect, useState, useCallback } from "react";
import { useNavigate } from "@tanstack/react-router";

interface Shortcut {
  keys: string;
  label: string;
  description: string;
  action: () => void;
}

/**
 * Keyboard shortcut system using vim-style "g + key" navigation
 * and "?" for help overlay toggle.
 *
 * Navigation shortcuts (g prefix):
 *   g h  - Go to Throne Room (home/dashboard)
 *   g d  - Go to Decrees
 *   g r  - Go to Runs
 *   g g  - Go to Agents
 *   g a  - Go to Realm Analytics
 *
 * Global:
 *   ?    - Toggle keyboard shortcuts help
 *   Esc  - Close help overlay / close panels
 */
export function useKeyboardShortcuts() {
  const [helpOpen, setHelpOpen] = useState(false);
  const [pendingG, setPendingG] = useState(false);
  const navigate = useNavigate();

  const toggleHelp = useCallback(() => setHelpOpen((v) => !v), []);
  const closeHelp = useCallback(() => setHelpOpen(false), []);

  const shortcuts: Shortcut[] = [
    { keys: "g h", label: "g h", description: "Go to Throne Room", action: () => navigate({ to: "/" }) },
    { keys: "g d", label: "g d", description: "Go to Decrees", action: () => navigate({ to: "/decrees" }) },
    { keys: "g r", label: "g r", description: "Go to Runs", action: () => navigate({ to: "/runs" }) },
    { keys: "g g", label: "g g", description: "Go to Agents", action: () => navigate({ to: "/agents" }) },
    { keys: "g a", label: "g a", description: "Go to Realm Analytics", action: () => navigate({ to: "/realm-analytics" }) },
    { keys: "?", label: "?", description: "Toggle shortcuts help", action: toggleHelp },
  ];

  useEffect(() => {
    let gTimer: ReturnType<typeof setTimeout> | null = null;

    function handleKeyDown(e: KeyboardEvent) {
      // Don't capture when user is typing in an input/textarea/contenteditable
      const target = e.target as HTMLElement;
      if (
        target.tagName === "INPUT" ||
        target.tagName === "TEXTAREA" ||
        target.tagName === "SELECT" ||
        target.isContentEditable
      ) {
        return;
      }

      // Escape closes help
      if (e.key === "Escape") {
        setHelpOpen(false);
        setPendingG(false);
        return;
      }

      // "?" toggles help (Shift+/ on US layout, or literal "?")
      if (e.key === "?") {
        e.preventDefault();
        toggleHelp();
        return;
      }

      // Handle "g" prefix for navigation
      if (pendingG) {
        setPendingG(false);
        if (gTimer) clearTimeout(gTimer);

        const navMap: Record<string, string> = {
          h: "/",
          d: "/decrees",
          r: "/runs",
          g: "/agents",
          a: "/realm-analytics",
        };

        const target = navMap[e.key];
        if (target) {
          e.preventDefault();
          navigate({ to: target });
          setHelpOpen(false);
        }
        return;
      }

      // Start "g" prefix mode
      if (e.key === "g" && !e.ctrlKey && !e.metaKey && !e.altKey) {
        setPendingG(true);
        // Auto-cancel after 1.5s
        gTimer = setTimeout(() => setPendingG(false), 1500);
        return;
      }
    }

    window.addEventListener("keydown", handleKeyDown);
    return () => {
      window.removeEventListener("keydown", handleKeyDown);
      if (gTimer) clearTimeout(gTimer);
    };
  }, [pendingG, navigate, toggleHelp]);

  return { helpOpen, closeHelp, shortcuts, pendingG };
}
