import { useEffect, useLayoutEffect, useState, useSyncExternalStore, type ReactNode } from "react";
import { ThemeContext } from "./context";
import { ThemeController, type ThemeClient } from "./controller";
import { applyPresentation } from "./presentation";

export function ThemeProvider({ children, client }: { children: ReactNode; client?: ThemeClient }) {
  const [controller] = useState(() => new ThemeController(client));
  const state = useSyncExternalStore(controller.subscribe, controller.getSnapshot);
  const { theme, assetURLs, activePackage } = state;
  useLayoutEffect(() => applyPresentation({ theme, assetURLs, activePackage }), [theme, assetURLs, activePackage]);
  useEffect(() => { void controller.initialize(); return controller.dispose; }, [controller]);
  return (
    <ThemeContext value={{ ...state, refreshCatalog: controller.refreshCatalog, selectTheme: controller.selectTheme, previewTheme: controller.previewTheme, cancelPreview: controller.cancelPreview }}>
      {state.loading ? <div className="theme-loading" role="status" aria-live="polite"><span className="theme-loading-mark" aria-hidden="true" /><span>Loading your workspace…</span></div> : children}
    </ThemeContext>
  );
}
