import { render as renderView, type RenderOptions } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { ReactElement } from "react";
import { ThemeContext } from "@/theme/context";
import { ThemeController } from "@/theme/controller";
import { beforeEach } from "vitest";

beforeEach(() => sessionStorage.clear());
export function render(ui: ReactElement, options?: RenderOptions) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  const theme = new ThemeController();
  const appearance = { ...theme.getSnapshot(), refreshCatalog: theme.refreshCatalog, selectTheme: theme.selectTheme, previewTheme: theme.previewTheme, cancelPreview: theme.cancelPreview };
  return renderView(ui, { wrapper: ({ children }) => <QueryClientProvider client={client}><ThemeContext.Provider value={appearance}>{children}</ThemeContext.Provider></QueryClientProvider>, ...options });
}
