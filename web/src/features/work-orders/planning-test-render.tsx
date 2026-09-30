import { render as renderView, type RenderOptions } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { ReactElement } from "react";
import { beforeEach } from "vitest";

beforeEach(() => sessionStorage.clear());
export function render(ui: ReactElement, options?: RenderOptions) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return renderView(ui, { wrapper: ({ children }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>, ...options });
}
