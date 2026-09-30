import { createContext, useContext } from "react";
import type { ThemeController, ThemeState } from "./controller";
import type { CopySlot, TokenSlot } from "./contract";
import { roleIdentity } from "./contract";

export type ThemeContextValue = ThemeState & Pick<ThemeController, "refreshCatalog" | "selectTheme" | "previewTheme" | "cancelPreview">;
export const ThemeContext = createContext<ThemeContextValue | null>(null);
export function useTheme() {
  const context = useContext(ThemeContext);
  if (!context) throw new Error("useTheme must be used within ThemeProvider");
  return context;
}
export function useThemeToken(slot: TokenSlot): string { return useTheme().theme.tokens[slot]; }
export function useThemeCopy(slot: CopySlot): string { return useTheme().theme.copy[slot]; }
export function useRoleIdentity(role: string) { return roleIdentity(useTheme().theme, role); }
