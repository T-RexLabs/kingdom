/// <reference types="vitest/config" />
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import path from "path";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
    rollupOptions: {
      output: {
        manualChunks: {
          // Vendor: TanStack (router + query)
          "vendor-tanstack": [
            "@tanstack/react-router",
            "@tanstack/react-query",
          ],
          // Vendor: D3 (heavy — only needed by DAG + charts)
          "vendor-d3": ["d3"],
          // Vendor: UI utilities
          "vendor-ui": [
            "class-variance-authority",
            "clsx",
            "tailwind-merge",
            "lucide-react",
          ],
        },
      },
    },
  },
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: "http://localhost:8080",
        changeOrigin: false,
      },
      "/healthz": {
        target: "http://localhost:8080",
        changeOrigin: false,
      },
    },
  },
  test: {
    globals: true,
    environment: "jsdom",
    setupFiles: "./src/test/setup.ts",
  },
});
