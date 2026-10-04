/// <reference types="vitest/config" />
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import path from "path";
import { developmentServer } from "./dev-backend";
import { testWorkerOptions } from "./test-workers";
import { testProjects } from "./test-projects";

export default defineConfig((environment) => ({
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
  server: developmentServer(environment, process.env),
  test: {
    ...testWorkerOptions(process.env.KINGDOM_TEST_CPUS),
    pool: "threads",
    globals: true,
    projects: testProjects,
  },
}));
