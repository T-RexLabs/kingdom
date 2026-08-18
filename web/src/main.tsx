import { StrictMode, Suspense } from "react";
import { createRoot } from "react-dom/client";
import { RouterProvider } from "@tanstack/react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { ErrorBoundary } from "@/components/error-boundary";
import { NotificationProvider } from "@/components/notifications";
import { FormalizeNotifierMount, GlobalFormalizeProvider } from "@/features/decrees/use-global-formalize";
import { RunsCollectionEventStreamMount } from "@/features/runs/use-runs-collection-event-stream";
import { SCENE_ROOT_ID } from "@/lib/overlay-system";
import { router } from "./router";
import { LocalSessionBoundary } from "@/features/local-session/local-session-boundary";
import { shouldRetryApiMutation, shouldRetryApiQuery } from "@/api/client";
import "./index.css";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 2_000,
      retry: (failureCount, error) => shouldRetryApiQuery(failureCount, error, 1),
    },
    mutations: {
      retry: (failureCount, error) => shouldRetryApiMutation(failureCount, error, 2),
    },
  },
});

createRoot(document.getElementById(SCENE_ROOT_ID)!).render(
  <StrictMode>
    <ErrorBoundary>
      <QueryClientProvider client={queryClient}>
        <NotificationProvider>
          <LocalSessionBoundary><GlobalFormalizeProvider>
            {/* Headless side-effect component: subscribes to the
                tracked formalize operation's stream and fires the
                terminal toast (Plan Ready / Plan Failed / Plan
                Generation Cancelled). Mounted exactly once at the
                app root so the latch lives across route changes. */}
            <FormalizeNotifierMount />
            <RunsCollectionEventStreamMount />
            <Suspense
              fallback={
                <div className="flex h-screen items-center justify-center bg-background">
                  <div className="h-10 w-10 animate-spin rounded-full border-2 border-gold border-t-transparent" />
                </div>
              }
            >
              <RouterProvider router={router} />
            </Suspense>
          </GlobalFormalizeProvider></LocalSessionBoundary>
        </NotificationProvider>
      </QueryClientProvider>
    </ErrorBoundary>
  </StrictMode>
);
