import type { ReactNode } from "react";
import { PageContent, PageHeader } from "./page-layout";

/** Shared hierarchy for collection/settings screens; workflow and chat own
 * their specialized workspaces while retaining the same shell and tokens. */
export function WorkspacePage({
  section,
  title,
  description,
  actions,
  children,
}: {
  section: string;
  title: string;
  description?: string;
  actions?: ReactNode;
  children: ReactNode;
}) {
  return (
    <div className="flex h-full min-h-0 flex-col">
      <PageHeader>
        <span className="text-sm text-foreground-muted">{section}</span>
      </PageHeader>
      <PageContent className="py-8 sm:py-10">
        <div className="mx-auto w-full max-w-6xl">
          <div className="mb-8 flex flex-wrap items-start justify-between gap-5">
            <div>
              <h1 className="font-display text-4xl leading-tight sm:text-[2.65rem]">
                {title}
              </h1>
              {description && (
                <p className="mt-3 max-w-2xl text-sm leading-relaxed text-foreground-muted">
                  {description}
                </p>
              )}
            </div>
            {actions && (
              <div className="flex flex-wrap items-center gap-2">{actions}</div>
            )}
          </div>
          {children}
        </div>
      </PageContent>
    </div>
  );
}
