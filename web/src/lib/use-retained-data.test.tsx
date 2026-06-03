import { renderHook } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { useRetainedData } from "./use-retained-data";

describe("useRetainedData", () => {
  it("keeps the last successful payload visible while a new identity is fetching", () => {
    type HookProps = {
      identity: string;
      data: { items: string[] } | undefined;
      isPending: boolean;
      isFetching: boolean;
    };

    const hook = renderHook(
      ({ identity, data, isPending, isFetching }: HookProps) => useRetainedData({ identity, data, isPending, isFetching }),
      {
        initialProps: {
          identity: "all",
          data: { items: ["run-1"] },
          isPending: false,
          isFetching: false,
        } as HookProps,
      },
    );

    hook.rerender({
      identity: "running",
      data: undefined,
      isPending: true,
      isFetching: true,
    } as HookProps);

    expect(hook.result.current.displayData).toEqual({ items: ["run-1"] });
    expect(hook.result.current.isInitialLoading).toBe(false);
    expect(hook.result.current.isRetainingPreviousData).toBe(true);
  });

  it("drops retained data once the new identity settles without data", () => {
    type HookProps = {
      identity: string;
      data: { items: string[] } | undefined;
      isPending: boolean;
      isFetching: boolean;
    };

    const hook = renderHook(
      ({ identity, data, isPending, isFetching }: HookProps) => useRetainedData({ identity, data, isPending, isFetching }),
      {
        initialProps: {
          identity: "all",
          data: { items: ["run-1"] },
          isPending: false,
          isFetching: false,
        } as HookProps,
      },
    );

    hook.rerender({
      identity: "running",
      data: undefined,
      isPending: false,
      isFetching: false,
    } as HookProps);

    expect(hook.result.current.displayData).toBeUndefined();
    expect(hook.result.current.isInitialLoading).toBe(false);
    expect(hook.result.current.isRetainingPreviousData).toBe(false);
  });

  it("does not report a retained transition during same-identity refetch", () => {
    type HookProps = {
      identity: string;
      data: { items: string[] } | undefined;
      isPending: boolean;
      isFetching: boolean;
    };

    const hook = renderHook(
      ({ identity, data, isPending, isFetching }: HookProps) => useRetainedData({ identity, data, isPending, isFetching }),
      {
        initialProps: {
          identity: "all",
          data: { items: ["run-1"] },
          isPending: false,
          isFetching: false,
        } as HookProps,
      },
    );

    hook.rerender({
      identity: "all",
      data: { items: ["run-1"] },
      isPending: false,
      isFetching: true,
    } as HookProps);

    expect(hook.result.current.displayData).toEqual({ items: ["run-1"] });
    expect(hook.result.current.isInitialLoading).toBe(false);
    expect(hook.result.current.isRetainingPreviousData).toBe(false);
  });

  it("does not report a retained transition once the next identity already has data", () => {
    type HookProps = {
      identity: string;
      data: { items: string[] } | undefined;
      isPending: boolean;
      isFetching: boolean;
    };

    const hook = renderHook(
      ({ identity, data, isPending, isFetching }: HookProps) => useRetainedData({ identity, data, isPending, isFetching }),
      {
        initialProps: {
          identity: "all",
          data: { items: ["run-1"] },
          isPending: false,
          isFetching: false,
        } as HookProps,
      },
    );

    hook.rerender({
      identity: "running",
      data: { items: ["run-2"] },
      isPending: false,
      isFetching: true,
    } as HookProps);

    expect(hook.result.current.displayData).toEqual({ items: ["run-2"] });
    expect(hook.result.current.isInitialLoading).toBe(false);
    expect(hook.result.current.isRetainingPreviousData).toBe(false);
  });
});
