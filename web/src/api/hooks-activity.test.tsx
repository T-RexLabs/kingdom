import type { PropsWithChildren } from "react";
import { act,renderHook,waitFor } from "@testing-library/react";
import { QueryClient,QueryClientProvider } from "@tanstack/react-query";
import { beforeEach,describe,expect,it,vi } from "vitest";
import { apiFetch } from "./client";
import { useActivityHistory } from "./hooks-activity";
import type { AcpRunActivityItem,AcpRunActivityResponse } from "./hooks-types";

vi.mock("./client",()=>({apiFetch:vi.fn(),shouldRetryApiQuery:()=>false}));
const fetchMock=vi.mocked(apiFetch);
const item=(sequence:number):AcpRunActivityItem=>({id:`call-${sequence}`,event_id:`event-${sequence}`,sequence,kind:"tool",label:"Read file",occurred_at:"2026-09-30T10:00:00Z",recorded_at:"2026-09-30T10:00:01Z",input:{availability:"not_provided"},output:{availability:"not_provided"},reasoning:{availability:"not_provided"},message:{availability:"not_provided"}});
const page=(overrides:Partial<AcpRunActivityResponse>={}):AcpRunActivityResponse=>({items:[item(3),item(4)],direction:"earlier",range_after_sequence:2,range_through_sequence:4,snapshot_through_sequence:4,remaining_count:2,next_cursor:"older-2",resume_cursor:"resume-4",...overrides});
function wrapper(){const client=new QueryClient({defaultOptions:{queries:{retry:false},mutations:{retry:false}}});return function Wrapper({children}:PropsWithChildren){return <QueryClientProvider client={client}>{children}</QueryClientProvider>}}

describe("bounded activity queries",()=>{
  beforeEach(()=>fetchMock.mockReset());
  it("polls only the independent delta cursor while an earlier page is pending",async()=>{
    let finishOlder!:(value:AcpRunActivityResponse)=>void;
    fetchMock.mockResolvedValueOnce(page());
    const hook=renderHook(()=>useActivityHistory("/runs/run/executions/attempt/activity",["activity","attempt"],{refetchIntervalMs:false}),{wrapper:wrapper()});
    await waitFor(()=>expect(hook.result.current.data?.items).toHaveLength(2));
    fetchMock.mockImplementationOnce(()=>new Promise(resolve=>{finishOlder=resolve}));
    let older:Promise<unknown>;
    act(()=>{older=hook.result.current.loadMore("older-2")});
    await waitFor(()=>expect(fetchMock).toHaveBeenCalledTimes(2));
    fetchMock.mockResolvedValueOnce(page({items:[item(5)],direction:"later",range_after_sequence:4,range_through_sequence:5,snapshot_through_sequence:5,resume_cursor:"resume-5",next_cursor:undefined,remaining_count:0}));
    await act(async()=>{await hook.result.current.refetch()});
    expect(fetchMock.mock.calls[2][1]?.params?.cursor).toBe("resume-4");
    expect(hook.result.current.gaps[0].count).toBe(2);
    await act(async()=>{finishOlder(page({items:[item(1),item(2)],range_after_sequence:0,range_through_sequence:2,remaining_count:0,next_cursor:undefined}));await older!});
    await waitFor(()=>expect(hook.result.current.data?.items.map(value=>value.sequence)).toEqual([1,2,3,4,5]));
    expect(hook.result.current.data?.resumeCursor).toBe("resume-5");
    expect(hook.result.current.gaps).toEqual([]);
  });
  it("keeps completed-attempt history isolated when a previous request resolves after switching",async()=>{
    let finishOlder!:(value:AcpRunActivityResponse)=>void;
    fetchMock.mockResolvedValueOnce(page());
    const hook=renderHook(({attempt})=>useActivityHistory(`/runs/run/executions/${attempt}/activity`,["activity",attempt],{refetchIntervalMs:false}),{initialProps:{attempt:"one"},wrapper:wrapper()});
    await waitFor(()=>expect(hook.result.current.data?.items).toHaveLength(2));
    fetchMock.mockImplementationOnce(()=>new Promise(resolve=>{finishOlder=resolve}));
    let pending:Promise<unknown>;
    act(()=>{pending=hook.result.current.loadMore("older-2")});
    await waitFor(()=>expect(fetchMock).toHaveBeenCalledTimes(2));
    fetchMock.mockResolvedValueOnce(page({items:[{...item(1),id:"other-call",event_id:"other-event"}],range_after_sequence:0,range_through_sequence:1,snapshot_through_sequence:1,remaining_count:0,next_cursor:undefined,resume_cursor:"other-resume"}));
    hook.rerender({attempt:"two"});
    await waitFor(()=>expect(hook.result.current.data?.items[0].event_id).toBe("other-event"));
    await act(async()=>{finishOlder(page({items:[item(1),item(2)],range_after_sequence:0,range_through_sequence:2,remaining_count:0,next_cursor:undefined}));await pending!});
    expect(hook.result.current.data?.items.map(value=>value.event_id)).toEqual(["other-event"]);
    expect(hook.result.current.data?.resumeCursor).toBe("other-resume");
  });
});
