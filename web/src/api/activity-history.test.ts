import { describe,expect,it } from "vitest";
import type { AcpRunActivityResponse } from "./hooks-types";
import {mergeActivityPage} from "./activity-history";

const page=(overrides:Partial<AcpRunActivityResponse>):AcpRunActivityResponse=>({items:[],direction:"earlier",remaining_count:0,range_after_sequence:0,range_through_sequence:100,snapshot_through_sequence:100,resume_cursor:"resume-100",...overrides});

describe("activity snapshot ranges",()=>{
  it("keeps an older unloaded gap while new activity arrives and older pages load concurrently",()=>{
    const first=mergeActivityPage(undefined,page({range_after_sequence:50,remaining_count:50,next_cursor:"older-50"}));
    const live=mergeActivityPage(first,page({direction:"later",range_after_sequence:100,range_through_sequence:130,snapshot_through_sequence:150,resume_cursor:"resume-150",remaining_count:20,next_cursor:"delta-130"}),"resume-100");
    const older=mergeActivityPage(live,page({range_after_sequence:20,range_through_sequence:50,remaining_count:20,next_cursor:"older-20"}),"older-50");
    expect(older.resumeCursor).toBe("resume-150");
    expect(older.ranges).toEqual([{after:20,through:130}]);
    expect(older.gaps.map(gap=>[gap.after,gap.through,gap.count])).toEqual([[0,20,20],[130,150,20]]);
    const caughtUp=mergeActivityPage(older,page({direction:"later",range_after_sequence:130,range_through_sequence:150,snapshot_through_sequence:150,resume_cursor:"resume-150"}),"delta-130");
    expect(caughtUp.gaps).toHaveLength(1);
    expect(mergeActivityPage(caughtUp,page({range_after_sequence:20,range_through_sequence:50,remaining_count:20,next_cursor:"older-20"}),"older-50")).toBe(caughtUp);
  });
  it("keeps an empty resume cursor reusable for a future observation",()=>{
    const first=mergeActivityPage(undefined,page({}));
    const empty=mergeActivityPage(first,page({direction:"later",range_after_sequence:100}),"resume-100");
    const next=mergeActivityPage(empty,page({direction:"later",range_after_sequence:100,range_through_sequence:101,snapshot_through_sequence:101,resume_cursor:"resume-101"}),"resume-100");
    expect(next.ranges).toEqual([{after:0,through:101}]);
    expect(next.resumeCursor).toBe("resume-101");
  });
});
