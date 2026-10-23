import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";
import { PublishResult } from "./publish-result";
const { centralFetch }=vi.hoisted(()=>({centralFetch:vi.fn()}));
vi.mock("@/api/client",async()=>({...await vi.importActual<typeof import("@/api/client")>("@/api/client"),centralFetch}));
vi.mock("@/lib/runtime-mode",()=>({appBase:()=>"/projects/project"}));
afterEach(()=>{cleanup();centralFetch.mockReset()});
function view(){render(<QueryClientProvider client={new QueryClient({defaultOptions:{queries:{retry:false}}})}><PublishResult runID="run"/></QueryClientProvider>)}
it("publishes only after explicit confirmation and retries the exact request",async()=>{
 let writes=0;centralFetch.mockImplementation(async(path:string,options?:RequestInit)=>{if(options?.method){if(++writes===1)throw new Error("lost reply");return {id:"publication",state:"pending",branch:"kingdom/publication"}}return path.includes("publications?")?[]:{available:true,can_publish:true}});
 const user=userEvent.setup();view();await user.click(await screen.findByRole("button",{name:"Publish to GitHub"}));expect(writes).toBe(0);await user.click(screen.getByRole("button",{name:"Create Pull Request"}));await user.click(await screen.findByRole("button",{name:"Retry Same Publication"}));await waitFor(()=>expect(writes).toBe(2));const requests=centralFetch.mock.calls.filter(call=>call[1]?.method==="POST");expect(requests[0]).toEqual(requests[1]);expect(JSON.parse(requests[0][1].body)).toEqual({run_id:"run",base_branch:"main",title:"Publish Kingdom Result",body:""});
});
it("recovers a saved pending request after reload instead of creating another",async()=>{
 centralFetch.mockImplementation(async(path:string,options?:RequestInit)=>options?.method?{id:"saved",state:"pending",branch:"kingdom/saved"}:path.includes("publications?")?[{id:"saved",state:"pending",branch:"kingdom/saved"}]:{available:true,can_publish:true});
 const user=userEvent.setup();view();await user.click(await screen.findByRole("button",{name:"Publish to GitHub"}));await screen.findByRole("button",{name:"Check Saved Publication"});expect(screen.getByRole("button",{name:"Create Pull Request"})).toBeDisabled();await user.click(screen.getByRole("button",{name:"Check Saved Publication"}));await waitFor(()=>expect(centralFetch).toHaveBeenCalledWith("/projects/project/repository/publications/saved/reconcile",{method:"POST"}));
});
it("does not offer publication without an operator write grant",async()=>{centralFetch.mockResolvedValue({available:true,can_publish:false});view();await waitFor(()=>expect(centralFetch).toHaveBeenCalled());expect(screen.queryByRole("button",{name:"Publish to GitHub"})).not.toBeInTheDocument()});
