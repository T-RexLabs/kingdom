import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";
import { CentralConnections } from "./connections";
import { SessionContext } from "./session-context";
const { centralFetch } = vi.hoisted(() => ({ centralFetch: vi.fn() }));
vi.mock("@/api/client", async () => ({ ...await vi.importActual<typeof import("@/api/client")>("@/api/client"), centralFetch }));
afterEach(()=>centralFetch.mockReset());
function view(owner=false) {
 const client=new QueryClient({defaultOptions:{queries:{retry:false},mutations:{retry:false}}});
 render(<QueryClientProvider client={client}><SessionContext.Provider value={{session_id:"session",csrf_token:"csrf",user:{id:"user",display_name:"User",team_owner:owner}}}><CentralConnections projects={[{id:"project",display_name:"Project"}]}/></SessionContext.Provider></QueryClientProvider>);
}
it("keeps uncertain API-key mutation bytes and key stable without browser storage",async()=>{
 const user=userEvent.setup(); let mutations=0;
 const store=vi.spyOn(Storage.prototype,"setItem");
 centralFetch.mockImplementation(async(_path:string,options?:RequestInit)=>{if(!options?.method)return []; if(++mutations===1)throw new Error("lost reply");return {};});
 view();
 await user.type(screen.getByLabelText("Connection Name"),"Personal");
 await user.type(screen.getByLabelText("API Key"),"fixture-secret");
 await user.click(screen.getByRole("button",{name:"Add Connection"}));
 const alert=await screen.findByRole("alert"); expect(alert).toHaveFocus();
 expect(screen.getByLabelText("API Key")).toHaveValue("");
 expect(screen.getByLabelText("Connection Name")).toBeDisabled();
 const first=centralFetch.mock.calls.find(call=>call[1]?.method==="POST");
 await user.click(screen.getByRole("button",{name:"Retry Same Request"}));
 await waitFor(()=>expect(screen.queryByRole("button",{name:"Retry Same Request"})).not.toBeInTheDocument());
 const requests=centralFetch.mock.calls.filter(call=>call[1]?.method==="POST");
 expect(requests[1]).toEqual(first);
 expect(JSON.parse(first?.[1].body)).toMatchObject({scope:"personal",credential:"fixture-secret",definition:{auth_kind:"api_key"}});
 expect(store).not.toHaveBeenCalled(); store.mockRestore();
 expect(screen.queryByText("fixture-secret")).not.toBeInTheDocument();
});
it("offers team connections only to team owners and keeps replacement revision exact",async()=>{
 const user=userEvent.setup();
 centralFetch.mockImplementation(async(_path:string,options?:RequestInit)=>options?.method?{}:[{id:"team",owner_id:"other",scope:"team",revision:4,disabled:false,definition:{label:"Team",provider:"openai",auth_kind:"api_key"}}]);
 view(true);
 await user.click(await screen.findByRole("button",{name:"Replace Key"}));
 await user.type(screen.getByLabelText("API Key"),"replacement-secret");
 await user.click(screen.getByRole("button",{name:"Save Replacement Key"}));
 await waitFor(()=>expect(centralFetch.mock.calls.some(call=>call[1]?.method==="PUT")).toBe(true));
 const request=centralFetch.mock.calls.find(call=>call[1]?.method==="PUT");
 expect(request?.[0]).toBe("/connections/team");
 expect(JSON.parse(request?.[1].body)).toMatchObject({expected_revision:4,credential:"replacement-secret"});
});
