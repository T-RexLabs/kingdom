import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";
import { ProjectInput } from "./project-input";
import { SessionContext } from "./session-context";
const { centralFetch }=vi.hoisted(()=>({centralFetch:vi.fn()}));
vi.mock("@/api/client",async()=>({...await vi.importActual<typeof import("@/api/client")>("@/api/client"),centralFetch}));
afterEach(()=>centralFetch.mockReset());
function view(){render(<QueryClientProvider client={new QueryClient({defaultOptions:{queries:{retry:false}}})}><SessionContext.Provider value={{session_id:"session",csrf_token:"csrf",user:{id:"owner",display_name:"Owner",team_owner:true}}}><ProjectInput projects={[{id:"project",display_name:"Project"}]}/></SessionContext.Provider></QueryClientProvider>)}
it("does not offer replacement after first startup",async()=>{centralFetch.mockResolvedValue({state:"started"});view();await userEvent.selectOptions(screen.getByLabelText("Project"),"project");expect(await screen.findByText("This project has started. Initial uploads are closed.")).toBeInTheDocument();expect(screen.queryByLabelText("File")).not.toBeInTheDocument()});
it("retries identical file and key when upload result is uncertain",async()=>{
 let writes=0;centralFetch.mockImplementation(async(_path:string,options?:RequestInit)=>{if(options?.method){if(++writes===1)throw new Error("lost reply");return {}}return {state:writes>1?"imported":"empty",can_upload:writes<=1}});
 const user=userEvent.setup();view();await user.selectOptions(screen.getByLabelText("Project"),"project");await user.upload(await screen.findByLabelText("File"),new File(["fixture"],"input.zip",{type:"application/zip"}));await user.click(screen.getByRole("button",{name:"Add Initial Files"}));await user.click(await screen.findByRole("button",{name:"Retry Same Input"}));await waitFor(()=>expect(writes).toBe(2));const requests=centralFetch.mock.calls.filter(call=>call[1]?.method==="POST");expect(requests[0]).toEqual(requests[1]);
});

it("retries the exact configured repository reference without offering replacement",async()=>{
 let writes=0;centralFetch.mockImplementation(async(path:string,options?:RequestInit)=>{if(options?.method){if(++writes===1)throw new Error("lost reply");return {}}if(path.endsWith("/repository"))return {available:true};return {state:writes>1?"imported":"empty",can_upload:writes<=1}});
 const user=userEvent.setup();view();await user.selectOptions(screen.getByLabelText("Project"),"project");await user.selectOptions(await screen.findByLabelText("Input Type"),"github");await user.clear(screen.getByLabelText("Branch, Tag, or Commit"));await user.type(screen.getByLabelText("Branch, Tag, or Commit"),"release");await user.click(screen.getByRole("button",{name:"Add Initial Files"}));await user.click(await screen.findByRole("button",{name:"Retry Same Input"}));await waitFor(()=>expect(writes).toBe(2));const requests=centralFetch.mock.calls.filter(call=>call[1]?.method==="POST");expect(requests[0]).toEqual(requests[1]);expect(requests[0][0]).toBe("/projects/project/repository/import");expect(JSON.parse(requests[0][1].body)).toEqual({ref:"release"});await waitFor(()=>expect(screen.queryByLabelText("Branch, Tag, or Commit")).not.toBeInTheDocument());
});
