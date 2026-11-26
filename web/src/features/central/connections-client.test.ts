import { afterEach, describe, expect, it, vi } from "vitest";
import { hostedProviderModels } from "./connections-client";
import { selectedModelPayload } from "@/features/work-orders/provider-model-selection";
const { centralFetch } = vi.hoisted(() => ({ centralFetch: vi.fn() }));
vi.mock("@/api/client", () => ({ centralFetch }));
afterEach(() => { document.head.innerHTML=""; centralFetch.mockReset(); });
function hosted() {
  document.head.innerHTML='<meta name="kingdom-runtime-mode" content="hosted-project"><meta name="kingdom-app-base" content="/projects/11111111-1111-4111-8111-111111111111">';
}
describe("hosted exact model selections", () => {
 it("retains two connections with the same model and builds exact run role tuples", async () => {
  hosted();
  centralFetch.mockImplementation(async (path:string) => path==="/connections" ? [
    {id:"personal",revision:2,definition:{provider:"openai",label:"Mine"}},
    {id:"team",revision:7,definition:{provider:"openai",label:"Team"}},
  ] : {connection_id:path.includes("/personal/")?"personal":"team",revision:path.includes("/personal/")?2:7,expires_at:new Date(Date.now()+60000).toISOString(),models:[{id:"gpt",name:"GPT",efforts:["medium"]}]});
  const catalog=await hostedProviderModels(false);
  expect(catalog.items).toHaveLength(2);
  expect(catalog.items[0].id).not.toBe(catalog.items[1].id);
  const global=selectedModelPayload(catalog.items,catalog.items[0].id,"medium");
  const roles=Object.fromEntries(["engineer","reviewer","validator"].map(role=>[role,selectedModelPayload(catalog.items,catalog.items[1].id,"medium")]));
  expect(global).toEqual({model:"openai/gpt",thinking_effort:"medium",connection:{id:"personal",revision:2}});
  for(const entry of Object.values(roles)) expect(entry).toEqual({model:"openai/gpt",thinking_effort:"medium",connection:{id:"team",revision:7}});
  expect(selectedModelPayload(catalog.items,'["team",8,"openai/gpt"]',"medium")).toBeUndefined();
  expect(selectedModelPayload(catalog.items,"openai/gpt","medium")).toBeUndefined();
 });
 it("rejects expired and foreign revision responses",async()=>{
  hosted();
  centralFetch.mockImplementation(async(path:string)=>path==="/connections"?[{id:"mine",revision:2,definition:{provider:"openai",label:"Mine"}}]:{connection_id:"mine",revision:3,expires_at:new Date(Date.now()+60000).toISOString(),models:[{id:"gpt",name:"GPT",efforts:[]}]});
  expect((await hostedProviderModels(false)).items).toEqual([]);
 });
 it("keeps local model IDs unchanged",()=>{
  expect(selectedModelPayload([],"openai/gpt","medium")).toEqual({model:"openai/gpt",thinking_effort:"medium"});
 });
});
