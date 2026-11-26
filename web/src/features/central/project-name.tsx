import { useQuery } from "@tanstack/react-query";
import { centralFetch } from "@/api/client";
import { appBase } from "@/lib/runtime-mode";
import { useCentralSession } from "./session-context";

export function SharedProjectName() {
  const session = useCentralSession();
  const projectID = appBase().split("/").at(-1);
  const projects = useQuery({ queryKey: ["central-projects", session.user.id], queryFn: () => centralFetch<{ id: string; display_name: string }[]>("/projects") });
  return <>{projects.data?.find(project => project.id === projectID)?.display_name ?? "Shared Project"}</>;
}
