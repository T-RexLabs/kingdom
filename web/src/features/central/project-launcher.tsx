import { useRef, useState } from "react";
import { Button } from "@/components/ui/button";
import { Modal, ModalFooter } from "@/components/ui/modal";
import type { CentralProject } from "./access-client";
import { ProjectInput } from "./project-input";
import { useCentralSession } from "./session-context";
import { useProjectInputController } from "./use-project-input-controller";

type Props = { project: CentralProject; onNavigate?: (url: string) => void };
export function ProjectLauncher(props: Props) {
  const session = useCentralSession();
  return <BoundProjectLauncher key={JSON.stringify([session.user.id, session.session_id, props.project.id])} {...props} />;
}
function BoundProjectLauncher({ project, onNavigate = url => window.location.assign(url) }: Props) {
  const [open, setOpen] = useState(false); const [confirmEmpty, setConfirmEmpty] = useState(false);
  const attempt = useRef(0); const opener = useRef<HTMLButtonElement>(null);
  const input = useProjectInputController(project, open);
  const url = `/projects/${encodeURIComponent(project.id)}/`;
  function close() { attempt.current++; setOpen(false); setConfirmEmpty(false); }
  function navigate() { close(); onNavigate(url); }
  async function begin() {
    const request = ++attempt.current; setConfirmEmpty(false); setOpen(true);
    const source = await input.checkStatus();
    if (request !== attempt.current || !input.isCurrent() || !source || input.hasPendingRequest()) return;
    if (source.state === "imported" || source.state === "started") navigate();
  }
  async function startEmpty() {
    if (input.hasPendingRequest()) return;
    const request = attempt.current; const source = await input.checkStatus();
    if (request !== attempt.current || !input.isCurrent() || !source || input.hasPendingRequest()) return;
    if (source.state === "empty" && (!source.can_upload || project.role !== "owner")) { setConfirmEmpty(false); return; }
    navigate();
  }
  const ready = !input.status.isFetching && !input.status.isError && !input.locked;
  const imported = input.status.data?.state === "imported" || input.status.data?.state === "started";
  const confirming = confirmEmpty && input.canConfigure && !input.status.isError;
  return <>
    <Button ref={opener} variant="outline" onClick={() => { void begin(); }}>Open Project</Button>
    <Modal open={open} onClose={close} title={confirming ? "Start Empty Project" : "Set Up Project"} caption={project.display_name} description={confirming ? undefined : "Choose the initial files for this project before starting work."} size="sm" getFinalFocus={() => opener.current} footer={<ModalFooter>
      <Button variant="ghost" onClick={close}>Close</Button>
      {confirming ? <><Button variant="secondary" disabled={input.busy} onClick={() => setConfirmEmpty(false)}>Back</Button><Button disabled={!ready} onClick={() => { void startEmpty(); }}>Start Empty Project</Button></> : imported ? <Button disabled={!ready} onClick={() => { void begin(); }}>Open Project</Button> : input.canConfigure ? <Button variant="secondary" disabled={!ready} onClick={() => setConfirmEmpty(true)}>Start Empty Project</Button> : null}
    </ModalFooter>}>
      {confirming ? <p>Starting this project closes initial uploads. You cannot add a ZIP archive, file, or repository as its initial source later. Start with an empty project only if this is intended.</p> : <ProjectInput controller={input} />}
    </Modal>
  </>;
}
