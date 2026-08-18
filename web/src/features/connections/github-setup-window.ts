/**
 * The single browser tab that carries every GitHub hop of a setup flow.
 *
 * Kingdom's own tab drives the flow and must never navigate away: it holds the
 * flow's stage, its idempotency keys, and the operator's page state. Each hop
 * therefore targets one named tab, and the manifest callbacks return that tab
 * to a chromeless handoff page so it can close itself.
 *
 * Opening must happen synchronously inside the click that starts a hop —
 * browsers block `window.open` once an await has intervened. No window features
 * are passed, so the browser opens an ordinary tab rather than a popup window.
 */
const SETUP_WINDOW_NAME = "kingdom-github-setup";

export const setupWindowBlockedMessage =
  "Kingdom could not open a tab for GitHub. Allow pop-ups for this site, then try again.";

/** Opens or focuses the setup tab. Returns false when the browser blocked it. */
export function openSetupWindow(url = ""): boolean {
  const target = window.open(url, SETUP_WINDOW_NAME);
  target?.focus();
  return !!target;
}

/**
 * GitHub's App manifest flow only accepts a form POST, so the manifest is
 * submitted into the already-open setup tab. The form is removed immediately:
 * it is a transport detail, not page content.
 */
export function postManifestToSetupWindow(action: string, manifest: string) {
  const form = document.createElement("form");
  form.method = "post";
  form.action = action;
  form.target = SETUP_WINDOW_NAME;
  form.hidden = true;
  const input = document.createElement("input");
  input.type = "hidden";
  input.name = "manifest";
  input.value = manifest;
  form.append(input);
  document.body.append(form);
  form.submit();
  form.remove();
}
