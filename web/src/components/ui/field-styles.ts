/**
 * The one text-field surface in Kingdom.
 *
 * Trigger surfaces (SelectField, comboboxes) share it so a dropdown sits at the
 * same visual weight as the input beside it, and so a new field cannot quietly
 * drift onto a different background or lose its focus ring.
 */
export const fieldClass =
  "w-full min-w-0 rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground outline-none transition-colors placeholder:text-foreground-subtle focus:border-gold focus:outline-none focus:ring-1 focus:ring-gold";

/** Field label styling, so labels stay consistent across forms. */
export const fieldLabelClass = "mb-1.5 block text-sm font-medium";
