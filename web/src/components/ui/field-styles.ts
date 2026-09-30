/**
 * The one text-field surface in Kingdom.
 *
 * Trigger surfaces (SelectField, comboboxes) share it so a dropdown sits at the
 * same visual weight as the input beside it, and so a new field cannot quietly
 * drift onto a different background or lose its focus ring.
 */
export const fieldClass =
  "ui-field w-full min-w-0 rounded-md border border-border bg-background px-3 py-2 text-sm text-foreground outline-none transition-colors placeholder:text-foreground-subtle focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent";

/** Field label styling, so labels stay consistent across forms. */
export const fieldLabelClass = "mb-1.5 block text-sm font-medium";

/** A choice has the same field rhythm, with room for a visible menu affordance. */
export const selectTriggerClass = `${fieldClass} ui-select-trigger group flex min-h-10 items-center text-left disabled:cursor-not-allowed disabled:opacity-50 enabled:hover:border-border-strong`;
