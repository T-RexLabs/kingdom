/** Parse a relative time expression like "now-4h" into a Date. */
export function parseRelativeTime(expr: string): Date | null {
  const trimmed = expr.trim().toLowerCase();
  if (trimmed === "now") return new Date();

  const match = trimmed.match(/^now\s*-\s*(\d+)\s*(m|h|d)$/);
  if (!match) return null;

  const amount = parseInt(match[1], 10);
  const unit = match[2];
  const now = new Date();

  switch (unit) {
    case "m":
      now.setMinutes(now.getMinutes() - amount);
      break;
    case "h":
      now.setHours(now.getHours() - amount);
      break;
    case "d":
      now.setDate(now.getDate() - amount);
      break;
  }
  return now;
}

/** Convert a total-minutes value into a relative "from" expression. */
export function minutesToRelativeExpr(totalMinutes: number): string {
  if (totalMinutes < 60) return `now-${totalMinutes}m`;
  if (totalMinutes % 1440 === 0) {
    return `now-${totalMinutes / 1440}d`;
  }
  if (totalMinutes % 60 === 0 && totalMinutes < 1440) {
    return `now-${totalMinutes / 60}h`;
  }
  if (totalMinutes < 1440) {
    return `now-${totalMinutes}m`;
  }
  const days = totalMinutes / 1440;
  if (Number.isInteger(days)) return `now-${days}d`;
  return `now-${totalMinutes}m`;
}
