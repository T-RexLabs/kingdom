export function orderMilestonesForDisplay<T>(milestones: readonly T[]): T[] {
	return [...milestones].reverse();
}
