/** Bound test workers without changing file isolation or test selection. */
export function testWorkerOptions(cpuBudget: string | undefined) {
  if (cpuBudget === undefined) {
    return { maxWorkers: 2, minWorkers: 1 };
  }

  const input = cpuBudget.trim();
  const workers = Number(input);
  if (!/^\d+$/.test(input) || !Number.isSafeInteger(workers) || workers < 1) {
    throw new Error("KINGDOM_TEST_CPUS must be a positive integer.");
  }

  return { maxWorkers: workers, minWorkers: 1 };
}
