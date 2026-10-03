#!/usr/bin/env python3
"""Read the shared test CPU budget used by Make and standalone coverage."""
from __future__ import annotations

import os
import re
import sys


DEFAULT_TEST_CPUS = 2
MAX_SAFE_INTEGER = 2**53 - 1


def parse_test_cpu_budget(value: str | None) -> int:
    if value is None:
        return DEFAULT_TEST_CPUS
    value = value.strip()
    if not re.fullmatch(r'[0-9]+', value):
        raise ValueError('KINGDOM_TEST_CPUS must be a positive decimal integer')
    budget = int(value)
    if not 0 < budget <= MAX_SAFE_INTEGER:
        raise ValueError('KINGDOM_TEST_CPUS must be a positive decimal integer within the safe integer range')
    return budget


def main() -> int:
    try:
        value = sys.argv[1] if len(sys.argv) > 1 else os.environ.get('KINGDOM_TEST_CPUS')
        print(parse_test_cpu_budget(value))
    except ValueError as error:
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
