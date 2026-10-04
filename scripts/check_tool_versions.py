#!/usr/bin/env python3
"""Check active tools and required package metadata against .tool-versions."""
from __future__ import annotations

import argparse
import errno
import json
from pathlib import Path
import re
import shlex
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


def check_versions(go: str, node: str, pnpm: str) -> None:
    versions = dict(line.split() for line in (ROOT / '.tool-versions').read_text().splitlines() if line.strip() and not line.lstrip().startswith('#'))
    python_version = '.'.join(str(part) for part in sys.version_info[:3])
    if python_version != versions['python']:
        raise ValueError(f"Python version is {python_version}; use {versions['python']} from .tool-versions")
    module_version = re.search(r'^go\s+(\S+)$', (ROOT / 'go.mod').read_text(), re.MULTILINE)
    if module_version is None or module_version.group(1) != versions['go']:
        raise ValueError('go.mod must use the Go version in .tool-versions')
    package = json.loads((ROOT / 'web/package.json').read_text())
    if package.get('packageManager') != 'pnpm@' + versions['pnpm']:
        raise ValueError('web/package.json packageManager must use the pnpm version in .tool-versions')
    checks = (
        ('Go', go, ['version'], versions['go'], r'go version go(\S+)'),
        ('Node', node, ['--version'], versions['nodejs'], r'v(\S+)'),
        ('pnpm', pnpm, ['--version'], versions['pnpm'], r'(\S+)'),
    )
    for name, command, arguments, expected, pattern in checks:
        invocation = [*shlex.split(command), *arguments]
        try:
            result = subprocess.run(invocation, capture_output=True, text=True, check=True)
        except OSError as error:
            # pnpm's native-package bootstrap is intentionally a shell script
            # without a shebang. Match the shell's executable-script fallback.
            if error.errno != errno.ENOEXEC:
                raise
            result = subprocess.run(['/bin/sh', *invocation], capture_output=True, text=True, check=True)
        match = re.match(pattern, result.stdout.strip())
        actual = match.group(1) if match else result.stdout.strip()
        if actual != expected:
            raise ValueError(f'{name} version is {actual or "unknown"}; use {expected} from .tool-versions')
    print('Active tool versions match .tool-versions.')


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--go', default='go')
    parser.add_argument('--node', default='node')
    parser.add_argument('--pnpm', default='pnpm')
    args = parser.parse_args()
    try:
        check_versions(args.go, args.node, args.pnpm)
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        print(f'Tool check failed: {error}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
