from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest


ROOT = Path(__file__).resolve().parents[2]


class MakeWorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        shutil.copy(ROOT / 'Makefile', self.root / 'Makefile')
        shutil.copy(ROOT / '.tool-versions', self.root / '.tool-versions')
        shutil.copy(ROOT / 'go.mod', self.root / 'go.mod')
        for name in ('web/src', 'web/public', 'contracts/theme', 'internal/bootstrapassets/files', 'internal/dbmigrations', 'cmd', 'scripts/tests', 'internal/apigen'):
            (self.root / name).mkdir(parents=True, exist_ok=True)
        shutil.copy(ROOT / 'web/package.json', self.root / 'web/package.json')
        for name in ('web/pnpm-lock.yaml', 'web/pnpm-workspace.yaml', 'go.sum'):
            (self.root / name).touch()
        for name in ('scripts/check_tool_versions.py',):
            if (ROOT / name).exists():
                shutil.copy(ROOT / name, self.root / name)
        (self.root / 'scripts/tests/test_fixture.py').write_text('import unittest\nclass FixtureTest(unittest.TestCase):\n    def test_fixture(self):\n        self.assertTrue(True)\n')
        (self.root / 'contracts/theme/theme.schema.json').touch()
        (self.root / 'scripts/generate_theme_contract.mjs').touch()
        (self.root / 'scripts/a_valid.sh').write_text('#!/bin/bash\ntrue\n')
        (self.root / 'contracts/openapi').mkdir()
        (self.root / 'contracts/openapi/openapi.yaml').touch()
        (self.root / 'internal/apigen/types.gen.go').write_text('types\n')
        (self.root / 'web/src/api').mkdir()
        (self.root / 'web/src/api/schema.ts').write_text('schema\n')
        (self.root / 'web/public/deleted.txt').write_text('asset\n')
        (self.root / 'cmd/deleted.go').write_text('source\n')
        versions = dict(line.split() for line in (ROOT / '.tool-versions').read_text().splitlines())
        versions['python'] = '.'.join(str(part) for part in sys.version_info[:3])
        # Fake tool fixtures use the interpreter that runs this unit suite.
        (self.root / '.tool-versions').write_text(''.join(f'{name} {version}\n' for name, version in versions.items()))
        tool_script = self.root / 'fake_tool.py'
        tool_script.write_text('#!' + sys.executable + '\n' + '''import json, os, pathlib, sys
name = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]
root = pathlib.Path(os.environ['FIXTURE_ROOT'])
versions = json.loads(os.environ['FIXTURE_VERSIONS'])
with (root / 'calls.jsonl').open('a') as handle:
    handle.write(json.dumps([name, *args]) + '\\n')
if name == 'go' and args[:1] == ['env']:
    print(root / 'bin')
elif name == 'go' and args == ['version']:
    print('go version go' + versions['go'] + ' linux/amd64')
elif name == 'node' and args == ['--version']:
    print('v' + versions['nodejs'])
elif name == 'node' and args[:1] == ['scripts/generate_bootstrap_assets.mjs']:
    sys.exit(int(os.environ.get('FAIL_ASSETS_CHECK', '0')))
elif name == 'pnpm' and args == ['--version']:
    print(versions['pnpm'])
elif name == 'pnpm' and args == ['install', '--frozen-lockfile']:
    (root / 'web/node_modules').mkdir(exist_ok=True)
elif name == 'pnpm' and args == ['build']:
    dist = root / 'web/dist'
    if dist.exists():
        import shutil
        shutil.rmtree(dist)
    dist.mkdir()
    (dist / 'index.html').write_text('built')
    for asset in (root / 'web/public').iterdir():
        (dist / asset.name).write_text(asset.read_text())
elif name == 'go' and args[:1] == ['build']:
    destination = pathlib.Path(args[args.index('-o') + 1])
    destination.parent.mkdir(exist_ok=True)
    destination.write_text('binary')
elif name == 'go' and args[:1] == ['run']:
    pathlib.Path(args[args.index('-o') + 1]).write_text('types\\n')
    sys.exit(int(os.environ.get('FAIL_GO_GENERATOR', '0')))
elif name == 'pnpm' and args[:2] == ['exec', 'openapi-typescript']:
    pathlib.Path(args[args.index('-o') + 1]).write_text('schema\\n')
    sys.exit(int(os.environ.get('FAIL_WEB_GENERATOR', '0')))
''')
        tool_script.chmod(0o755)
        self.tools = self.root / 'tools'
        self.tools.mkdir()
        for name in ('go', 'node', 'pnpm'):
            (self.tools / name).symlink_to(tool_script)
        (self.tools / 'python3').symlink_to(sys.executable)
        self.env = {**os.environ, 'PATH': str(self.tools) + os.pathsep + os.environ['PATH'], 'FIXTURE_ROOT': str(self.root), 'FIXTURE_VERSIONS': json.dumps(versions)}

    def make(self, *targets: str, **env: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(['make', f'GO={self.tools / "go"}', f'NODE={self.tools / "node"}', f'PNPM={self.tools / "pnpm"}', *targets], cwd=self.root, env={**self.env, **env}, capture_output=True, text=True)

    def calls(self) -> list[list[str]]:
        return [json.loads(line) for line in (self.root / 'calls.jsonl').read_text().splitlines()]

    def test_deleted_web_asset_and_go_source_rebuild_existing_outputs(self) -> None:
        first = self.make('build')
        self.assertEqual(first.returncode, 0, first.stdout + first.stderr)
        (self.root / 'web/public/deleted.txt').unlink()
        (self.root / 'cmd/deleted.go').unlink()
        second = self.make('build')
        self.assertEqual(second.returncode, 0, second.stdout + second.stderr)
        self.assertFalse((self.root / 'web/dist/deleted.txt').exists())
        self.assertEqual(sum(call[:2] == ['go', 'build'] for call in self.calls()), 2)

    def test_build_refuses_stale_bundled_assets(self) -> None:
        result = self.make('build', FAIL_ASSETS_CHECK='1')
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.root / 'bin/kd').exists())
        self.assertFalse(any(call == ['pnpm', 'build'] for call in self.calls()))

    def test_shared_web_build_runs_once_per_verification_invocation(self) -> None:
        result = self.make('test', 'vet', 'test-race')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(sum(call == ['pnpm', 'build'] for call in self.calls()), 1)

    def test_syntax_check_rejects_errors_in_later_scripts(self) -> None:
        (self.root / 'scripts/z_invalid.sh').write_text('if then\n')
        result = self.make('scripts-check')
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_contract_check_stops_when_a_generator_writes_output_then_fails(self) -> None:
        for variable in ('FAIL_GO_GENERATOR', 'FAIL_WEB_GENERATOR'):
            with self.subTest(generator=variable):
                result = self.make('contracts-check', **{variable: '7'})
                self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_tool_check_accepts_shell_bootstrap_without_a_shebang(self) -> None:
        pnpm = self.tools / 'pnpm'
        pnpm.unlink()
        version = json.loads(self.env['FIXTURE_VERSIONS'])['pnpm']
        pnpm.write_text("printf '%s\\n' '" + version + "'\n")
        pnpm.chmod(0o755)
        result = self.make('tools-check')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_tool_check_rejects_python_runtime_mismatch(self) -> None:
        versions = json.loads(self.env['FIXTURE_VERSIONS'])
        versions['python'] = '0.0.0'
        (self.root / '.tool-versions').write_text(''.join(f'{name} {version}\n' for name, version in versions.items()))
        result = self.make('tools-check')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Python', result.stdout + result.stderr)

    def test_tool_check_rejects_active_version_mismatch(self) -> None:
        versions = json.loads(self.env['FIXTURE_VERSIONS'])
        versions['nodejs'] = '0.0.0'
        result = self.make('tools-check', FIXTURE_VERSIONS=json.dumps(versions))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Node', result.stdout + result.stderr)


if __name__ == '__main__':
    unittest.main()
