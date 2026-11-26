#!/usr/bin/env python3
"""Apply the experimental profile only to the exact reviewed source tree."""

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile


def tree_digest(root):
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if relative.parts[0] in {".git", "target"}:
            continue
        if path.is_symlink():
            payload = b"link\0" + str(path.readlink()).encode()
        elif path.is_file():
            payload = b"file\0" + path.read_bytes()
        elif path.is_dir():
            continue
        else:
            raise ValueError(f"unsupported source entry: {relative}")
        digest.update(relative.as_posix().encode() + b"\0")
        digest.update(hashlib.sha256(payload).digest())
    return digest.hexdigest()


def prepare(source, output, package):
    source = source.resolve(strict=True)
    output = output.absolute()
    parent = output.parent.resolve(strict=True)
    output = parent / output.name
    if output.exists() or output.is_symlink() or source == parent or source in parent.parents:
        raise ValueError("output must be a new directory outside the source tree")
    manifest = json.loads((package / "manifest.json").read_text())
    patch = package / "managed-commands.patch"
    if hashlib.sha256(patch.read_bytes()).hexdigest() != manifest["patch_sha256"]:
        raise ValueError("patch digest does not match manifest")
    if tree_digest(source) != manifest["source_tree_sha256"]:
        raise ValueError("source tree does not match the pinned OpenShell release")
    with tempfile.TemporaryDirectory(prefix=".kingdom-openshell-", dir=parent) as temporary:
        staged = Path(temporary) / "source"
        shutil.copytree(source, staged, symlinks=True, ignore=shutil.ignore_patterns(".git", "target"))
        subprocess.run(["git", "apply", "--check", str(patch.resolve())], cwd=staged, check=True)
        subprocess.run(["git", "apply", str(patch.resolve())], cwd=staged, check=True)
        if tree_digest(staged) != manifest["patched_tree_sha256"]:
            raise ValueError("patched source tree does not match reviewed result")
        staged.rename(output)
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    try:
        print(prepare(args.source, args.output, Path(__file__).resolve().parent))
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"Preparation failed: {error}\n")


if __name__ == "__main__":
    main()
