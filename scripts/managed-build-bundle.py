#!/usr/bin/env python3
"""Export immutable managed Docker inputs, excluding unrelated documentation."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess

BUILD_PATHS = ('core', 'framework', 'plugins', 'transports', 'ui', 'tests/managedadmission',
               'scripts/managed-build.py', '.dockerignore', 'LICENSE', 'THIRD_PARTY_NOTICES.md')
MAX_ARCHIVE_BYTES = 128 * 1024 ** 2


def check_copy_closure(dockerfile):
    inputs = []
    for line in dockerfile.splitlines():
        line = line.strip()
        if line.startswith('ADD '):
            raise ValueError('Audit new ADD inputs before exporting')
        if not line.startswith('COPY '):
            continue
        words = shlex.split(line)
        if words[1].startswith('--from='):
            continue
        if any(word.startswith('--') for word in words[1:]) or line.endswith('\\'):
            raise ValueError('Audit new COPY syntax before exporting')
        for source in words[1:-1]:
            if not any(source == path or source.startswith(path + '/') for path in BUILD_PATHS):
                raise ValueError('Uncovered managed Docker input')
            inputs.append(source)
    if not inputs:
        raise ValueError('No managed Docker inputs found')
    return inputs


def export(repository, revision, destination):
    assert re.fullmatch('[0-9a-f]{40}', revision)
    assert not destination.exists() and not destination.is_symlink()
    run = lambda *args: subprocess.check_output(['git', '-C', str(repository), *args], stderr=subprocess.PIPE)
    dockerfile = run('show', revision + ':transports/Dockerfile.managed').decode()
    inputs = check_copy_closure(dockerfile)
    raw = run('archive', '--format=tar.gz', revision, *BUILD_PATHS)
    if len(raw) > MAX_ARCHIVE_BYTES:
        raise ValueError('Managed build bundle exceeds bounded staging budget')
    with destination.open('xb') as stream:
        os.fchmod(stream.fileno(), 0o600)
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    return {'revision': revision, 'archive_sha256': hashlib.sha256(raw).hexdigest(),
            'archive_bytes': len(raw), 'export_paths': list(BUILD_PATHS),
            'checked_docker_copy_inputs': inputs, 'excluded_docs_not_docker_inputs': True,
            'build_or_deployment_executed': False}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--revision', required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(export(Path(__file__).resolve().parents[1], args.revision, args.out)))
