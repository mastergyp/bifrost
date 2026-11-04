#!/usr/bin/env python3
"""Owned network-none Docker fixture for dependency COPY cache boundaries.

Uses a caller-specified already-present immutable shell image. The synthetic go
command performs no compilation or downloads; this verifies Docker invalidation,
not full application compilation or runtime behavior.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile
import uuid


def run(args):
    r=subprocess.run(args,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=60)
    if r.returncode:
        raise RuntimeError('Owned fixture command failed: '+args[0]+' '+(args[1] if len(args)>1 else '')+'; '+r.stderr.decode(errors='replace')[-2000:])
    return r.stdout


def copies(text):
    stage = text.split(' AS build-dependencies\n', 1)[1].split('\nFROM ', 1)[0]
    return [line for line in stage.splitlines() if line.startswith('COPY ')]


def qualify(base, old_revision):
    assert re.fullmatch('sha256:[0-9a-f]{64}', base)
    assert re.fullmatch('[0-9a-f]{40}', old_revision)
    repo = Path(__file__).resolve().parents[1]
    image = json.loads(run(['docker', 'image', 'inspect', base]))[0]
    assert image['Id'] == base
    # BuildKit parses a bare sha256:ID as repository:tag, not local image ID.
    # Require a pre-existing immutable public Rust digest; never a mutable tag.
    references = [v for v in image.get('RepoDigests', [])
                  if re.fullmatch(r'(docker.io/library/)?rust@sha256:[0-9a-f]{64}', v)]
    assert references, 'The fixture needs an already-present immutable Rust image digest'
    reference = references[0]
    old = run(['git', '-C', str(repo), 'show', old_revision + ':transports/Dockerfile.managed']).decode()
    new = (repo / 'transports/Dockerfile.managed').read_text()
    token = uuid.uuid4().hex
    tags = []
    ids = {}
    with tempfile.TemporaryDirectory(prefix='sw-owned-dependency-cache-') as directory:
        root = Path(directory)
        (root / 'fakego').write_text('#!/bin/sh\nprintf fixed-dependency-marker > /build/dependency.marker\n')
        (root / 'fakego').chmod(0o755)
        modules = ['core', 'framework', 'transports'] + [str(p.parent.relative_to(repo)) for p in sorted((repo / 'plugins').glob('*/go.mod'))]
        for module in modules:
            path = root / module
            path.mkdir(parents=True)
            (path / 'go.mod').write_text('module fixture/' + module + '\ngo 1.27.0\n')
            (path / 'go.sum').write_text('')
        for name, source in [('old', old), ('new', new)]:
            (root / 'Dockerfile').write_text('FROM ' + reference + '\nUSER root\nWORKDIR /build\nCOPY fakego /fixture/go\nENV PATH=/fixture:$PATH\n' + '\n'.join(copies(source)) + '\nRUN go mod download all\n')
            ids[name] = []
            for i in range(2):
                (root / 'core/runtime.go').write_text('// synthetic runtime-only change ' + str(i) + '\n')
                tag = 'sw-owned-dependency-cache:' + token + '-' + name + '-' + str(i)
                tags.append(tag)
                iid = root / (name + str(i) + '.id')
                run(['docker', 'build', '--network=none', '--pull=false', '--label',
                     'sw.owner=dependency-cache-qualification', '--iidfile', str(iid), '-t', tag, str(root)])
                ids[name].append(iid.read_text().strip())
        assert ids['old'][0] != ids['old'][1], 'Historical full-source COPY must invalidate dependencies'
        assert ids['new'][0] == ids['new'][1], 'Runtime-only changes must retain the dependency image'
        # Exact owned tags, no generic image/container/cache prune.
        for tag in tags:
            row = json.loads(run(['docker', 'image', 'inspect', tag]))[0]
            assert row['Config']['Labels']['sw.owner'] == 'dependency-cache-qualification'
        run(['docker', 'image', 'rm', *tags])
    return {'passed': True, 'base_image': base, 'historical_revision': old_revision,
            'dockerfile_sha256': hashlib.sha256(new.encode()).hexdigest(),
            'old_runtime_edit_invalidated_dependency_image': True,
            'new_runtime_edit_retained_dependency_image': True,
            'owned_image_tags_removed': tags, 'production_actions': 0,
            'real_go_download_or_application_build': False}


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--base-image', required=True)
    p.add_argument('--old-revision', required=True)
    args = p.parse_args()
    print(json.dumps(qualify(args.base_image, args.old_revision)))
