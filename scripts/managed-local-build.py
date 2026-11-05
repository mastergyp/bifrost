#!/usr/bin/env python3
"""Compile fixed Git inputs in local Linux Docker, never on the production host."""
import argparse
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


a = module('artifact', Path(__file__).with_name('managed-artifact.py'))
b = module('bundle', Path(__file__).with_name('managed-build-bundle.py'))


def run(args):
    return subprocess.check_output(args, stderr=subprocess.PIPE, timeout=30)


def build(revision, root, probe_image):
    repo = Path(__file__).resolve().parents[1]
    assert re.fullmatch('[0-9a-f]{40}', revision) and re.fullmatch('sha256:[0-9a-f]{64}', probe_image)
    assert not run(['git', '-C', str(repo), 'status', '--porcelain']).strip()
    assert not root.exists() and root.is_absolute() and not root.is_symlink()
    info = json.loads(run(['docker', 'info', '--format', '{"OperatingSystem":{{json .OperatingSystem}},"OSType":{{json .OSType}},"Architecture":{{json .Architecture}}}']))
    assert info['OperatingSystem'] == 'Docker Desktop' and info['OSType'] == 'linux'
    assert info['Architecture'] in ('x86_64', 'amd64'), 'Use the qualified native Linux/amd64 desktop'
    assert shutil.disk_usage(root.parent).free >= 32 * 1024**3
    probe = run(['docker', 'run', '--rm', '--network=none', '--read-only', '--label',
                 'sw.owner=local-build-capacity-probe', '--entrypoint', 'df', probe_image, '-Pk', '/']).decode().splitlines()[-1].split()
    assert int(probe[3]) * 1024 >= 24 * 1024**3, 'Local Docker filesystem lacks build headroom'
    root.mkdir(mode=0o700)
    archive = b.export(repo, revision, root / 'source.tar.gz')
    a.save(root / 'source-archive.json', archive)
    source = root / 'source'
    source.mkdir(mode=0o700)
    with tarfile.open(root / 'source.tar.gz', 'r:gz') as tar:
        members = tar.getmembers()
        assert sum(m.size for m in members) <= 512 * 1024**2
        assert all((m.isfile() or m.isdir()) and not Path(m.name).is_absolute() and '..' not in Path(m.name).parts for m in members)
        tar.extractall(source)
    command = ['docker', 'buildx', 'build', '--builder', 'desktop-linux', '--platform', 'linux/amd64', '--load',
               '--resource', 'memory=4g', '--resource', 'cpu-quota=200000', '--resource', 'cpu-period=100000',
               '--build-arg', 'APK_MIRROR=https://dl-cdn.alpinelinux.org/alpine',
               '--build-arg', 'GOPROXY=https://proxy.golang.org,direct', '--build-arg', 'SOURCE_REVISION=' + revision,
               '--iidfile', str(root / 'image.id'), '-f', 'transports/Dockerfile.managed',
               '-t', 'shinewing-bifrost-local:' + revision, '.']
    a.save(root / 'build-intent.json', {'revision': revision, 'source_archive': archive, 'builder': 'desktop-linux',
                                      'operating_system': info['OperatingSystem'], 'per_step_memory': 4 * 1024**3,
                                      'per_step_cpu_quota': 200000, 'production_actions': 0})
    with (root / 'build.private.log').open('x') as log:
        subprocess.run(command, cwd=source, stdout=log, stderr=subprocess.STDOUT, check=True)
    identity = (root / 'image.id').read_text().strip()
    image = json.loads(run(['docker', 'image', 'inspect', identity]))[0]
    base = json.loads(run(['docker', 'image', 'inspect', a.BASE]))[0]
    assert image['Config']['Labels']['org.opencontainers.image.revision'] == revision
    assert all(image['Config'].get(k) == base['Config'].get(k) for k in a.FIELDS)
    name = 'sw-bifrost-local-export-' + revision[:12]
    a.save(root / 'export-intent.json', {'name': name, 'image': identity})
    cid = run(['docker', 'create', '--network=none', '--label', 'sw.owner=local-binary-export', '--name', name, identity]).decode().strip()
    run(['docker', 'cp', cid + ':/app/main', str(root / 'main')])
    a.static_amd64(root / 'main')
    row = json.loads(run(['docker', 'inspect', cid]))[0]
    assert row['State']['Status'] == 'created' and row['Config']['Labels']['sw.owner'] == 'local-binary-export'
    run(['docker', 'rm', '-v', cid])
    artifact = {'schema': 1, 'revision': revision, 'source_archive_sha256': archive['archive_sha256'],
                'main_sha256': a.sha(root / 'main'), 'main_bytes': (root / 'main').stat().st_size,
                'local_image': identity, 'base_reference': a.BASE}
    a.save(root / 'artifact.json', artifact)
    recipe = (source / 'transports/Dockerfile.managed-runtime').read_text()
    assert recipe == a.RECIPE
    with tarfile.open(root / 'runtime.tar.gz', 'w:gz') as tar:
        for name, path in [('main', root / 'main'), ('artifact.json', root / 'artifact.json')]:
            tar.add(path, arcname=name)
        raw = recipe.encode(); entry = tarfile.TarInfo('Dockerfile.managed-runtime');entry.size=len(raw);entry.mode=0o600
        tar.addfile(entry, io.BytesIO(raw))
    assert (root / 'runtime.tar.gz').stat().st_size <= a.MAX_BINARY
    result = {'artifact': artifact, 'runtime_package_sha256': a.sha(root / 'runtime.tar.gz'),
              'runtime_package_bytes': (root / 'runtime.tar.gz').stat().st_size,
              'eight_official_runtime_fields_preserved': True, 'static_amd64': True, 'production_actions': 0}
    a.save(root / 'built.json', result)
    return result


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__);p.add_argument('--revision', required=True)
    p.add_argument('--root', type=Path, required=True);p.add_argument('--probe-image', required=True)
    args=p.parse_args();os.umask(0o077);print(json.dumps(build(args.revision,args.root,args.probe_image)))
