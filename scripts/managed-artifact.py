"""Finite Linux runtime package shared by local build and primary assembly."""
import hashlib
import io
import json
from pathlib import Path
import re
import struct
import tarfile

BASE = 'docker.io/maximhq/bifrost@sha256:74826952fd6f1fc32f7bf8c364c41e27d996d0d03e4157ca8b55ddadaacb0668'
MAX_BINARY = 512 * 1024**2
FIELDS = ('User', 'Entrypoint', 'Cmd', 'Env', 'WorkingDir', 'Volumes', 'Healthcheck', 'ExposedPorts')
RECIPE = ('FROM ' + BASE + '\nARG SOURCE_REVISION\n'
          'LABEL org.opencontainers.image.source="https://github.com/mastergyp/bifrost" \\\n      org.opencontainers.image.revision="$SOURCE_REVISION"\n'
          'COPY --chmod=0755 main /app/main\n')


def sha(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024**2), b''):
            value.update(chunk)
    return value.hexdigest()


def save(path, value):
    with path.open('x') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')


def static_amd64(path):
    assert path.is_file() and not path.is_symlink() and 64 <= path.stat().st_size <= MAX_BINARY
    with path.open('rb') as stream:
        header = stream.read(64)
        assert header[:6] == b'\x7fELF\x02\x01' and struct.unpack_from('<H', header, 18)[0] == 62
        offset = struct.unpack_from('<Q', header, 32)[0]
        size, count = struct.unpack_from('<HH', header, 54)
        assert size >= 56 and 0 < count <= 128 and offset + count * size <= path.stat().st_size
        for i in range(count):
            stream.seek(offset + i * size)
            assert struct.unpack('<I', stream.read(4))[0] != 3, 'Dynamic ELF interpreter is not qualified'


def check_manifest(value, revision):
    assert set(value) == {'schema', 'revision', 'source_archive_sha256', 'main_sha256', 'main_bytes', 'local_image', 'base_reference'}
    assert type(value['schema']) is int and value['schema'] == 1 and value['revision'] == revision and re.fullmatch('[0-9a-f]{40}', revision)
    assert all(re.fullmatch('[0-9a-f]{64}', value[k]) for k in ['source_archive_sha256', 'main_sha256'])
    assert re.fullmatch('sha256:[0-9a-f]{64}', value['local_image'])
    assert type(value['main_bytes']) is int and 64 <= value['main_bytes'] <= MAX_BINARY
    assert value['base_reference'] == BASE


def unpack_package(raw, root, revision):
    assert len(raw) <= MAX_BINARY and not root.exists()
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:gz') as archive:
        limits = {'main': MAX_BINARY, 'artifact.json': 16384, 'Dockerfile.managed-runtime': 1024}
        rows = []
        for row in archive:
            assert len(rows) < 3 and row.name in limits and row.isfile() and not row.issym() and not row.islnk()
            assert 0 <= row.size <= limits[row.name]
            rows.append(row)
        assert len(rows) == 3 and {r.name for r in rows} == set(limits)
        value = json.loads(archive.extractfile('artifact.json').read())
        check_manifest(value, revision)
        assert archive.extractfile('Dockerfile.managed-runtime').read().decode() == RECIPE
        root.mkdir(mode=0o700)
        for row in rows:
            target = root / row.name
            with target.open('xb') as out:
                source = archive.extractfile(row)
                for chunk in iter(lambda: source.read(1024**2), b''):
                    out.write(chunk)
            target.chmod(0o755 if row.name == 'main' else 0o600)
    assert (root / 'main').stat().st_size == value['main_bytes'] and sha(root / 'main') == value['main_sha256']
    static_amd64(root / 'main')
    return value


def assembly_budget(stat):
    # Package + extraction + one COPY context/layer are each bounded above.
    total, free = stat.f_blocks * stat.f_frsize, stat.f_bavail * stat.f_frsize
    reserve, headroom = max(total // 10, 24 * 1024**3), 3 * 1024**3
    assert total > 0 and free >= 0
    if free < reserve + headroom:
        raise RuntimeError('Runtime assembly refused: preserve service reserve plus 3GiB bounded-copy headroom')
    return {'available_bytes': free, 'service_reserve_bytes': reserve, 'assembly_headroom_bytes': headroom}
