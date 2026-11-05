#!/usr/bin/env python3
"""Assemble a verified static binary on the primary: no compiler or downloads."""
import argparse
import fcntl
import importlib.util
import json
import os
from pathlib import Path
import re
import socket
import subprocess

spec=importlib.util.spec_from_file_location('artifact',Path(__file__).with_name('managed-artifact.py'))
a=importlib.util.module_from_spec(spec);spec.loader.exec_module(a)


def run(args):
    return subprocess.check_output(args,stderr=subprocess.PIPE,timeout=90)


def assemble(revision):
    assert os.geteuid()==0 and socket.gethostname()=='VM-1-9-ubuntu' and re.fullmatch('[0-9a-f]{40}',revision)
    root=Path('/data/releases/shinewing-bifrost-managed')/revision
    assert Path(__file__).resolve()==root/'source/scripts/managed-runtime-assemble.py'
    archive=json.loads((root/'source-archive.json').read_text())
    assert archive['revision']==revision and a.sha(root/'source.tar.gz')==archive['archive_sha256']
    package=(root/'runtime.tar.gz').read_bytes()
    manifest=json.loads((root/'delivery.json').read_text())
    assert manifest['revision']==revision and a.sha(root/'runtime.tar.gz')==manifest['runtime_package_sha256']
    budget=a.assembly_budget(os.statvfs('/data'))
    base=json.loads(run(['docker','image','inspect',a.BASE]))[0] # must already be local
    operation=root/'build-r1';assert not operation.exists(),'Observe original assembly intent, never replay'
    operation.mkdir(mode=0o700);a.save(operation/'intent.json',{'revision':revision,'package_sha256':manifest['runtime_package_sha256'],'assembly_capacity':budget,'compile_or_download':False})
    value=a.unpack_package(package,root/'runtime-input',revision)
    assert value['source_archive_sha256']==archive['archive_sha256']
    command=['docker','build','--pull=false','--network=none','--build-arg','SOURCE_REVISION='+revision,
             '--iidfile',str(operation/'image.id'),'-t','shinewing-bifrost-managed:'+revision,
             '-f','Dockerfile.managed-runtime','.']
    with (operation/'assembly.private.log').open('x') as log:
        subprocess.run(command,cwd=root/'runtime-input',stdout=log,stderr=subprocess.STDOUT,check=True,timeout=120)
    identity=(operation/'image.id').read_text().strip();image=json.loads(run(['docker','image','inspect',identity]))[0]
    assert image['Config']['Labels']['org.opencontainers.image.revision']==revision
    assert all(image['Config'].get(k)==base['Config'].get(k) for k in a.FIELDS)
    assert image['RootFS']['Layers'][:-1]==base['RootFS']['Layers']
    proof={'revision':revision,'image':identity,'target':'main','archive_sha256':archive['archive_sha256'],
           'runtime_base_image':base['Id'],'main_binary_sha256':value['main_sha256'],'local_build_image':value['local_image'],
           'runtime_package_sha256':manifest['runtime_package_sha256'],'compile_or_download':False,
           'capacity_preflight':budget,'production_activated':False}
    a.save(operation/'image.json',proof);return proof


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--revision',required=True);args=p.parse_args();os.umask(0o077)
    with Path('/run/shinewing-managed-build.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);print(json.dumps(assemble(args.revision)))
