#!/usr/bin/env python3
"""Prepare one stopped Bifrost diagnostics container. Never stop/start a service."""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import sqlite3
import subprocess
import time
import uuid

APP_REVISION='68a57c41cb9380a321252dd9060eef6529c50dc2'
APP_IMAGE='sha256:72d3c7d7260ee46c48f905a652a3ba7bd7555d5bbc67f64aad4acbd35d0d495a'
SOURCE='c36579edc5e75cc51e59fba4ed12f0a11f896d47ace369b452b6bf3d2d43b0e3'
SOURCE_IMAGE='sha256:8206f4a3022b40891acc8bb98c743ae6315a8d27516e1d0106de7814b9f197f0'
RUNTIME_FIELDS=('User','Entrypoint','Cmd','Env','WorkingDir','Volumes','Healthcheck','ExposedPorts')


def read_release(path):
    value=json.loads(path.read_text())
    assert set(value)=={'version','app_revision','app_image','source','source_image','purpose'} and value['version']==1
    assert re.fullmatch('[0-9a-f]{40}',value['app_revision'])
    assert all(re.fullmatch('sha256:[0-9a-f]{64}',value[k]) for k in ['app_image','source_image'])
    assert re.fullmatch('[0-9a-f]{64}',value['source']) and value['app_image']!=value['source_image']
    assert re.fullmatch('[a-z][a-z0-9-]{0,31}',value['purpose'])
    return value


def configure_release(path):
    global APP_REVISION,APP_IMAGE,SOURCE,SOURCE_IMAGE
    value=read_release(path)
    APP_REVISION,APP_IMAGE,SOURCE,SOURCE_IMAGE=(value[k] for k in ['app_revision','app_image','source','source_image'])
    return value


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True).encode()).hexdigest()


def footprint(row):
    return digest({'id':row['Id'],'image':row['Image'],'config':row['Config'],'host':row['HostConfig'],'mounts':sorted(row['Mounts'],key=lambda m:m['Destination']),'pid':row['State'].get('Pid'),'started':row['State'].get('StartedAt'),'restart':row.get('RestartCount'),'running':row['State']['Running']})


def prepare_request(source, image, operation):
    assert str(uuid.UUID(operation))==operation
    assert source['Id']==SOURCE and source['Image']==SOURCE_IMAGE
    assert source['State']['Running'] and not source['State'].get('Paused')
    assert image['Id']==APP_IMAGE and image['Config']['Labels']['org.opencontainers.image.revision']==APP_REVISION
    assert source['Config']['User']==image['Config']['User']=='1000:0'
    # Env is the original live configuration, not the image's bootstrap defaults.
    for field in RUNTIME_FIELDS:
        if field not in ('Env',):
            assert source['Config'].get(field)==image['Config'].get(field),field
    host=source['HostConfig'];assert not host.get('AutoRemove') and not host.get('Privileged')
    assert not host.get('Mounts') and host.get('Binds') and not host.get('VolumesFrom')
    networks=source['NetworkSettings']['Networks'];assert len(networks)==1
    assert len([m for m in source['Mounts'] if m['Destination']=='/app/data' and m['Type']=='bind' and m['RW']])==1
    request=copy.deepcopy(source['Config']);request['Image']=APP_IMAGE
    request['Labels']={**(request.get('Labels') or {}),'org.opencontainers.image.revision':APP_REVISION,
        'sw.streamdiag.operation':operation,'sw.streamdiag.source':SOURCE}
    request['HostConfig']=copy.deepcopy(host);request['HostConfig']['RestartPolicy']={'Name':'no','MaximumRetryCount':0}
    request['NetworkingConfig']={'EndpointsConfig':{name:{'Aliases':list(row.get('Aliases') or [])} for name,row in networks.items()}}
    return request


def check_created(request, created, source):
    assert created['State']['Status']=='created' and not created['State']['Running']
    assert created['Image']==APP_IMAGE
    assert created['Config']=={k:v for k,v in request.items() if k not in ('HostConfig','NetworkingConfig')}
    expected=copy.deepcopy(request['HostConfig']);actual=copy.deepcopy(created['HostConfig'])
    if expected.get('OomKillDisable') is None and actual.get('OomKillDisable') is False:expected['OomKillDisable']=False
    assert actual==expected
    normalize=lambda mounts:sorted(mounts,key=lambda row:row['Destination'])
    assert normalize(created['Mounts'])==normalize(source['Mounts'])


def save(path,value):
    with path.open('x') as stream:json.dump(value,stream,indent=2)
    path.chmod(0o600)


def main(revision):
    assert os.geteuid()==0 and socket.gethostname()=='VM-1-9-ubuntu'
    assert re.fullmatch('[0-9a-f]{40}',revision)
    base=Path('/data/releases/shinewing-bifrost-diagnostics')/revision
    assert Path(__file__).resolve()==base/'source/scripts/diagnostic-prepare.py'
    manifest=base/'source/scripts/managed-release.json'
    release=configure_release(manifest) if manifest.exists() else None
    root=base/'prepare-r1';assert not root.exists(),'Observe original prepare intent; never replay'
    inspect=lambda identity:json.loads(subprocess.check_output(['docker','inspect',identity]))[0]
    source=inspect('bifrost');image=json.loads(subprocess.check_output(['docker','image','inspect',APP_IMAGE]))[0]
    build=Path('/data/releases/shinewing-bifrost-managed')/APP_REVISION/'build-r1/image.json'
    meta=json.loads(build.read_text());assert meta['revision']==APP_REVISION and meta['image']==APP_IMAGE
    operation=str(uuid.uuid4());request=prepare_request(source,image,operation)
    root.mkdir(mode=0o700)
    save(root/'original.private.json',source);save(root/'request.private.json',request)
    data=Path(next(m['Source'] for m in source['Mounts'] if m['Destination']=='/app/data'))
    with sqlite3.connect('file:'+str(data/'config.db')+'?mode=ro',uri=True,timeout=2) as src:
        with sqlite3.connect(str(root/'config-online.db')) as out:
            deadline=time.monotonic()+10
            def progress(*_):assert time.monotonic()<deadline,'Backup exceeded finite budget; no create'
            src.backup(out,pages=128,progress=progress,sleep=.01)
            assert out.execute('PRAGMA quick_check').fetchone()[0]=='ok'
    (root/'config-online.db').chmod(0o600)
    name='shinewing-bifrost-'+(release['purpose'] if release else 'streamdiag')+'-'+APP_REVISION[:7]
    save(root/'create-intent.json',{'operation':operation,'name':name,'request_sha256':digest(request),'source':SOURCE,'image':APP_IMAGE,'start_authorized':False})
    response=subprocess.check_output(['curl','--unix-socket','/var/run/docker.sock','--silent','--show-error','--fail','--max-time','15','-H','Content-Type: application/json','--data-binary','@'+str(root/'request.private.json'),'http://localhost/v1.47/containers/create?name='+name],stderr=subprocess.PIPE)
    identity=json.loads(response)['Id'];created=inspect(identity);check_created(request,created,source)
    current=inspect('bifrost');assert footprint(current)==footprint(source),'Original actor changed; no activation is permitted'
    proof={'prepared':True,'candidate':identity,'operation':operation,'name':name,'source':SOURCE,'app_revision':APP_REVISION,'image':APP_IMAGE,'ops_revision':revision,'candidate_status':'created','candidate_started':False,'configuration_mounts_environment_preserved':True,'original_running_unchanged':True,'config_online_backup_bytes':(root/'config-online.db').stat().st_size,'config_online_backup_sha256':hashlib.sha256((root/'config-online.db').read_bytes()).hexdigest(),'activation_authorized_by_this_operation':False}
    save(root/'prepared.json',proof);print(json.dumps(proof))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--revision',required=True)
    os.umask(0o077);main(parser.parse_args().revision)
