#!/usr/bin/env python3
"""One Bifrost-only cutover. Unknown effects are retained, never replayed."""
import argparse
import fcntl
import importlib.util
import json
import os
from pathlib import Path
import re
import sqlite3
import uuid
import subprocess
import time
import urllib.request

spec=importlib.util.spec_from_file_location('prepare',Path(__file__).with_name('diagnostic-prepare.py'));p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)
PREPARE=Path('/data/releases/shinewing-bifrost-diagnostics/bbfbe31d8ac1886de50ef0c127233da2be4fa510/prepare-r1')
CANDIDATE='a53cfe51613e265e09ec0a69f6eebc8ed10fecfe270f2aad1f20b8272ad9014c'
ROLLBACK_NAME='bifrost-rollback-streamdiag-68a57c4'
RETAINED_NAME='shinewing-bifrost-streamdiag-retained-68a57c4'
class UnknownEffect(RuntimeError):pass
class NotReady(RuntimeError):pass


def cutover(r):
    r.check()
    r.backup()
    r.check()
    r.save('activation-intent',{'source':p.SOURCE,'candidate':CANDIDATE,'physical_lease_changes':0})
    r.restart(p.SOURCE,'no')
    r.stop(p.SOURCE)
    r.rename(p.SOURCE,ROLLBACK_NAME)
    r.rename(CANDIDATE,'bifrost')
    r.start(CANDIDATE)
    try:r.ready(CANDIDATE)
    except NotReady:
        # Only an acknowledged start + failed readiness enters rollback. A
        # timeout/unknown Docker effect propagates without issuing more effects.
        r.save('rollback-intent',{'candidate':CANDIDATE,'source':p.SOURCE,'restore_old_database':False})
        r.stop(CANDIDATE)
        r.rename(CANDIDATE,RETAINED_NAME)
        r.rename(p.SOURCE,'bifrost')
        r.start(p.SOURCE)
        r.ready(p.SOURCE)
        r.restore_restart(p.SOURCE)
        value={'rolled_back':True,'deployed':False,'current_data_preserved':True};r.save('rolled-back',value);return value
    r.restore_restart(CANDIDATE)
    r.verify_protected()
    value={'deployed':True,'source':p.SOURCE,'candidate':CANDIDATE,'revision':p.APP_REVISION,'image':p.APP_IMAGE,'physical_lease_changes':0,'historical_backfill':False,'current_data_preserved':True}
    r.save('activated',value);return value


class Runtime:
    def __init__(self,root):
        self.root=root;self.source=json.loads((PREPARE/'original.private.json').read_text());self.request=json.loads((PREPARE/'request.private.json').read_text())
        self.old_restart=self.source['HostConfig']['RestartPolicy']['Name'];self.opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
        self.protected={n:p.footprint(self.inspect(n)) for n in ['shinewing-ai-gateway','shinewing-llm-postgres','shinewing-llm-redis','app-store-api-1']}
        self.authority=self.get(self.base(p.SOURCE)+'/sw/gateway/health');assert self.authority['state']=='ready'
    def inspect(self,identity):return json.loads(subprocess.check_output(['docker','inspect',identity],stderr=subprocess.PIPE,timeout=8))[0]
    def base(self,identity):
        row=self.inspect(identity);nets=row['NetworkSettings']['Networks'];assert len(nets)==1
        address=next(iter(nets.values()))['IPAddress'];assert re.fullmatch('[0-9.]+',address)
        return 'http://'+address+':8080'
    def get(self,url):
        with self.opener.open(url,timeout=4) as response:return json.load(response)
    def save(self,name,value):
        with (self.root/(name+'.json')).open('x') as stream:
            json.dump(value,stream,indent=2);stream.flush();os.fsync(stream.fileno())
        directory=os.open(self.root,os.O_RDONLY)
        try:os.fsync(directory)
        finally:os.close(directory)
    def verify_protected(self):assert all(p.footprint(self.inspect(n))==v for n,v in self.protected.items()),'Protected runtime changed'
    def check(self):
        assert p.footprint(self.inspect('bifrost'))==p.footprint(self.source)
        assert self.inspect('bifrost')['Id']==p.SOURCE
        p.check_created(self.request,self.inspect(CANDIDATE),self.source);self.verify_protected()
        assert not Path('/var/lib/shinewing-joint-maintenance/active.json').exists()
        names=subprocess.check_output(['docker','ps','-a','--format','{{.Names}}'],text=True,timeout=5).splitlines()
        assert ROLLBACK_NAME not in names and RETAINED_NAME not in names
        state=json.loads(Path('/var/lib/shinewing-engine-watchdog/state.json').read_text());assert state['active'] is None and state['revision']=='6c3f65b808dab9d81adc2f164ee762e7661ec5e5'
    def backup(self):
        data=Path(next(m['Source'] for m in self.source['Mounts'] if m['Destination']=='/app/data'))
        with sqlite3.connect('file:'+str(data/'config.db')+'?mode=ro',uri=True,timeout=2) as src:
            with sqlite3.connect(str(self.root/'config-online.db')) as out:
                deadline=time.monotonic()+10
                def progress(*_):assert time.monotonic()<deadline,'Backup deadline exceeded before any stop'
                src.backup(out,pages=128,progress=progress,sleep=.01);assert out.execute('PRAGMA quick_check').fetchone()[0]=='ok'
    def effect(self,label,command):
        self.save(label+'-intent',{'command_kind':command[1:3],'at':time.time()})
        try:subprocess.run(command,check=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=15)
        except (subprocess.SubprocessError,OSError):raise UnknownEffect('Observe original '+label+' intent and exact container; do not replay') from None
        self.save(label+'-ack',{'acknowledged':True,'at':time.time()})
    def restart(self,identity,policy):self.effect('restart-'+identity[:12],['docker','update','--restart='+policy,identity])
    def restore_restart(self,identity):
        maximum=self.source['HostConfig']['RestartPolicy']['MaximumRetryCount']
        policy=self.old_restart+(':'+str(maximum) if self.old_restart=='on-failure' and maximum else '')
        assert self.old_restart=='on-failure' or maximum==0
        self.effect('restore-restart-'+identity[:12],['docker','update','--restart='+policy,identity])
    def stop(self,identity):
        assert self.inspect(identity)['State']['Running']
        self.effect('term-'+identity[:12],['docker','kill','--signal=SIGTERM',identity])
        deadline=time.monotonic()+120
        while True:
            row=self.inspect(identity)
            if not row['State']['Running']:
                assert row['State']['ExitCode']==0 and not row['State']['OOMKilled'],'Unclean stop; no second writer authorized'
                self.save('normal-exit-'+identity[:12],{'exit_code':0,'at':time.time()});return
            if time.monotonic()>=deadline:raise UnknownEffect('Original process still draining; never SIGKILL or start another writer')
            time.sleep(.25)
    def rename(self,identity,name):self.effect('rename-'+identity[:12]+'-'+name,['docker','rename',identity,name])
    def start(self,identity):
        other=CANDIDATE if identity==p.SOURCE else p.SOURCE;assert not self.inspect(other)['State']['Running']
        assert self.inspect(identity)['Image']==(p.APP_IMAGE if identity==CANDIDATE else p.SOURCE_IMAGE)
        self.effect('start-'+identity[:12],['docker','start',identity])
    def ready(self,identity):
        deadline=time.monotonic()+45
        while time.monotonic()<deadline:
            row=self.inspect('bifrost');assert row['Id']==identity
            if not row['State']['Running']:raise NotReady('Acknowledged candidate exited')
            try:
                if self.get(self.base(identity)+'/sw/gateway/health')==self.authority:
                    health=self.get('http://10.100.1.9:4842/health')
                    if health['identity_ready'] and health['accounting_ready'] and health['managed_upstream_ready']:return
            except (OSError,ValueError):pass
            time.sleep(.5)
        raise NotReady('Native authority or Gateway readiness did not recover')


def main(revision,operation):
    global PREPARE,CANDIDATE,ROLLBACK_NAME,RETAINED_NAME
    assert os.geteuid()==0 and re.fullmatch('[0-9a-f]{40}',revision)
    assert str(uuid.UUID(operation))==operation
    base=Path('/data/releases/shinewing-bifrost-diagnostics')/revision;assert Path(__file__).resolve()==base/'source/scripts/diagnostic-activate.py'
    manifest=base/'source/scripts/managed-release.json'
    if manifest.exists():
        release=p.configure_release(manifest);PREPARE=base/'prepare-r1'
        prepared=json.loads((PREPARE/'prepared.json').read_text())
        assert prepared['prepared'] and prepared['ops_revision']==revision and prepared['app_revision']==p.APP_REVISION and prepared['image']==p.APP_IMAGE and prepared['source']==p.SOURCE
        CANDIDATE=prepared['candidate'];assert re.fullmatch('[0-9a-f]{64}',CANDIDATE) and CANDIDATE!=p.SOURCE
        ROLLBACK_NAME='bifrost-rollback-'+release['purpose']+'-'+p.APP_REVISION[:7]
        RETAINED_NAME='shinewing-bifrost-'+release['purpose']+'-retained-'+p.APP_REVISION[:7]
    root=base/('activation-'+uuid.UUID(operation).hex);assert not root.exists(),'Observe original activation; do not replay'
    root.mkdir(mode=0o700)
    with Path('/var/lib/shinewing-engine-watchdog/watchdog.lock').open('a') as watch,Path('/run/shinewing-joint-maintenance.lock').open('a') as joint:
        fcntl.flock(watch,fcntl.LOCK_EX|fcntl.LOCK_NB);fcntl.flock(joint,fcntl.LOCK_EX|fcntl.LOCK_NB)
        runtime=Runtime(root);value=cutover(runtime);print(json.dumps(value))
        if value.get('rolled_back'):raise SystemExit(75)
if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--revision',required=True);parser.add_argument('--operation',required=True)
    os.umask(0o077);args=parser.parse_args();main(args.revision,args.operation)
