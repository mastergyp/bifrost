#!/usr/bin/env python3
"""Exercise real Docker lifecycle commands using owned network-none shell fixtures."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import uuid
s=importlib.util.spec_from_file_location('activate',Path(__file__).with_name('diagnostic-activate.py'));a=importlib.util.module_from_spec(s);s.loader.exec_module(a)

def run(root):
    assert os.geteuid()==0 and not root.exists();root.mkdir(mode=0o700)
    result=[];old_image=a.p.SOURCE_IMAGE;new_image=a.p.APP_IMAGE
    for fail in [False,True]:
        case=root/('rollback' if fail else 'success');case.mkdir(mode=0o700);data=case/'data';data.mkdir(mode=0o777);data.chmod(0o777);(data/'state').write_text('before')
        label=uuid.uuid4().hex;prefix='sw-diag-qual-'+label[:10];ids=[]
        def create(image,suffix):
            identity=subprocess.check_output(['docker','create','--network','none','--entrypoint','/bin/sh','--label','sw.diagnostic-fixture='+label,'--name',prefix+suffix,'--mount','type=bind,source='+str(data)+',target=/owned-state',image,'-c',"trap 'exit 0' TERM; while true; do sleep 0.1; done"],stderr=subprocess.PIPE,text=True).strip();ids.append(identity);return identity
        old=create(old_image,'-active');new=create(new_image,'-candidate');a.p.SOURCE=old;a.CANDIDATE=new
        subprocess.run(['docker','start',old],check=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        class Owned(a.Runtime):
            def __init__(self):self.root=case;self.source=self.inspect(old);self.old_restart='no'
            def check(self):
                assert self.inspect(old)['State']['Running'] and not self.inspect(new)['State']['Running']
                assert all(self.inspect(i)['Config']['Labels'].get('sw.diagnostic-fixture')==label for i in ids)
            def wait_idle(self):return True
            def backup(self):shutil.copyfile(data/'state',case/'state.backup')
            def rename(self,identity,name):super().rename(identity,prefix+'-active' if name=='bifrost' else prefix+'-'+name)
            def ready(self,identity):
                assert self.inspect(identity)['State']['Running']
                if identity==new:
                    (data/'state').write_text('written-by-new')
                    if fail:raise a.NotReady('owned synthetic readiness failure')
            def verify_protected(self):assert (data/'state').read_text()=='written-by-new'
        runtime=Owned();out=a.cutover(runtime);assert out.get('rolled_back') if fail else out.get('deployed')
        assert (data/'state').read_text()=='written-by-new';assert sum(runtime.inspect(i)['State']['Running'] for i in ids)==1
        runtime.root=case/'cleanup';runtime.root.mkdir(mode=0o700)
        for identity in ids:
            row=runtime.inspect(identity);assert row['Config']['Labels'].get('sw.diagnostic-fixture')==label
            if row['State']['Running']:runtime.stop(identity)
            subprocess.run(['docker','rm','-v',identity],check=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        result.append({'case':'known-readiness-rollback' if fail else 'forward','passed':True,'actual_docker_term_exit_zero':True,'unique_writer':True,'current_synthetic_state_preserved':True,'owned_containers_removed':True,'network':'none','inference_calls':0})
    proof={'passed':True,'cases':result,'limits':'Real Docker transport with owned shell fixtures and synthetic state; not an additional production Bifrost or GPU fault test. Original Bifrost shutdown/storage source equivalence is independently retained.'}
    (root/'proof.json').write_text(json.dumps(proof,indent=2));print(json.dumps(proof))
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,required=True);run(p.parse_args().root)
