import copy
import importlib.util
from pathlib import Path
import unittest

spec=importlib.util.spec_from_file_location('prepare',Path(__file__).with_name('diagnostic-prepare.py'));p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)
OP='328958aa-0f19-4d13-8fa8-b78c8956f847'
def fixture():
    config={'Image':p.SOURCE_IMAGE,'User':'1000:0','Entrypoint':['/app/main'],'Cmd':None,'Env':['SYNTHETIC_SECRET=fixture-value'],'WorkingDir':'/app','Volumes':{'/app/data':{}},'Healthcheck':{'Test':['CMD','true']},'ExposedPorts':{'8080/tcp':{}},'Labels':{'original':'retained'}}
    host={'Binds':['/owned/data:/app/data:rw','/owned/profile:/etc/profile:ro'],'RestartPolicy':{'Name':'always','MaximumRetryCount':0},'NetworkMode':'private-net','Memory':123456,'SecurityOpt':['no-new-privileges:true'],'LogConfig':{'Type':'json-file','Config':{'max-size':'10m','max-file':'3'}},'UnknownFutureField':{'keep':True}}
    source={'Id':p.SOURCE,'Image':p.SOURCE_IMAGE,'Config':config,'HostConfig':host,'Mounts':[{'Type':'bind','Source':'/owned/data','Destination':'/app/data','RW':True},{'Type':'bind','Source':'/owned/profile','Destination':'/etc/profile','RW':False}],'State':{'Status':'running','Running':True,'Paused':False},'NetworkSettings':{'Networks':{'private-net':{'Aliases':['bifrost']}}}}
    image={'Id':p.APP_IMAGE,'Config':copy.deepcopy(config)};image['Config']['Labels']['org.opencontainers.image.revision']=p.APP_REVISION
    return source,image
class PreparationTests(unittest.TestCase):
    def test_preserves_every_field_and_never_mutates_original(self):
        source,image=fixture();before=copy.deepcopy(source);req=p.prepare_request(source,image,OP);self.assertEqual(source,before)
        expected=copy.deepcopy(source['HostConfig']);expected['RestartPolicy']={'Name':'no','MaximumRetryCount':0};self.assertEqual(req['HostConfig'],expected)
        self.assertEqual(req['Env'],source['Config']['Env']);self.assertEqual(req['HostConfig']['Binds'],source['HostConfig']['Binds'])
        self.assertEqual(req['NetworkingConfig']['EndpointsConfig'],{'private-net':{'Aliases':['bifrost']}})
    def test_refuses_changed_source_image_user_mount_privilege(self):
        for edit in [lambda s,i:s.update(Id='0'*64),lambda s,i:s.update(Image='sha256:'+'0'*64),lambda s,i:i.update(Id='sha256:'+'0'*64),lambda s,i:s['Config'].update(User='root'),lambda s,i:s['HostConfig'].update(Privileged=True),lambda s,i:s['HostConfig'].update(Mounts=[{}]),lambda s,i:s['State'].update(Running=False),lambda s,i:s['Mounts'][0].update(RW=False)]:
            with self.subTest(edit=edit):
                s,i=fixture();edit(s,i)
                with self.assertRaises(AssertionError):p.prepare_request(s,i,OP)
    def test_verify_refuses_unexpected_create_or_early_start(self):
        s,i=fixture();req=p.prepare_request(s,i,OP);created={'Image':p.APP_IMAGE,'Config':{k:v for k,v in req.items() if k not in ('HostConfig','NetworkingConfig')},'HostConfig':copy.deepcopy(req['HostConfig']),'Mounts':list(reversed(s['Mounts'])),'State':{'Status':'created','Running':False}}
        p.check_created(req,created,s)
        for edit in [lambda r:r['State'].update(Status='running',Running=True),lambda r:r['HostConfig'].update(Memory=1),lambda r:r['Config'].update(Env=['CHANGED=wrong']),lambda r:r['Mounts'][0].update(RW=True)]:
            with self.subTest(edit=edit):
                r=copy.deepcopy(created);edit(r)
                with self.assertRaises(AssertionError):p.check_created(req,r,s)
if __name__=='__main__':unittest.main()
