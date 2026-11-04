import importlib.util
from pathlib import Path
import sys
import unittest
sys.path.insert(0,str(Path(__file__).parent))
s=importlib.util.spec_from_file_location('activate',Path(__file__).with_name('diagnostic-activate.py'));a=importlib.util.module_from_spec(s);s.loader.exec_module(a)
class Fake:
    def __init__(self,busy=False,fail=None):self.calls=[];self.busy=busy;self.fail=fail;self.running={a.p.SOURCE:True,a.CANDIDATE:False}
    def check(self):self.calls.append('check')
    def wait_idle(self):return not self.busy
    def backup(self):self.calls.append('backup')
    def save(self,n,v):self.calls.append(n)
    def restart(self,i,p):self.calls.append('restart')
    def stop(self,i):
        self.calls.append('stop:'+i)
        if self.fail=='unknown_stop':raise a.UnknownEffect('unknown')
        self.running[i]=False
    def rename(self,i,n):self.calls.append('rename:'+i)
    def start(self,i):
        assert not any(v for k,v in self.running.items() if k!=i),'two writers'
        self.calls.append('start:'+i)
        if self.fail=='unknown_start':raise a.UnknownEffect('unknown')
        self.running[i]=True
    def ready(self,i):
        if i==a.CANDIDATE and self.fail=='not_ready':raise a.NotReady('synthetic')
    def restore_restart(self,i):self.calls.append('restore:'+i)
    def verify_protected(self):self.calls.append('protected')
class Tests(unittest.TestCase):
    def test_success_one_writer_and_backup_before_stop(self):
        r=Fake();self.assertTrue(a.cutover(r)['deployed']);self.assertLess(r.calls.index('backup'),r.calls.index('stop:'+a.p.SOURCE));self.assertFalse(r.running[a.p.SOURCE]);self.assertTrue(r.running[a.CANDIDATE])
    def test_busy_is_deferred_without_effects(self):
        r=Fake(busy=True);self.assertTrue(a.cutover(r)['deferred']);self.assertEqual(r.calls,['check','deferred'])
    def test_unknown_stop_never_starts_candidate(self):
        r=Fake(fail='unknown_stop')
        with self.assertRaises(a.UnknownEffect):a.cutover(r)
        self.assertFalse(any(x.startswith('start:') for x in r.calls))
    def test_unknown_start_never_replays_or_rolls_back(self):
        r=Fake(fail='unknown_start')
        with self.assertRaises(a.UnknownEffect):a.cutover(r)
        self.assertEqual([x for x in r.calls if x.startswith('start:')],['start:'+a.CANDIDATE]);self.assertNotIn('rollback-intent',r.calls)
    def test_known_readiness_failure_rolls_back_current_data_one_writer(self):
        r=Fake(fail='not_ready');v=a.cutover(r);self.assertTrue(v['rolled_back']);self.assertTrue(v['current_data_preserved']);self.assertTrue(r.running[a.p.SOURCE]);self.assertFalse(r.running[a.CANDIDATE])
if __name__=='__main__':unittest.main()
