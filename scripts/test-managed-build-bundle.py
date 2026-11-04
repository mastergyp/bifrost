import importlib.util
from pathlib import Path
import unittest
import shlex
from types import SimpleNamespace
s=importlib.util.spec_from_file_location('bundle',Path(__file__).with_name('managed-build-bundle.py'));m=importlib.util.module_from_spec(s);s.loader.exec_module(m)

class ClosureTests(unittest.TestCase):
    def test_dependency_download_does_not_depend_on_runtime_source(self):
        root=Path(__file__).resolve().parents[1]
        dockerfile=(root/'transports/Dockerfile.managed').read_text()
        dependency=dockerfile.split(' AS build-dependencies\n',1)[1].split('\nFROM ',1)[0]
        copies=[shlex.split(l)[1:-1] for l in dependency.splitlines() if l.startswith('COPY ')]
        sources=[s for row in copies for s in row]
        self.assertTrue(sources)
        self.assertTrue(all(s.endswith(('/go.mod','/go.sum')) for s in sources), sources)
        modules=[root/'core',root/'framework',root/'transports',*sorted((root/'plugins').iterdir())]
        expected={str(p.relative_to(root)) for d in modules if (d/'go.mod').is_file() for p in (d/'go.mod',d/'go.sum') if p.exists()}
        self.assertEqual(set(sources), expected)
    def test_current_docker_inputs_are_all_retained(self):
        dockerfile=(Path(__file__).resolve().parents[1]/'transports/Dockerfile.managed').read_text()
        inputs=m.check_copy_closure(dockerfile)
        self.assertIn('tests/managedadmission/',inputs)
        self.assertTrue(all(root in m.BUILD_PATHS for root in ['core','framework','plugins','transports','ui']))
        self.assertNotIn('docs',m.BUILD_PATHS)
    def test_unknown_root_or_add_never_silently_omits_build_input(self):
        for text in ['COPY other-runtime/ /build/','COPY . /build/','ADD runtime.tar /build/','COPY ${SOURCE}/ /build/']:
            with self.subTest(text=text),self.assertRaises(ValueError):m.check_copy_closure(text)
    def test_from_stage_is_not_a_repository_input_but_unknown_flags_fail(self):
        self.assertEqual(m.check_copy_closure('COPY core/ ./core/\nCOPY --from=builder /app/main /app/main'),['core/'])
        with self.assertRaises(ValueError):m.check_copy_closure('COPY --chown=1000 core/ ./core/')

class DiskBudgetTests(unittest.TestCase):
    def setUp(self):
        spec=importlib.util.spec_from_file_location('build',Path(__file__).with_name('managed-build.py'))
        self.build=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.build)
    def test_exact_budget_boundary_and_one_block_below(self):
        gib=self.build.GIB
        stat=SimpleNamespace(f_blocks=400*gib//4096,f_frsize=4096,f_bavail=52*gib//4096)
        value=self.build.check_disk_budget(stat)
        self.assertEqual(value['service_reserve_bytes'],40*gib)
        stat.f_bavail-=1
        with self.assertRaises(RuntimeError):self.build.check_disk_budget(stat)
    def test_small_filesystem_keeps_absolute_reserve(self):
        gib=self.build.GIB
        stat=SimpleNamespace(f_blocks=100*gib//4096,f_frsize=4096,f_bavail=35*gib//4096)
        with self.assertRaises(RuntimeError):self.build.check_disk_budget(stat)
        stat.f_bavail=36*gib//4096
        self.assertEqual(self.build.check_disk_budget(stat)['service_reserve_bytes'],24*gib)
if __name__=='__main__':unittest.main()
