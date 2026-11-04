import importlib.util
from pathlib import Path
import unittest
s=importlib.util.spec_from_file_location('bundle',Path(__file__).with_name('managed-build-bundle.py'));m=importlib.util.module_from_spec(s);s.loader.exec_module(m)

class ClosureTests(unittest.TestCase):
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
if __name__=='__main__':unittest.main()
