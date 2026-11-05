import importlib.util
import io
import json
from pathlib import Path
import struct
import tarfile
import tempfile
from types import SimpleNamespace
import unittest

spec=importlib.util.spec_from_file_location('artifact',Path(__file__).with_name('managed-artifact.py'))
a=importlib.util.module_from_spec(spec);spec.loader.exec_module(a)


def elf(interpreter=False, machine=62):
    raw=bytearray(120);raw[:6]=b'\x7fELF\x02\x01';struct.pack_into('<H',raw,18,machine)
    struct.pack_into('<Q',raw,32,64);struct.pack_into('<HH',raw,54,56,1);struct.pack_into('<I',raw,64,3 if interpreter else 1)
    return bytes(raw)


class RuntimeArtifactTests(unittest.TestCase):
    def setUp(self):
        self.revision='a'*40
        import hashlib
        self.main=elf();self.value={'schema':1,'revision':self.revision,'source_archive_sha256':'b'*64,
          'main_sha256':hashlib.sha256(self.main).hexdigest(),'main_bytes':len(self.main),
          'local_image':'sha256:'+'c'*64,'base_reference':a.BASE}
    def package(self, extra=None, recipe=None, main=None, value=None, symlink=False):
        output=io.BytesIO()
        with tarfile.open(fileobj=output,mode='w:gz') as tar:
            rows=[('main',self.main if main is None else main),('artifact.json',json.dumps(value or self.value).encode()),('Dockerfile.managed-runtime',(a.RECIPE if recipe is None else recipe).encode())]
            if extra:rows.append(extra)
            for name,raw in rows:
                entry=tarfile.TarInfo(name);entry.size=len(raw)
                if symlink and name=='main':entry.type=tarfile.SYMTYPE;entry.linkname='/tmp/foreign';entry.size=0
                tar.addfile(entry,None if entry.issym() else io.BytesIO(raw))
        return output.getvalue()
    def test_runtime_recipe_is_exact_and_has_no_run_or_dependency_download(self):
        self.assertEqual((Path(__file__).resolve().parents[1]/'transports/Dockerfile.managed-runtime').read_text(),a.RECIPE)
        self.assertNotIn('\nRUN ',a.RECIPE)
    def test_small_assembly_matches_full_build_final_runtime_contract(self):
        full=(Path(__file__).resolve().parents[1]/'transports/Dockerfile.managed').read_text()
        final='FROM '+full.rsplit('\nFROM ',1)[1]
        self.assertEqual(final.strip(),a.RECIPE.replace('COPY --chmod=0755 main /app/main','COPY --from=builder /app/main /app/main').strip())
    def test_roundtrip_checks_binary_and_preserves_bytes(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)/'package';value=a.unpack_package(self.package(),root,self.revision)
            self.assertEqual(value,self.value);self.assertEqual((root/'main').read_bytes(),self.main)
            self.assertEqual((root/'main').stat().st_mode & 0o777,0o755)
            with self.assertRaises(AssertionError):a.unpack_package(self.package(),root,self.revision)
    def test_extra_paths_symlinks_and_recipe_injection_rejected(self):
        for raw in [self.package(extra=('../foreign',b'x')),self.package(symlink=True),self.package(recipe=a.RECIPE+'RUN echo untrusted\n')]:
            with tempfile.TemporaryDirectory() as temp:
                with self.assertRaises(AssertionError):a.unpack_package(raw,Path(temp)/'package',self.revision)
    def test_wrong_source_or_binary_hash_rejected(self):
        for value in [{**self.value,'revision':'d'*40},{**self.value,'main_sha256':'d'*64},{**self.value,'main_bytes':len(self.main)+1},{**self.value,'unexpected_key':'data'},{**self.value,'schema':True}]:
            with tempfile.TemporaryDirectory() as temp:
                with self.assertRaises(AssertionError):a.unpack_package(self.package(value=value),Path(temp)/'package',self.revision)
    def test_dynamic_or_wrong_architecture_not_accepted(self):
        for raw in [elf(interpreter=True),elf(machine=183),b'not-a-static-binary']:
            with tempfile.TemporaryDirectory() as temp:
                p=Path(temp)/'main';p.write_bytes(raw)
                with self.assertRaises(AssertionError):a.static_amd64(p)
    def test_assembly_reserves_service_disk_and_only_small_copy_headroom(self):
        gib=1024**3;stat=SimpleNamespace(f_blocks=400*gib//4096,f_frsize=4096,f_bavail=43*gib//4096)
        self.assertEqual(a.assembly_budget(stat)['assembly_headroom_bytes'],3*gib)
        stat.f_bavail-=1
        with self.assertRaises(RuntimeError):a.assembly_budget(stat)


if __name__=='__main__':unittest.main()
