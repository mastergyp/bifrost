#!/usr/bin/env python3
"""Build a fixed-source managed artifact without changing running services."""
import argparse,hashlib,json,os,re,socket,subprocess,fcntl
from pathlib import Path

BASE_IMAGE="docker.io/maximhq/bifrost@sha256:74826952fd6f1fc32f7bf8c364c41e27d996d0d03e4157ca8b55ddadaacb0668"
GIB=1024**3
def check_disk_budget(stat):
    total=stat.f_blocks*stat.f_frsize
    available=stat.f_bavail*stat.f_frsize
    reserve=max(total//10,24*GIB)
    headroom=12*GIB
    assert total>0 and available>=0
    if available < reserve+headroom:
        raise RuntimeError('Managed build refused: insufficient free space for 12GiB build headroom plus 10%/24GiB service reserve; inspect capacity. No automatic cleanup or runtime changes.')
    return {'total_bytes':total,'available_bytes':available,'service_reserve_bytes':reserve,'build_headroom_bytes':headroom}
def save(path,value):
    with path.open("x") as out:json.dump(value,out,indent=2);out.write("\n")
def main(args):
    assert os.geteuid()==0 and socket.gethostname()=="VM-1-9-ubuntu"
    assert re.fullmatch("[0-9a-f]{40}",args.revision)
    base=Path("/data/releases/shinewing-bifrost-managed")/args.revision
    assert Path(__file__).resolve()==base/"source/scripts/managed-build.py"
    metadata=json.loads((base/"source-archive.json").read_text())
    assert metadata["revision"]==args.revision and hashlib.sha256((base/"source.tar.gz").read_bytes()).hexdigest()==metadata["archive_sha256"]
    capacity=check_disk_budget(os.statvfs('/data'))
    root=base/("build-r1" if args.target=="main" else "probe-build-r1")
    root.mkdir(mode=0o700,exist_ok=True)
    save(root/"intent.json",{"revision":args.revision,"target":args.target,"source_archive_sha256":metadata["archive_sha256"],"capacity_preflight":capacity})
    command=["docker","build","--build-arg","APK_MIRROR=https://mirrors.cloud.tencent.com/alpine","--build-arg","GOPROXY=https://goproxy.cn,https://proxy.golang.org,direct","--build-arg","SOURCE_REVISION="+args.revision,"--iidfile",str(root/"image.id"),"-f","transports/Dockerfile.managed","-t","shinewing-bifrost-managed:"+args.revision+("-probe" if args.target=="probe" else "")]
    if args.target=="probe":command += ["--target","profile-probe"]
    with (root/"build.private.log").open("x") as out:subprocess.run(command+["."],cwd=base/"source",stdout=out,stderr=subprocess.STDOUT,check=True)
    image=(root/"image.id").read_text().strip();assert re.fullmatch("sha256:[0-9a-f]{64}",image)
    inspect=lambda value:json.loads(subprocess.check_output(["docker","image","inspect",value]))[0]
    actual,original=inspect(image),inspect(BASE_IMAGE)
    if args.target=="main":
        assert actual["Config"]["Labels"]["org.opencontainers.image.revision"]==args.revision
        for field in ["User","Entrypoint","Cmd","Env","WorkingDir","Volumes","Healthcheck","ExposedPorts"]:assert actual["Config"].get(field)==original["Config"].get(field),field
    proof={"revision":args.revision,"image":image,"target":args.target,"archive_sha256":metadata["archive_sha256"],"runtime_base_image":original["Id"],"production_activated":False}
    save(root/"image.json",proof);print(json.dumps(proof),flush=True)
if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("--revision",required=True);p.add_argument("--target",choices=["main","probe"],default="main")
    os.umask(0o077)
    with Path('/run/shinewing-managed-build.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        main(p.parse_args())
