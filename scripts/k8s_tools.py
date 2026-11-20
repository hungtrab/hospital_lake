"""Install pinned Kubernetes demo tools into ignored results/tools/bin, no sudo."""
from __future__ import annotations
import hashlib
import io
import platform
import tarfile
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BIN = ROOT/'results/tools/bin'


def install(name, url, checksum_url, member=None):
    def read(address):
        with urllib.request.urlopen(address, timeout=120) as response:
            return response.read()
    body = read(url)
    expected = read(checksum_url).decode().strip().split()[0]
    if hashlib.sha256(body).hexdigest() != expected:
        raise ValueError(f'{name}: SHA256 mismatch')
    if member:
        with tarfile.open(fileobj=io.BytesIO(body), mode='r:gz') as archive:
            body = archive.extractfile(member).read()
    path = BIN/name
    path.write_bytes(body)
    path.chmod(0o755)
    print(f'Installed {name}: {path}', flush=True)


def main():
    arch = {'x86_64':'amd64', 'aarch64':'arm64'}.get(platform.machine())
    if platform.system() != 'Linux' or not arch:
        raise SystemExit('This local installer supports Linux amd64/arm64; install the pinned tools manually elsewhere')
    BIN.mkdir(parents=True, exist_ok=True)
    kind = f'https://github.com/kubernetes-sigs/kind/releases/download/v0.30.0/kind-linux-{arch}'
    kubectl = f'https://dl.k8s.io/release/v1.34.0/bin/linux/{arch}/kubectl'
    helm = f'https://get.helm.sh/helm-v3.19.0-linux-{arch}.tar.gz'
    with ThreadPoolExecutor(max_workers=3) as pool:
        jobs = [pool.submit(install, 'kind', kind, kind+'.sha256sum'),
                pool.submit(install, 'kubectl', kubectl, kubectl+'.sha256'),
                pool.submit(install, 'helm', helm, helm+'.sha256sum', f'linux-{arch}/helm')]
        for job in jobs:
            job.result()


if __name__ == '__main__':
    main()
