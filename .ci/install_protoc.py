"""Install reviewed protoc 29.3 archives without trusting cached executables."""

import hashlib
import io
import json
import pathlib
import platform
import shutil
import urllib.request
import zipfile

ROOT = pathlib.Path(__file__).resolve().parents[1]


def install() -> pathlib.Path:
    system = platform.system().lower()
    machine = {"aarch64": "arm64", "AMD64": "x86_64"}.get(platform.machine(), platform.machine())
    name = f"{system}-{machine}"
    tool = json.loads((ROOT / ".ci/protoc.json").read_text())[name]
    directory = ROOT / ".tools"
    directory.mkdir(exist_ok=True)
    cached = directory / f"protoc-29.3-{name}.zip"
    if cached.is_file():
        data = cached.read_bytes()
    else:
        with urllib.request.urlopen(tool["url"], timeout=60) as response:
            data = response.read()
    if hashlib.sha256(data).hexdigest() != tool["sha256"]:
        raise ValueError("protoc archive checksum mismatch")
    cached.write_bytes(data)
    dest = directory / "protoc"
    if dest.exists():
        shutil.rmtree(dest)
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        for member in archive.infolist():
            path = pathlib.PurePosixPath(member.filename)
            if path.is_absolute() or ".." in path.parts:
                raise ValueError("Unsafe protoc archive path")
            if member.is_dir() or not (member.filename == "bin/protoc" or member.filename.startswith("include/")):
                continue
            target = dest / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(archive.read(member))
    executable = dest / "bin/protoc"
    executable.chmod(0o755)
    return executable


if __name__ == "__main__":
    print(install())
