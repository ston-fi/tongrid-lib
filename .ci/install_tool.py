"""Install one pinned CI executable after verifying its archive digest."""

import hashlib
import io
import json
import pathlib
import sys
import tarfile
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[1]


def install(name: str) -> None:
    tool = json.loads((ROOT / ".ci/tools.json").read_text())[name]
    directory = ROOT / ".tools"
    directory.mkdir(exist_ok=True)
    archive = directory / (name + ".tar.gz")
    if not archive.is_file() or hashlib.sha256(archive.read_bytes()).hexdigest() != tool["sha256"]:
        with urllib.request.urlopen(tool["url"], timeout=60) as response:
            data = response.read()
        if hashlib.sha256(data).hexdigest() != tool["sha256"]:
            raise ValueError("Tool archive checksum mismatch")
        archive.write_bytes(data)
    # Re-extract from the verified archive even on a cache hit. Never trust a cached executable.
    with tarfile.open(fileobj=io.BytesIO(archive.read_bytes()), mode="r:gz") as bundle:
        member = bundle.getmember(tool["member"])
        if not member.isfile() or pathlib.PurePosixPath(member.name).name != member.name:
            raise ValueError("Unexpected executable archive member")
        source = bundle.extractfile(member)
        if source is None:
            raise ValueError("Missing executable")
        executable = directory / member.name
        executable.write_bytes(source.read())
        executable.chmod(0o755)


if __name__ == "__main__":
    try:
        install(sys.argv[1])
    except Exception:
        print("Pinned tool installation failed.", file=sys.stderr)
        sys.exit(1)
