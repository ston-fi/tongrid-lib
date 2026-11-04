"""Prepare only pinned public native sources in the ignored build cache."""
import json
import pathlib
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[1]

def prepare() -> None:
    pins = json.loads((ROOT / ".ci/native.json").read_text())
    destination = ROOT / "target/native-ton"
    url = "https://github.com/ton-blockchain/ton"
    if not destination.exists():
        destination.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(["git", "clone", "--depth", "1", "--branch", pins["tonTag"],
                        "--recurse-submodules", "--shallow-submodules", url, str(destination)], check=True)
    head = subprocess.check_output(["git", "-C", str(destination), "rev-parse", "HEAD"], text=True).strip()
    remote = subprocess.check_output(["git", "-C", str(destination), "remote", "get-url", "origin"], text=True).strip()
    if head != pins["tonRevision"] or remote.rstrip("/") not in {url, url + ".git"}:
        raise ValueError("Native source cache does not match the reviewed revision")
    print("Native source revision verified.")

if __name__ == "__main__":
    prepare()
