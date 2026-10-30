#!/usr/bin/env python3
"""Check public dependency settings, immutable CI actions, runtime pin and actual Go isolation."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[2]
GO_POLICY = {"GOENV": "", "GOAUTH": "off", "GOPROXY": "https://proxy.golang.org", "GOSUMDB": "sum.golang.org",
             "GOPRIVATE": "", "GONOPROXY": "", "GONOSUMDB": "", "GOINSECURE": "", "GOVCS": "*:off",
             "GOTOOLCHAIN": "local", "GOWORK": "off", "GOFLAGS": "-mod=readonly"}


def static_policy(root=ROOT):
    lock = json.loads((root / "build/dependency-lock.json").read_text())
    settings = ET.parse(root / "build/maven-settings-public.xml").getroot()
    namespace = {"m": "http://maven.apache.org/SETTINGS/1.2.0"}
    mirrors = settings.findall("m:mirrors/m:mirror", namespace)
    if len(mirrors) != 1 or mirrors[0].findtext("m:url", "", namespace) != "https://repo.maven.apache.org/maven2" or mirrors[0].findtext("m:mirrorOf", "", namespace) != "*" or settings.findall("m:servers", namespace):
        raise ValueError("public Maven settings must use only Central without credentials")
    runtime = lock["runtimeImage"]
    if runtime["javaRelease"] != lock["jdk"]["release"] or runtime["platform"] != "linux/amd64" or not re.fullmatch(r"sha256:[0-9a-f]{64}", runtime["platformDigest"]) or not runtime["reference"].endswith("@" + runtime["platformDigest"]):
        raise ValueError("runtime platform and locked JDK patch differ")
    if (root / "server/src/main/docker/Dockerfile.m0").read_text().splitlines()[0] != "FROM " + runtime["reference"]:
        raise ValueError("Dockerfile does not use the exact locked image")
    workflows = sorted((root / ".github/workflows").glob("*.yml"))
    if not workflows:
        raise ValueError("public verification workflows are missing")
    for workflow in workflows:
        content = workflow.read_text()
        if "pull_request_target" in content or re.search(r"secrets\s*[.\[]", content) or re.search(r"permissions\s*:\s*(?:write-all|[\s\S]*?contents\s*:\s*write)", content):
            raise ValueError("M0 public CI must not request target secrets or write access")
        uses = re.findall(r"^\s*-?\s*uses:\s*([^\s#]+)", content, re.MULTILINE)
        for action in uses:
            name, separator, revision = action.partition("@")
            if not separator or not re.fullmatch(r"[0-9a-f]{40}", revision) or lock["actions"].get(name) != revision:
                raise ValueError("CI action is not an approved immutable pin")
        if "persist-credentials: false" not in content:
            raise ValueError("checkout credentials must not persist")
    return {"workflows": len(workflows), "immutable_action_pins": len(lock["actions"]),
            "runtime_base": runtime["reference"], "public_maven_only": True}


def actual_go_policy(root=ROOT):
    result = subprocess.run([str(root / "scripts/dev/go-m0.sh"), "env", "-json", *GO_POLICY],
                            cwd=root, text=True, capture_output=True, timeout=300, check=True)
    environment = json.loads(result.stdout)
    if environment != GO_POLICY:
        raise ValueError("actual Go environment differs from public verification policy")
    return environment


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    result = static_policy()
    result["actual_go_environment"] = actual_go_policy()
    arguments.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError, ET.ParseError, subprocess.SubprocessError):
        raise SystemExit("public build policy rejected") from None
