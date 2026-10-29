#!/usr/bin/env python3
"""Check current Maven/source boundaries and optional compiled SPI JDK dependencies."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[2]
NS = {"m": "http://maven.apache.org/POM/4.0.0"}
PROJECT_GROUP = "io.etcdhbase"
MODULES = {
    "protocol": set(), "spi": set(), "core": {"spi"}, "hbase": {"spi", "core"},
    "grpc": {"protocol", "spi", "core"}, "admin": {"hbase", "spi"},
    "server": {"protocol", "spi", "core", "hbase", "grpc", "admin"},
    "testkit": {"protocol", "spi"},
}
FORBIDDEN_DEPENDENCIES = {
    "protocol": ("org.apache.hbase", "org.apache.hadoop"),
    "core": ("org.apache.hbase", "org.apache.hadoop", "io.grpc", "com.google.protobuf"),
    "grpc": ("org.apache.hbase", "org.apache.hadoop"),
}
FORBIDDEN = {
    "spi": re.compile(r"\b(?:org\.apache\.(?:hadoop|hbase)|io\.grpc|com\.google\.protobuf|io\.etcdhbase\.(?:core|server|grpc|hbase|admin))\b"),
    "core": re.compile(r"\b(?:org\.apache\.(?:hadoop|hbase)|io\.grpc|com\.google\.protobuf|io\.etcdhbase\.(?:server|grpc|hbase|admin))\b"),
    "grpc": re.compile(r"\b(?:org\.apache\.(?:hadoop|hbase)|io\.etcdhbase\.(?:server|hbase|admin))\b"),
}
IMPORT = re.compile(r"^\s*import\s+(?:static\s+)?([A-Za-z0-9_.*]+)\s*;", re.MULTILINE)


def dependencies(pom):
    # dependencyManagement and plugin dependencies do not add production types.
    return pom.findall("m:dependencies/m:dependency", NS) + pom.findall("m:profiles/m:profile/m:dependencies/m:dependency", NS)


def coordinate(value):
    for expression in ("${project.groupId}", "${project.parent.groupId}", "${pom.groupId}", "${pom.parent.groupId}"):
        value = value.replace(expression, PROJECT_GROUP)
    if "${" in value or not value:
        raise ValueError("unresolved production dependency coordinate")
    return value


def inspect_dependencies(module, pom):
    count = 0
    for dependency in dependencies(pom):
        scope = dependency.findtext("m:scope", "compile", NS).strip()
        if scope not in {"compile", "test", "provided", "runtime", "system", "import"}:
            raise ValueError("unresolved dependency scope")
        if scope == "test":
            continue
        count += 1
        group = coordinate(dependency.findtext("m:groupId", "", NS).strip())
        artifact = coordinate(dependency.findtext("m:artifactId", "", NS).strip())
        if module == "spi":
            raise ValueError("SPI must not declare non-test dependencies")
        if any(group == prefix or group.startswith(prefix + ".") for prefix in FORBIDDEN_DEPENDENCIES.get(module, ())):
            raise ValueError(f"{module} has a forbidden backend or transport dependency")
        if group == PROJECT_GROUP and artifact not in MODULES[module]:
            raise ValueError(f"{module} has a reversed project dependency")
    return count


def verify(root=ROOT):
    parent = ET.parse(root / "pom.xml").getroot()
    if parent.findtext("m:groupId", "", NS) != PROJECT_GROUP:
        raise ValueError("project group coordinates require an explicit boundary review")
    configured = [item.text for item in parent.findall("m:modules/m:module", NS)]
    if len(configured) != len(MODULES) or set(configured) != set(MODULES):
        raise ValueError("M0 module inventory differs")
    # Any inherited production dependency would also change the SPI classpath.
    if any(item.findtext("m:scope", "compile", NS).strip() != "test" for item in dependencies(parent)):
        raise ValueError("parent must not inject production dependencies into SPI")
    sources, dependency_count = 0, 0
    for module in configured:
        pom = ET.parse(root / module / "pom.xml").getroot()
        if pom.findtext("m:parent/m:groupId", "", NS) != PROJECT_GROUP or pom.findtext("m:groupId", PROJECT_GROUP, NS) != PROJECT_GROUP:
            raise ValueError("module group coordinates require an explicit boundary review")
        dependency_count += inspect_dependencies(module, pom)
        java = sorted((root / module / "src/main/java").rglob("*.java"))
        if module == "spi" and not java:
            raise ValueError("SPI sources are missing")
        for path in java:
            content = path.read_text(encoding="utf-8")
            forbidden = FORBIDDEN.get(module)
            if forbidden and forbidden.search(content):
                raise ValueError(f"{module} references an implementation or forbidden transport/storage type")
            if module == "spi" and any(not name.startswith(("java.", "io.etcdhbase.spi.")) for name in IMPORT.findall(content)):
                raise ValueError("SPI imports a non-JDK external type")
            sources += 1
    return {"modules": len(configured), "production_sources": sources, "production_dependencies": dependency_count}


def compiled_spi(jar, jdk):
    if not jar.is_file():
        raise ValueError("packaged SPI JAR is missing")
    executable = Path(jdk) / "bin/jdeps"
    result = subprocess.run([str(executable), "--multi-release", "21", "--print-module-deps", str(jar)], text=True, capture_output=True)
    if result.returncode:
        raise ValueError("compiled SPI has missing or non-platform dependencies")
    modules = result.stdout.strip().split(",")
    if not modules or any(not re.fullmatch(r"java\.[a-z0-9_.]+", module) for module in modules):
        raise ValueError("compiled SPI is not restricted to public JDK modules")
    return modules


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jar", type=Path)
    arguments = parser.parse_args()
    result = verify()
    if arguments.jar:
        jdk = os.environ.get("ETCD_HBASE_JDK21_HOME")
        if not jdk:
            raise ValueError("set the locked JDK home for compiled SPI verification")
        result["compiled_jdk_modules"] = compiled_spi(arguments.jar, jdk)
    else:
        result["compiled_jdk_modules"] = "not checked; pass --jar after packaging"
    print(json.dumps(result))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, ET.ParseError):
        raise SystemExit("M0 SPI boundary verification failed") from None
