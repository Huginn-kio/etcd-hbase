#!/usr/bin/env python3
"""Check the pinned etcd v3.5.23 proto sources and generator versions."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from xml.etree import ElementTree

ROOT = Path(__file__).resolve().parents[2]
SOURCE_LOCK = ROOT / "protocol/proto-sources.lock.json"
DEPENDENCY_LOCK = ROOT / "build/dependency-lock.json"
EXPECTED_ETCD_REVISION = "1cf4bc40111fd95d1e14a90a09a146357b15eb13"
EXPECTED_ETCD_FILES = {
    "etcd/api/etcdserverpb/rpc.proto",
    "etcd/api/mvccpb/kv.proto",
    "etcd/api/authpb/auth.proto",
}


def load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read {path.relative_to(ROOT)}: {error}") from error


def verify() -> list[str]:
    source_lock = load_json(SOURCE_LOCK)
    dependency_lock = load_json(DEPENDENCY_LOCK)
    if source_lock.get("schemaVersion") != 1:
        raise ValueError("unsupported protocol source lock schema")
    if source_lock.get("protocol") != "etcd-v3.5.23":
        raise ValueError("protocol baseline must remain etcd-v3.5.23")

    sources = source_lock.get("sources")
    if not isinstance(sources, list) or not sources:
        raise ValueError("source lock must contain a non-empty sources list")

    entries = []
    etcd_files = set()
    for source in sources:
        if not isinstance(source, dict) or not isinstance(source.get("files"), list):
            raise ValueError("every source entry must contain a files list")
        for file_entry in source["files"]:
            if not isinstance(file_entry, dict):
                raise ValueError("every proto file entry must be an object")
            relpath = file_entry.get("path")
            if not isinstance(relpath, str) or Path(relpath).is_absolute() or ".." in Path(relpath).parts:
                raise ValueError("every source entry needs a safe repository-relative path")
            source_path = ROOT / "protocol/src/main/proto" / relpath
            try:
                content = source_path.read_bytes()
            except OSError as error:
                raise ValueError(f"missing locked proto source {relpath}") from error
            actual = hashlib.sha256(content).hexdigest()
            if actual != file_entry.get("sha256"):
                raise ValueError(f"SHA-256 mismatch for {relpath}: {actual}")
            entries.append(relpath)
            if source.get("name") == "etcd":
                etcd_files.add(relpath)

    if len(entries) != len(set(entries)):
        raise ValueError("duplicate proto source path in lock")
    actual_files = {
        path.relative_to(ROOT / "protocol/src/main/proto").as_posix()
        for path in (ROOT / "protocol/src/main/proto").rglob("*.proto")
    }
    if set(entries) != actual_files:
        missing = sorted(set(entries) - actual_files)
        unlocked = sorted(actual_files - set(entries))
        raise ValueError(f"proto source inventory differs from lock: missing={missing}, unlocked={unlocked}")

    etcd_source = next((source for source in sources if source.get("name") == "etcd"), None)
    if etcd_source is None or etcd_source.get("tag") != "v3.5.23":
        raise ValueError("etcd source tag must remain v3.5.23")
    if etcd_source.get("revision") != EXPECTED_ETCD_REVISION:
        raise ValueError("etcd v3.5.23 tag must resolve to the reviewed source commit")
    if etcd_files != EXPECTED_ETCD_FILES:
        raise ValueError(f"unexpected etcd proto set: {sorted(etcd_files)}")

    generation = source_lock.get("generation", {})
    native_tools = dependency_lock.get("nativeTools", {})
    protobuf_lock = dependency_lock.get("libraries", {}).get("protobuf", {})
    if generation.get("protoc") != native_tools.get("protoc", {}).get("version"):
        raise ValueError("protocol lock and dependency lock disagree on protoc")
    if generation.get("grpcJava") != native_tools.get("protocGenGrpcJava", {}).get("version"):
        raise ValueError("protocol lock and dependency lock disagree on grpc-java generator")
    protobuf_runtime_version = protobuf_lock.get("javaCoordinate", "").rsplit(":", 1)[-1]
    if generation.get("protobufJava") != protobuf_runtime_version:
        raise ValueError("protocol lock and dependency lock disagree on protobuf-java")

    for key, plugin_key in (("protobufMavenPlugin", "protobuf-maven-plugin"), ("osMavenPlugin", "os-maven-plugin")):
        if generation.get(key) != dependency_lock.get("plugins", {}).get(plugin_key, {}).get("version"):
            raise ValueError(f"protocol lock and dependency lock disagree on {plugin_key}")

    namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
    parent_pom = ElementTree.parse(ROOT / "pom.xml").getroot()
    properties = parent_pom.find("m:properties", namespace)
    if properties is None:
        raise ValueError("parent POM is missing version properties")
    if properties.findtext("m:protobuf.version", namespaces=namespace) != generation.get("protobufJava"):
        raise ValueError("parent POM does not use the locked protobuf-java version")
    if properties.findtext("m:grpc.version", namespaces=namespace) != generation.get("grpcJava"):
        raise ValueError("parent POM does not use the locked grpc-java version")
    managed_plugins = parent_pom.findall("m:build/m:pluginManagement/m:plugins/m:plugin", namespace)
    managed_versions = {
        plugin.findtext("m:artifactId", namespaces=namespace): plugin.findtext("m:version", namespaces=namespace)
        for plugin in managed_plugins
    }
    if managed_versions.get("protobuf-maven-plugin") != generation.get("protobufMavenPlugin"):
        raise ValueError("parent POM does not pin the expected protobuf-maven-plugin version")

    protocol_pom = ElementTree.parse(ROOT / "protocol/pom.xml").getroot()
    active_plugins = protocol_pom.findall("m:build/m:plugins/m:plugin", namespace)
    protobuf_plugin = next(
        (plugin for plugin in active_plugins
         if plugin.findtext("m:artifactId", namespaces=namespace) == "protobuf-maven-plugin"),
        None,
    )
    if protobuf_plugin is None:
        raise ValueError("protocol module does not activate protobuf-maven-plugin")
    configuration = protobuf_plugin.find("m:configuration", namespace)
    expected_protoc = "com.google.protobuf:protoc:${protobuf.version}:exe:${os.detected.classifier}"
    expected_grpc_plugin = "io.grpc:protoc-gen-grpc-java:${grpc.version}:exe:${os.detected.classifier}"
    if configuration is None or configuration.findtext("m:protocArtifact", namespaces=namespace) != expected_protoc:
        raise ValueError("protocol module protocArtifact does not match the locked configuration")
    if configuration.findtext("m:pluginId", namespaces=namespace) != "grpc-java":
        raise ValueError("protocol module does not activate the grpc-java generator")
    if configuration.findtext("m:pluginArtifact", namespaces=namespace) != expected_grpc_plugin:
        raise ValueError("protocol module grpc plugin artifact does not match the locked configuration")
    extensions = protocol_pom.findall("m:build/m:extensions/m:extension", namespace)
    os_extension = next((extension for extension in extensions if extension.findtext("m:artifactId", namespaces=namespace) == "os-maven-plugin"), None)
    if os_extension is None or os_extension.findtext("m:version", namespaces=namespace) != generation.get("osMavenPlugin"):
        raise ValueError("protocol module does not pin the expected os-maven-plugin extension")
    return entries


def main() -> int:
    try:
        entries = verify()
    except ValueError as error:
        print(f"M0 protocol source verification failed: {error}", file=sys.stderr)
        return 1
    print(f"Verified {len(entries)} pinned proto sources and generator versions.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
