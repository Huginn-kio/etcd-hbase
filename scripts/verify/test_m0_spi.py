#!/usr/bin/env python3
"""Negative controls for production SPI and module dependency boundaries."""
import importlib.util
from pathlib import Path
import shutil
import tempfile
import unittest
import xml.etree.ElementTree as ET

SPEC = importlib.util.spec_from_file_location("m0_spi", Path(__file__).with_name("m0-spi.py"))
boundary = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(boundary)
NS = "{http://maven.apache.org/POM/4.0.0}"


class BoundaryTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        shutil.copyfile(boundary.ROOT / "pom.xml", self.root / "pom.xml")
        for module in boundary.MODULES:
            destination = self.root / module
            destination.mkdir()
            shutil.copyfile(boundary.ROOT / module / "pom.xml", destination / "pom.xml")
            source = boundary.ROOT / module / "src/main/java"
            if source.is_dir():
                shutil.copytree(source, destination / "src/main/java")

    def inject_dependency(self, module, group, artifact, scope="compile", profile=False):
        path = self.root / module / "pom.xml" if module else self.root / "pom.xml"
        tree = ET.parse(path)
        owner = tree.getroot()
        if profile:
            profiles = ET.SubElement(owner, NS + "profiles")
            owner = ET.SubElement(profiles, NS + "profile")
            ET.SubElement(owner, NS + "id").text = "unexpected"
        dependencies = ET.SubElement(owner, NS + "dependencies")
        dependency = ET.SubElement(dependencies, NS + "dependency")
        for field, value in (("groupId", group), ("artifactId", artifact), ("scope", scope)):
            ET.SubElement(dependency, NS + field).text = value
        tree.write(path)

    def test_non_jdk_source_references_are_rejected(self):
        self.assertEqual(8, boundary.verify(self.root)["modules"])
        source = self.root / "spi/src/main/java/External.java"
        for content in ("import example.vendor.Runtime;", "class Leaked { org.apache.hadoop.hbase.TableName name; }"):
            source.write_text(content)
            with self.assertRaises(ValueError):
                boundary.verify(self.root)

    def test_profile_and_inherited_production_dependencies_are_rejected(self):
        self.inject_dependency("spi", "example.vendor", "api", "test", profile=True)
        boundary.verify(self.root)
        self.inject_dependency("spi", "example.vendor", "api", "provided", profile=True)
        with self.assertRaises(ValueError):
            boundary.verify(self.root)

    def test_reversed_and_parent_injected_dependencies_are_rejected(self):
        self.inject_dependency("core", "io.etcdhbase", "server")
        with self.assertRaises(ValueError):
            boundary.verify(self.root)
        shutil.copyfile(boundary.ROOT / "core/pom.xml", self.root / "core/pom.xml")
        self.inject_dependency("", "example.vendor", "runtime")
        with self.assertRaises(ValueError):
            boundary.verify(self.root)

    def test_unused_backend_dependencies_cannot_enter_core_or_grpc(self):
        self.inject_dependency("core", "org.apache.hbase", "hbase-client")
        with self.assertRaises(ValueError):
            boundary.verify(self.root)
        shutil.copyfile(boundary.ROOT / "core/pom.xml", self.root / "core/pom.xml")
        self.inject_dependency("grpc", "org.apache.hadoop", "hadoop-common")
        with self.assertRaises(ValueError):
            boundary.verify(self.root)

    def test_group_property_aliases_cannot_bypass_project_boundaries(self):
        for group in ("${project.groupId}", "${project.parent.groupId}", "${pom.parent.groupId}", "${unresolvedGroup}"):
            shutil.copyfile(boundary.ROOT / "core/pom.xml", self.root / "core/pom.xml")
            self.inject_dependency("core", group, "protocol")
            with self.subTest(group=group), self.assertRaises(ValueError):
                boundary.verify(self.root)


if __name__ == "__main__":
    unittest.main()
