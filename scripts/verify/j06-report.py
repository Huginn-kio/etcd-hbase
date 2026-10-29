#!/usr/bin/env python3
"""Fail closed on missing, zero, skipped or failed J06 verification evidence."""
import datetime
import json
from pathlib import Path
import sys
import xml.etree.ElementTree as ET

REQUIRED = {
    "io.etcdhbase.core.request.RequestScopeTest",
    "io.etcdhbase.hbase.connection.HBaseRequestContextTest",
    "io.etcdhbase.hbase.probe.LocalHBaseProbeIT",
    "io.etcdhbase.hbase.probe.SecureHBaseAuthenticationIT",
    "io.etcdhbase.server.config.RuntimeConfigurationLoaderTest",
    "io.etcdhbase.server.config.RuntimeConfigurationSchemaTest",
    "io.etcdhbase.server.observability.RequestMetricsTest",
    "io.etcdhbase.server.RuntimeServerTest",
    "io.etcdhbase.server.RequestLifecycleTest",
    "io.etcdhbase.server.GoInteropIT",
}


def inspect_reports(paths):
    suites = {}
    for path in paths:
        suite = ET.parse(path).getroot()
        name = suite.attrib["name"]
        counts = {field: int(suite.attrib[field]) for field in ("tests", "failures", "errors", "skipped")}
        if name in suites or counts["tests"] < 1 or any(counts[field] for field in ("failures", "errors", "skipped")):
            raise ValueError("duplicate, empty, skipped or failing test evidence")
        cases = suite.findall("testcase")
        if len(cases) != counts["tests"] or any(case.find(tag) is not None for case in cases for tag in ("failure", "error", "skipped")):
            raise ValueError("test case evidence disagrees with suite counters")
        suites[name] = counts
    if not REQUIRED.issubset(suites):
        raise ValueError("required J06 verification suites are missing")
    return suites


def main():
    root = Path(__file__).resolve().parents[2]
    output = Path(sys.argv[1]).resolve()
    paths = sorted(root.glob("*/target/surefire-reports/TEST-*.xml")) + sorted(root.glob("*/target/failsafe-reports/TEST-*.xml"))
    suites = inspect_reports(paths)
    if "j06-hbase realScannerClosed=true sharedConnectionPreserved=true readAfterCancel=true" not in (output / "maven.log").read_text():
        raise ValueError("real HBase request lifecycle evidence is missing")
    source = json.loads((output / "source.json").read_text())
    source.update({"phase": "J06", "scope": "public-local", "ended_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                   "tests": sum(suite["tests"] for suite in suites.values()), "failures": 0, "errors": 0, "skipped": 0,
                   "suites": suites, "unverified": ["target HBase identity/permissions/TLS", "B01", "I07/I08", "published commit recovery"]})
    (output / "summary.json").write_text(json.dumps(source, indent=2) + "\n")
    print(f"J06 public verification passed: {source['tests']} tests, no failures/errors/skips; {output / 'summary.json'}")


if __name__ == "__main__":
    try:
        main()
    except (KeyError, ValueError, OSError, ET.ParseError) as failure:
        raise SystemExit(f"J06 evidence rejected: {type(failure).__name__}") from None
