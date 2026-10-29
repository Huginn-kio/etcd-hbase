#!/usr/bin/env python3
"""Collect real J08 contract, HBase regression and compiled-boundary evidence."""
import datetime
import hashlib
import json
from pathlib import Path
import shutil
import sys
from evidence import java_reports, require_unchanged_source

MODULES = ("protocol", "spi", "core", "hbase", "testkit")
REQUIRED = {"io.etcdhbase.spi.identity.BackendCredentialsTest", "io.etcdhbase.spi.identity.AuthorizedBackendIdentityTest",
            "io.etcdhbase.spi.salt.HashSaltStrategyTest", "io.etcdhbase.hbase.identity.HBaseBackendIdentityProviderTest",
            "io.etcdhbase.hbase.connection.HBaseRequestContextTest", "io.etcdhbase.hbase.probe.SecureHBaseAuthenticationIT",
            "io.etcdhbase.hbase.probe.LocalHBaseProbeIT", "io.etcdhbase.testkit.contract.IdentityProviderContractTest",
            "io.etcdhbase.testkit.contract.SaltStrategyContractTest"}


def collect(root, output):
    source = json.loads((output / "source.json").read_text())
    require_unchanged_source(root, source)
    paths, suites = java_reports(root, REQUIRED, MODULES)
    boundary = json.loads((output / "boundary.json").read_text())
    if boundary.get("modules") != 8 or boundary.get("compiled_jdk_modules") != ["java.base"]:
        raise ValueError("actual SPI JAR boundary was not verified")
    for path in paths:
        destination = output / "reports" / path.relative_to(root)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, destination)
    jar = root / "spi/target/spi-0.1.0-SNAPSHOT.jar"
    source.update(phase="J08", scope="public", ended_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  tests=sum(suite["tests"] for suite in suites.values()), failures=0, errors=0, skipped=0,
                  suites=suites, boundary=boundary, spi_jar_sha256=hashlib.sha256(jar.read_bytes()).hexdigest(),
                  unverified=["provider loading and published API compatibility J18/J19/J58", "per-session retirement and cross-replica identity J50/J51", "target HBase", "B01/B02/B03/B06", "I07/I08", "maintainer review"])
    (output / "summary.json").write_text(json.dumps(source, indent=2) + "\n")
    print(json.dumps({key: source[key] for key in ("phase", "checkout", "pr_head", "base", "tests", "boundary", "spi_jar_sha256")}))


if __name__ == "__main__":
    try:
        collect(Path(__file__).resolve().parents[2], Path(sys.argv[1]))
    except (ValueError, OSError, KeyError):
        raise SystemExit("J08 verification evidence rejected") from None
