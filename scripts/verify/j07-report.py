#!/usr/bin/env python3
"""Collect J07 protocol-tool evidence without claiming Java business equivalence."""
import datetime
import json
from pathlib import Path
import shutil
import sys
from evidence import go_report, java_reports, require_unchanged_source

REQUIRED = {"io.etcdhbase.server.GeneralGoInteropIT", "io.etcdhbase.server.ExternalProtocolIT",
            "io.etcdhbase.server.BootstrapTest", "io.etcdhbase.core.request.RequestScopeTest"}


def collect(root, output):
    paths, suites = java_reports(root, REQUIRED)
    source = json.loads((output / "source.json").read_text())
    require_unchanged_source(root, source)
    tests = go_report(output / "go-test.jsonl")
    race = go_report(output / "go-race.jsonl")
    native = output / "native"
    metadata = [json.loads((native / run / "metadata.json").read_text()) for run in ("run-one", "run-two")]
    for item in metadata:
        if item["protocol"] != "etcd-v3.5.23" or item["source_revision"] != "1cf4bc40111fd95d1e14a90a09a146357b15eb13" or len(item["modules"]) != 3:
            raise ValueError("native version evidence is inconsistent")
    mutations = ("watch-event", "more", "deleted", "revision", "txn-branch", "prev-kv", "stream-terminal", "error-kind", "count-existence", "newline")
    for mutation in mutations:
        if not (native / f"mutated-{mutation}.jsonl").is_file():
            raise ValueError("native negative-control input is missing")
        message = (native / f"mutated-{mutation}.log").read_text()
        if not message.startswith(("trace record ", "record count differs:", "read right trace:", "normalize right trace:")):
            raise ValueError("native negative control failed for an unrelated reason")
    if (native / "normalized-one.jsonl").read_bytes() != (native / "normalized-two.jsonl").read_bytes():
        raise ValueError("native normalized evidence differs")
    records = [json.loads(line) for line in (output / "wire/transport.jsonl").read_text().splitlines()]
    if len(records) != 10 or {record["evidence_scope"] for record in records} != {"transport", "transport-fixture"}:
        raise ValueError("transport observations are missing or wrongly scoped")
    for path in paths:
        destination = output / "reports" / path.relative_to(root)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, destination)
    source.update(phase="J07", scope="public", ended_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  java_tests=sum(suite["tests"] for suite in suites.values()), java_suites=suites,
                  go=tests, race=race, native_runs=2, rejected_mutations=len(mutations),
                  transport_records=10, external_methods=7, descriptor_methods=41, cases_in_inventory=33,
                  unverified=["C01-C33 complete Java business semantics", "target HBase", "B01/B02/B03/B06", "I07/I08", "maintainer review"])
    (output / "summary.json").write_text(json.dumps(source, indent=2) + "\n")
    print(json.dumps({key: source[key] for key in ("phase", "checkout", "pr_head", "base", "java_tests", "go", "race", "native_runs", "transport_records")}))


if __name__ == "__main__":
    try:
        collect(Path(__file__).resolve().parents[2], Path(sys.argv[1]))
    except (ValueError, KeyError, OSError) as error:
        raise SystemExit(f"J07 evidence rejected: {type(error).__name__}") from None
