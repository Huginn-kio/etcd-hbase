"""Validate the complete frozen public matrix without treating sampled counters as total IO."""
import itertools
import json

DIGEST = "80e44c6c5c8acadad794501fa41b4bb3de0679454bfbd91b001fb3975e1dab96"
CASES = {"empty": 0, "small": 2, "recovery": 8, "tombstones": 4, "old_view": 16,
         "large_value": 1, "hot_fixed": 1, "binary": 8, "long_key": 1}
LAYOUTS = ("HISTORY", "NATIVE")
BUCKETS = (1, 4, 16)
PHASES = ("memstore_warming", "flushed_cache_reset_then_warming")
OBSERVATIONS = {"SERVER_ROWS_SCANNED", "BLOCK_BYTES_SCANNED", "FS_BYTES", "BLOCK_CACHE_BYTES", "MEMSTORE_BYTES", "SCANNER_RETRIES"}


def integer(value, minimum=0):
    if type(value) is not int or value < minimum:
        raise ValueError("invalid observed metric")


def _profile_records(stdout):
    return [json.loads(line.removeprefix("m1-profile "))
            for line in stdout.splitlines() if line.startswith("m1-profile ")]


def _benchmark_records(stdout):
    return [json.loads(line.removeprefix("m1-benchmark "))
            for line in stdout.splitlines() if line.startswith("m1-benchmark ")]


def _validate_profile(profile):
    frozen_profile = (
        profile["digest"], profile["seed"], profile["logicalKeys"], profile["mutations"],
        profile["servers"], profile["regionsPerTable"])
    expected_profile = (DIGEST, 20261030, 78, 118, 3, 16)
    if frozen_profile != expected_profile:
        raise ValueError("frozen dataset or resource profile changed")
    tables = 2 if profile["layout"] == "HISTORY" else 1
    if (profile["tables"] != tables or profile["applicationPutCalls"] != 118 * tables
            or profile["hotObservedRegions"] != 1):
        raise ValueError("actual auxiliary writes or hot-key locality evidence missing")
    for name, total in (("uniqueKeysPerBucket", 78), ("writesPerBucket", 118)):
        values = profile[name]
        if len(values) != profile["buckets"] or sum(values) != total:
            raise ValueError("bucket distribution evidence changed")
        for value in values:
            integer(value)


def _validate_profile_matrix(profiles):
    expected_profiles = set(itertools.product(LAYOUTS, BUCKETS))
    if len(profiles) != 6:
        raise ValueError("missing or duplicate public layout/bucket profile")
    observed_profiles = {(row["layout"], row["buckets"]) for row in profiles}
    if observed_profiles != expected_profiles:
        raise ValueError("missing or duplicate public layout/bucket profile")
    for profile in profiles:
        _validate_profile(profile)


def _validate_benchmark_row(row):
    if (row["oracleMatches"] is not True or row["samples"] != 3
            or row["returnedPerSample"] != CASES[row["case"]]):
        raise ValueError("logical oracle evidence changed")
    for name in ("returnedPhysicalRows", "returnedCellBytes", "observedScanRpcCalls", "applicationPointReads", "peakLeasedBytes", "p50Micros", "p95Micros", "p99Micros"):
        integer(row[name])
    has_positive_result = row["returnedPerSample"] > 0
    has_minimum_returned_rows = row["returnedPhysicalRows"] >= row["samples"] * row["returnedPerSample"]
    has_returned_cell_bytes = row["returnedCellBytes"] > 0
    has_peak_leased_bytes = row["peakLeasedBytes"] > 0
    if (has_positive_result
            and not (has_minimum_returned_rows and has_returned_cell_bytes and has_peak_leased_bytes)):
        raise ValueError("positive result contradicts scan cost")
    if row["peakLocalScannerPermits"] != 1 or row["peakLeasedBytes"] > (66 << 20) + (row["buckets"] + 1) * 16384:
        raise ValueError("declared application resource bound exceeded")
    if not row["p50Micros"] <= row["p95Micros"] <= row["p99Micros"]:
        raise ValueError("latency summary contradicts percentiles")
    if row["applicationPointReads"] != (3 if row["case"] == "large_value" else 0):
        raise ValueError("unselected values were fetched or value-read accounting missing")
    if set(row["observedServerCounters"]) != OBSERVATIONS:
        raise ValueError("sampled server counters missing")
    for value in row["observedServerCounters"].values():
        integer(value)


def _validate_benchmark_matrix(rows):
    expected_rows = set(itertools.product(LAYOUTS, BUCKETS, PHASES, CASES))
    if len(rows) != len(expected_rows):
        raise ValueError("missing or duplicate benchmark case")
    observed_rows = {(row["layout"], row["buckets"], row["phase"], row["case"]) for row in rows}
    if observed_rows != expected_rows:
        raise ValueError("missing or duplicate benchmark case")
    for row in rows:
        _validate_benchmark_row(row)


def collect(stdout):
    profiles = _profile_records(stdout)
    rows = _benchmark_records(stdout)
    _validate_profile_matrix(profiles)
    _validate_benchmark_matrix(rows)
    return {"digest": DIGEST, "profiles": profiles, "cases": rows,
            "scan_only_metrics": ["returnedPhysicalRows", "returnedCellBytes"],
            "capacity_accepted": False, "missing_input": "B05",
            "uncollected": ["point-read returned Result rows and serialized Cell bytes",
                            "complete META, point, close and late RPC counts", "physical disk IO",
                            "JVM/direct-memory peak", "target workload"]}
