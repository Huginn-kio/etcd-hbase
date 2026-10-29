#!/usr/bin/env python3
"""Independently verify the frozen public CRC32-IEEE salt fixture with Python's standard library."""

import argparse
import binascii
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
IDENTIFIERS = {"", "61", "2f612f622f632f", "00", "ff", "6100ff", "2f726f6f74", "2f726f6f742f"}


def unique_properties(pairs):
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError("duplicate JSON property")
        result[name] = value
    return result


def verify(path):
    with path.open("rb") as source:
        data = source.read(65_537)
    if len(data) > 65_536:
        raise ValueError("salt fixture exceeds its size limit")
    profile = json.loads(data.decode("utf-8"), object_pairs_hook=unique_properties)
    for name, expected in {"schema": "etcd-hbase-salt-profile/v1", "strategy_id": "crc32-bucket-v1",
                           "encoding_version": 1, "bucket_count": 16, "salt_width_bytes": 2,
                           "bucket_encoding": "unsigned-fixed-width-big-endian"}.items():
        value = profile[name]
        if type(value) is not type(expected) or value != expected:
            raise ValueError("public salt profile metadata drifted")
    expected_splits = [bucket.to_bytes(2, "big").hex() for bucket in range(1, 16)]
    if profile["split_keys_hex"] != expected_splits:
        raise ValueError("public salt split boundaries drifted")
    vectors = profile["vectors"]
    if len(vectors) != 8 or {vector["identity_hex"] for vector in vectors} != IDENTIFIERS:
        raise ValueError("public salt vectors are incomplete")
    for vector in vectors:
        checksum = binascii.crc32(bytes.fromhex(vector["identity_hex"])) & 0xFFFFFFFF
        bucket = checksum % 16
        if (vector["crc32_hex"] != f"{checksum:08x}" or type(vector["bucket"]) is not int
                or vector["bucket"] != bucket or vector["salt_hex"] != bucket.to_bytes(2, "big").hex()):
            raise ValueError("public salt vector does not match CRC32-IEEE and big-endian encoding")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", type=Path, default=ROOT / "config/examples/j05/salt-profile.json")
    args = parser.parse_args()
    try:
        verify(args.profile)
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        parser.exit(1, "salt-profile: invalid or inconsistent public salt fixture\n")
    print("public salt fixture verified independently: vectors=8, split_keys=15")


if __name__ == "__main__":
    main()
