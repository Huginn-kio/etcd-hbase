#!/usr/bin/env python3
"""Report I07/I08 decisions without granting or performing publication."""
import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate release decision field")
        result[key] = value
    return result


def inspect(root=ROOT):
    path = root / "build/release-decisions.json"
    if path.stat().st_size > 65536:
        raise ValueError("release decisions exceed their size budget")
    data = json.loads(path.read_text(), object_pairs_hook=unique_object)
    if not isinstance(data, dict) or set(data) != {"schema_version", "I07", "I08"} or type(data["schema_version"]) is not int or data["schema_version"] != 1:
        raise ValueError("unsupported release decision schema")
    fields = {"I07": {"state", "license_spdx", "third_party_notice_reviewed", "approval_reference"},
              "I08": {"state", "coordinates", "publication_destination", "authorization_rules", "approval_reference"}}
    pending = []
    for key, required in fields.items():
        entry = data[key]
        if not isinstance(entry, dict) or set(entry) != required or not isinstance(entry["state"], str) or entry["state"] not in {"waiting_input", "approved"}:
            raise ValueError("unknown release decision state or fields")
        if entry["state"] == "waiting_input":
            pending.append(key)
            continue
        text_fields = required - {"state", "third_party_notice_reviewed"}
        if any(not isinstance(entry[name], str) or not entry[name].strip() for name in text_fields):
            raise ValueError("approved release decisions need explicit values and references")
        if key == "I07" and (entry["third_party_notice_reviewed"] is not True or not all((root / name).is_file() and (root / name).stat().st_size > 0 for name in ("LICENSE", "NOTICE"))):
            raise ValueError("approved licensing needs reviewed notices and actual LICENSE/NOTICE")
    return {"decision_inputs_complete": not pending, "waiting_inputs": pending,
            "publication_performed": False, "publication_authorized_by_this_check": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--require-decisions", action="store_true")
    arguments = parser.parse_args()
    result = inspect()
    encoded = json.dumps(result, indent=2) + "\n"
    if arguments.output:
        arguments.output.write_text(encoded)
    print(encoded, end="")
    return 2 if arguments.require_decisions and result["waiting_inputs"] else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError, KeyError):
        raise SystemExit("release decisions are invalid") from None
