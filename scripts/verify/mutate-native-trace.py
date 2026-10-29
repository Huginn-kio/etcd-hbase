#!/usr/bin/env python3
"""Produce deliberate protocol corruption for native-oracle negative checks."""
import base64
import json
import sys

source, destination, mutation = sys.argv[1:]
with open(source, encoding="utf-8") as stream:
    records = [json.loads(line) for line in stream]

def record_for(case_id, method):
    for record in records:
        if record["case_id"] == case_id and record["method"] == method:
            return record
    raise SystemExit(f"mutation target missing: {case_id} {method}")

if mutation == "watch-event":
    target = next((record for record in records if record["method"] == "etcdserverpb.Watch/Watch" and record["response"] and record["response"].get("fields", {}).get("events")), None)
    if target is None:
        raise SystemExit("watch event mutation target missing")
    target["response"]["fields"]["events"].pop(0)
elif mutation == "more":
    target = record_for("C03", "etcdserverpb.KV/Range")
    fields = target["response"]["fields"]
    fields["more"] = not fields["more"]
elif mutation == "deleted":
    target = record_for("C09", "etcdserverpb.KV/DeleteRange")
    fields = target["response"]["fields"]
    fields["deleted"] = "0" if fields["deleted"] != "0" else "1"
elif mutation == "revision":
    target = next(record for record in records if record["method"] == "etcdserverpb.Watch/Watch" and record["response"] and record["response"].get("fields", {}).get("events"))
    kv_fields = target["response"]["fields"]["events"][0]["fields"]["kv"]["fields"]
    kv_fields["modRevision"] = str(int(kv_fields["modRevision"]) + 1)
elif mutation == "txn-branch":
    target = record_for("C11", "etcdserverpb.KV/Txn")
    fields = target["response"]["fields"]
    fields["succeeded"] = not fields["succeeded"]
elif mutation == "prev-kv":
    target = record_for("C09", "etcdserverpb.KV/DeleteRange")
    previous = target["response"]["fields"]["prevKvs"][0]["fields"]
    previous["value"] = base64.b64encode(b"changed").decode("ascii")
elif mutation == "error-kind":
    target = next(record for record in records if (record.get("error") or {}).get("semantic_kind") == "etcd/Compacted")
    target["error"]["semantic_kind"] = "etcd/FutureRev"
elif mutation == "count-existence":
    record_for("C05", "etcdserverpb.KV/Range")["response"]["fields"]["count"] = "0"
elif mutation == "newline":
    pass
elif mutation == "stream-terminal":
    index = next(index for index, record in enumerate(records) if record["method"] == "etcdserverpb.Watch/Watch" and record["stream_terminal"] is not None)
    records.pop(index)
else:
    raise SystemExit(f"unknown mutation {mutation}")

with open(destination, "x", encoding="utf-8") as stream:
    for record in records:
        stream.write(json.dumps(record, separators=(",", ":")) + ("" if mutation == "newline" and record is records[-1] else "\n"))
