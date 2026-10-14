# M0 etcd v3 wire baseline

The Java protocol module is generated from the vendored etcd v3.5.23 API
sources. [`protocol/upstream-lock.json`](../../protocol/upstream-lock.json)
pins the etcd commit, the imported proto sources, and their SHA-256 digests.
The baseline describes the three etcd API files in that lock; imported option
definitions are included in the generated descriptor set but do not add etcd
RPC services to the contract.

`protocol/baseline/wire-contract.json` records the Java descriptor view of:

- protobuf package and all service/method names, input/output types, and stream
  directions;
- every field number, type, label, message/enum type reference, oneof, and
  proto3-optional marker in the etcd API messages;
- enum names and numeric values.

The canonical wire contract excludes source comments and non-wire proto
options such as grpc-gateway HTTP annotations. Those inputs remain pinned by
the upstream source lock and are included in the generated descriptor set.

The Maven protobuf plugin writes a self-contained descriptor set to
`protocol/target/generated-resources/protobuf/descriptor-sets/etcd-v3.5.23.protobin`.
Generated Java and descriptor files remain build outputs and are not committed.
The contract test compares protoc's descriptor set, the generated Java
descriptors, and the checked-in baseline, and verifies the etcd files and their
transitive proto imports. Binary round-trip tests cover arbitrary bytes,
large int64 values, nested transaction oneofs, Watch request variants,
LeaseKeepAlive, Authenticate fields, and PrevKV event data.

Run the locked-source and generation checks with:

```sh
scripts/verify/protocol.sh
```

When intentionally changing the locked protocol baseline, update the source
lock and its review evidence first, then regenerate the contract explicitly:

```sh
python3 scripts/verify/vendor-proto.py \
  --lock protocol/upstream-lock.json \
  --root protocol/src/main/proto
scripts/dev/mvn21.sh -B -ntp -pl protocol -am \
  -Dm0.wire-contract.update=true test
scripts/verify/protocol.sh
```

The source contract is the upstream etcd v3.5.23 revision
[`1cf4bc40111fd95d1e14a90a09a146357b15eb13`](https://github.com/etcd-io/etcd/tree/1cf4bc40111fd95d1e14a90a09a146357b15eb13/api).
Descriptor generation uses the project-locked
[protobuf Maven plugin 0.6.1](https://www.xolstice.org/protobuf-maven-plugin/compile-mojo.html)
and locked protoc/protobuf-java versions. This records protocol compatibility;
it does not claim that the service implements every method in the descriptor.
