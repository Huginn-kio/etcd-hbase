# M0 etcd protocol sources

The API contract is pinned to etcd v3.5.23 at commit
[`1cf4bc40111fd95d1e14a90a09a146357b15eb13`](https://github.com/etcd-io/etcd/tree/1cf4bc40111fd95d1e14a90a09a146357b15eb13).
Original files are vendored byte-for-byte under `src/main/proto/`. The lock
file records each source repository, revision, upstream path, license, local
path, and SHA-256.

| Source | Revision | Upstream path | Local path | License | SHA-256 |
|---|---|---|---|---|---|
| etcd | `1cf4bc40111fd95d1e14a90a09a146357b15eb13` | `api/etcdserverpb/rpc.proto` | `src/main/proto/etcd/api/etcdserverpb/rpc.proto` | Apache-2.0 | `7cf0d098aae57522a574bf591145e55539a15212885d43709536553833cd6f40` |
| etcd | `1cf4bc40111fd95d1e14a90a09a146357b15eb13` | `api/mvccpb/kv.proto` | `src/main/proto/etcd/api/mvccpb/kv.proto` | Apache-2.0 | `76b0d596ecc8dd1b535be683777910a5b4fe0705fb57baccacfcc7d98d11821e` |
| etcd | `1cf4bc40111fd95d1e14a90a09a146357b15eb13` | `api/authpb/auth.proto` | `src/main/proto/etcd/api/authpb/auth.proto` | Apache-2.0 | `443e661f9e423d3376d513e76446e957ae0132c6f63fff4510fde3c7a587be05` |
| gogo/protobuf | `b03c65ea87cdc3521ede29f62fe3ce239267c1bc` | `gogoproto/gogo.proto` | `src/main/proto/gogoproto/gogo.proto` | BSD-3-Clause | `f2f77edf7de807ded7884813d851656f4ccb18262db717a8f31061995f3e7324` |
| googleapis | `34bb7a118468a6ec56deb31842525971e41be2ac` | `google/api/annotations.proto` | `src/main/proto/google/api/annotations.proto` | Apache-2.0 | `e79ea741cb605a65e78ca322174764a4af9fde1962c1631e12b84c4934ba9a6c` |
| googleapis | `34bb7a118468a6ec56deb31842525971e41be2ac` | `google/api/http.proto` | `src/main/proto/google/api/http.proto` | Apache-2.0 | `ead99129aa15dd5f6233942030c72eec33bc0d7b1c7c260dc143293ef66c5b78` |

The etcd proto files import the gogo and Google API option definitions. The
lock explicitly records `google/protobuf/descriptor.proto` as a well-known
descriptor supplied by the locked protoc version. Java and
gRPC bindings use protoc/protobuf-java 4.36.2 and grpc-java 1.84.1; the Maven
plugin versions are also pinned in `upstream-lock.json` and the repository
dependency lock.

Verify the local import closure and byte hashes with:

```sh
python3 scripts/verify/vendor-proto.py \
  --lock protocol/upstream-lock.json \
  --root protocol/src/main/proto
```

Generated Java and descriptor files are build outputs. Do not edit generated
files or vendor changes from a floating branch. Updating the protocol requires
reviewing source provenance, the full descriptor contract, binary round trips,
and client compatibility together. See [`../docs/protocol/m0-wire-baseline.md`](../docs/protocol/m0-wire-baseline.md)
for the generated contract and validation boundary.
