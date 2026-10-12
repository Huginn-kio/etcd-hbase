package io.etcdhbase.protocol;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import etcdserverpb.Rpc;
import java.util.Set;
import java.util.stream.Collectors;
import mvccpb.Kv;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EtcdProtocolDescriptorTest {
  private static final Set<String> METHOD_SIGNATURES = Set.of(
      "KV.Range:RangeRequest->RangeResponse:unary",
      "KV.Put:PutRequest->PutResponse:unary",
      "KV.DeleteRange:DeleteRangeRequest->DeleteRangeResponse:unary",
      "KV.Txn:TxnRequest->TxnResponse:unary",
      "KV.Compact:CompactionRequest->CompactionResponse:unary",
      "Watch.Watch:WatchRequest->WatchResponse:bidi",
      "Lease.LeaseGrant:LeaseGrantRequest->LeaseGrantResponse:unary",
      "Lease.LeaseRevoke:LeaseRevokeRequest->LeaseRevokeResponse:unary",
      "Lease.LeaseKeepAlive:LeaseKeepAliveRequest->LeaseKeepAliveResponse:bidi",
      "Lease.LeaseTimeToLive:LeaseTimeToLiveRequest->LeaseTimeToLiveResponse:unary",
      "Lease.LeaseLeases:LeaseLeasesRequest->LeaseLeasesResponse:unary",
      "Cluster.MemberAdd:MemberAddRequest->MemberAddResponse:unary",
      "Cluster.MemberRemove:MemberRemoveRequest->MemberRemoveResponse:unary",
      "Cluster.MemberUpdate:MemberUpdateRequest->MemberUpdateResponse:unary",
      "Cluster.MemberList:MemberListRequest->MemberListResponse:unary",
      "Cluster.MemberPromote:MemberPromoteRequest->MemberPromoteResponse:unary",
      "Maintenance.Alarm:AlarmRequest->AlarmResponse:unary",
      "Maintenance.Status:StatusRequest->StatusResponse:unary",
      "Maintenance.Defragment:DefragmentRequest->DefragmentResponse:unary",
      "Maintenance.Hash:HashRequest->HashResponse:unary",
      "Maintenance.HashKV:HashKVRequest->HashKVResponse:unary",
      "Maintenance.Snapshot:SnapshotRequest->SnapshotResponse:server",
      "Maintenance.MoveLeader:MoveLeaderRequest->MoveLeaderResponse:unary",
      "Maintenance.Downgrade:DowngradeRequest->DowngradeResponse:unary",
      "Auth.AuthEnable:AuthEnableRequest->AuthEnableResponse:unary",
      "Auth.AuthDisable:AuthDisableRequest->AuthDisableResponse:unary",
      "Auth.AuthStatus:AuthStatusRequest->AuthStatusResponse:unary",
      "Auth.Authenticate:AuthenticateRequest->AuthenticateResponse:unary",
      "Auth.UserAdd:AuthUserAddRequest->AuthUserAddResponse:unary",
      "Auth.UserGet:AuthUserGetRequest->AuthUserGetResponse:unary",
      "Auth.UserList:AuthUserListRequest->AuthUserListResponse:unary",
      "Auth.UserDelete:AuthUserDeleteRequest->AuthUserDeleteResponse:unary",
      "Auth.UserChangePassword:AuthUserChangePasswordRequest->AuthUserChangePasswordResponse:unary",
      "Auth.UserGrantRole:AuthUserGrantRoleRequest->AuthUserGrantRoleResponse:unary",
      "Auth.UserRevokeRole:AuthUserRevokeRoleRequest->AuthUserRevokeRoleResponse:unary",
      "Auth.RoleAdd:AuthRoleAddRequest->AuthRoleAddResponse:unary",
      "Auth.RoleGet:AuthRoleGetRequest->AuthRoleGetResponse:unary",
      "Auth.RoleList:AuthRoleListRequest->AuthRoleListResponse:unary",
      "Auth.RoleDelete:AuthRoleDeleteRequest->AuthRoleDeleteResponse:unary",
      "Auth.RoleGrantPermission:AuthRoleGrantPermissionRequest->AuthRoleGrantPermissionResponse:unary",
      "Auth.RoleRevokePermission:AuthRoleRevokePermissionRequest->AuthRoleRevokePermissionResponse:unary");

  @Test
  void preservesEtcdServiceAndMethodDescriptors() {
    var descriptor = Rpc.getDescriptor();
    assertEquals("etcdserverpb", descriptor.getPackage());
    assertEquals(6, descriptor.getServices().size());

    Set<String> actual = descriptor.getServices().stream()
        .flatMap(service -> service.getMethods().stream())
        .map(EtcdProtocolDescriptorTest::methodSignature)
        .collect(Collectors.toSet());
    assertEquals(METHOD_SIGNATURES, actual);
  }

  @Test
  void preservesStreamingDirections() {
    assertTrue(method("Watch", "Watch").isClientStreaming());
    assertTrue(method("Watch", "Watch").isServerStreaming());
    assertTrue(method("Lease", "LeaseKeepAlive").isClientStreaming());
    assertTrue(method("Lease", "LeaseKeepAlive").isServerStreaming());
    assertFalse(method("Maintenance", "Snapshot").isClientStreaming());
    assertTrue(method("Maintenance", "Snapshot").isServerStreaming());
  }

  @Test
  void preservesEtcdWireFieldNumbersAndTypes() {
    var keyValue = Kv.KeyValue.getDescriptor();
    assertField(keyValue, "key", 1, FieldDescriptor.Type.BYTES);
    assertField(keyValue, "create_revision", 2, FieldDescriptor.Type.INT64);
    assertField(keyValue, "mod_revision", 3, FieldDescriptor.Type.INT64);
    assertField(keyValue, "version", 4, FieldDescriptor.Type.INT64);
    assertField(keyValue, "value", 5, FieldDescriptor.Type.BYTES);
    assertField(keyValue, "lease", 6, FieldDescriptor.Type.INT64);

    var rangeRequest = Rpc.RangeRequest.getDescriptor();
    assertField(rangeRequest, "key", 1, FieldDescriptor.Type.BYTES);
    assertField(rangeRequest, "range_end", 2, FieldDescriptor.Type.BYTES);
    assertField(rangeRequest, "limit", 3, FieldDescriptor.Type.INT64);

    var eventType = Kv.Event.getDescriptor().findFieldByName("type").getEnumType();
    EnumValueDescriptor put = eventType.findValueByName("PUT");
    EnumValueDescriptor delete = eventType.findValueByName("DELETE");
    assertEquals(0, put.getNumber());
    assertEquals(1, delete.getNumber());
  }

  @Test
  void generatedKeyValueRoundTripsArbitraryBytesAndSignedInt64() throws Exception {
    var value = Kv.KeyValue.newBuilder()
        .setKey(ByteString.copyFrom(new byte[] {0, 1, -1, 42}))
        .setValue(ByteString.copyFrom(new byte[] {-128, 0, 127}))
        .setCreateRevision(Long.MAX_VALUE)
        .setModRevision(Long.MIN_VALUE + 1)
        .setVersion(7)
        .setLease(-9)
        .build();

    assertEquals(value, Kv.KeyValue.parseFrom(value.toByteArray()));
  }

  private static String methodSignature(MethodDescriptor method) {
    String mode = method.isClientStreaming()
        ? (method.isServerStreaming() ? "bidi" : "client")
        : (method.isServerStreaming() ? "server" : "unary");
    return method.getService().getName() + "." + method.getName()
        + ":" + method.getInputType().getName() + "->" + method.getOutputType().getName()
        + ":" + mode;
  }

  private static MethodDescriptor method(String serviceName, String methodName) {
    return Rpc.getDescriptor().findServiceByName(serviceName).findMethodByName(methodName);
  }

  private static void assertField(
      com.google.protobuf.Descriptors.Descriptor descriptor,
      String name,
      int number,
      FieldDescriptor.Type type) {
    FieldDescriptor field = descriptor.findFieldByName(name);
    assertEquals(number, field.getNumber(), descriptor.getFullName() + "." + name);
    assertEquals(type, field.getType(), descriptor.getFullName() + "." + name);
  }
}
