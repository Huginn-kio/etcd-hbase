package io.etcdhbase.grpc.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import etcdserverpb.Rpc;
import io.etcdhbase.core.kv.KvFailure;
import io.etcdhbase.core.kv.PutCommand;
import org.junit.jupiter.api.Test;

class PutRequestMapperTest {
  @Test
  void keepsBinaryBytesAndFullSignedLeaseId() {
    Rpc.PutRequest request = Rpc.PutRequest.newBuilder()
        .setKey(ByteString.copyFrom(new byte[] {0, (byte) 0xff}))
        .setValue(ByteString.copyFrom(new byte[] {(byte) 0x80, 0}))
        .setLease(Long.MIN_VALUE).setPrevKv(true).build();

    PutCommand command = PutRequestMapper.fromWire(request);
    assertArrayEquals(new byte[] {0, (byte) 0xff}, command.key().bytes());
    assertArrayEquals(new byte[] {(byte) 0x80, 0}, command.value());
    assertEquals(Long.MIN_VALUE, command.leaseId());
    assertEquals(PutCommand.LeaseIntent.EXPLICIT, command.leaseIntent());
    assertTrue(command.previousRequested());
  }

  @Test
  void keepsMissingKeyDistinctFromSingleZeroByteAndPreservesFlags() {
    assertEquals(KvFailure.Reason.EMPTY_KEY,
        assertThrows(KvFailure.class,
            () -> PutRequestMapper.fromWire(Rpc.PutRequest.getDefaultInstance())).reason());
    PutCommand command = PutRequestMapper.fromWire(Rpc.PutRequest.newBuilder()
        .setKey(ByteString.copyFrom(new byte[] {0})).setIgnoreValue(true)
        .setIgnoreLease(true).build());
    assertTrue(command.ignoreValue());
    assertEquals(PutCommand.LeaseIntent.PRESERVE, command.leaseIntent());
  }
}
