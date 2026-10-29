package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.CallOptions;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ExternalProtocolIT {
  private static final MethodDescriptor.Marshaller<byte[]> BYTES =
      new MethodDescriptor.Marshaller<>() {
        public InputStream stream(byte[] value) {
          return new ByteArrayInputStream(value);
        }

        public byte[] parse(InputStream input) {
          try {
            return input.readAllBytes();
          } catch (IOException failure) {
            throw new UncheckedIOException(failure);
          }
        }
      };

  @Test
  void rejectsEveryExternalLockAndElectionMethodWithoutAResponse() throws Exception {
    try (Bootstrap server =
        Bootstrap.start(new Bootstrap.ListenerBinding("127.0.0.1", 0, "j07-external"))) {
      var channel =
          ManagedChannelBuilder.forAddress("127.0.0.1", server.boundListener().port())
              .usePlaintext()
              .build();
      try {
        for (String method :
            List.of(
                "v3lockpb.Lock/Lock",
                "v3lockpb.Lock/Unlock",
                "v3electionpb.Election/Campaign",
                "v3electionpb.Election/Proclaim",
                "v3electionpb.Election/Leader",
                "v3electionpb.Election/Resign")) {
          var descriptor = method(method, MethodDescriptor.MethodType.UNARY);
          var failure =
              assertThrows(
                  StatusRuntimeException.class,
                  () ->
                      ClientCalls.blockingUnaryCall(
                          channel,
                          descriptor,
                          CallOptions.DEFAULT.withDeadlineAfter(2, TimeUnit.SECONDS),
                          new byte[0]));
          assertEquals(Status.Code.UNIMPLEMENTED, failure.getStatus().getCode(), method);
        }
        var descriptor =
            method("v3electionpb.Election/Observe", MethodDescriptor.MethodType.SERVER_STREAMING);
        var failure =
            assertThrows(
                StatusRuntimeException.class,
                () ->
                    ClientCalls.blockingServerStreamingCall(
                            channel,
                            descriptor,
                            CallOptions.DEFAULT.withDeadlineAfter(2, TimeUnit.SECONDS),
                            new byte[0])
                        .hasNext());
        assertEquals(Status.Code.UNIMPLEMENTED, failure.getStatus().getCode());
      } finally {
        channel.shutdownNow();
        assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS));
      }
    }
  }

  private static MethodDescriptor<byte[], byte[]> method(
      String path, MethodDescriptor.MethodType type) {
    return MethodDescriptor.<byte[], byte[]>newBuilder()
        .setType(type)
        .setFullMethodName(path)
        .setRequestMarshaller(BYTES)
        .setResponseMarshaller(BYTES)
        .build();
  }
}
