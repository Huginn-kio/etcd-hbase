package io.etcdhbase.core.lease;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.etcdhbase.core.bytes.ByteKey;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class LeaseExpiryStepCommandTest {
  @Test
  void roundTripsInitialAndCurrentCheckpointRequests() {
    var initial = new LeaseExpiryStepCommand(0);
    var current = new LeaseExpiryStepCommand(Long.MAX_VALUE);

    assertEquals(initial, LeaseExpiryStepCommand.decode(initial.encode()));
    assertEquals(current, LeaseExpiryStepCommand.decode(current.encode()));
  }

  @Test
  void rejectsNegativeSequenceAndMalformedEncoding() {
    assertThrows(IllegalArgumentException.class, () -> new LeaseExpiryStepCommand(-1));

    var command = new LeaseExpiryStepCommand(11);
    byte[] negativeSequence = command.encode().bytes();
    ByteBuffer.wrap(negativeSequence).putLong(4, -1);
    assertThrows(IllegalArgumentException.class,
        () -> LeaseExpiryStepCommand.decode(ByteKey.copyOf(negativeSequence)));

    byte[] truncated = new byte[command.encode().length() - 1];
    System.arraycopy(command.encode().bytes(), 0, truncated, 0, truncated.length);
    assertThrows(IllegalArgumentException.class,
        () -> LeaseExpiryStepCommand.decode(ByteKey.copyOf(truncated)));
  }
}
