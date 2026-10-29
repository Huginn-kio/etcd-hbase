package io.etcdhbase.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class WireTraceAssertions {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final long EXPECTED_REVISION = 1L << 54;

  private WireTraceAssertions() {}

  static void assertWireTrace(Path tracePath) throws IOException {
    String serializedTrace = readTrace(tracePath);
    Map<String, JsonNode> byCase = indexByCase(parseRecords(serializedTrace));
    assertCaseOutcomes(byCase);
    assertRangeRequests(byCase);
    assertSkeletonWatch(byCase.get("wire-skeleton-watch"));
    assertLeaseKeepAlive(byCase.get("wire-skeleton-lease-keepalive"));
    assertWatchCancellation(byCase.get("wire-fixture-watch-cancel"));
    assertTracePrivacy(serializedTrace);
  }

  private static String readTrace(Path tracePath) throws IOException {
    try (var lines = Files.lines(tracePath)) {
      return String.join("\n", lines.toList());
    }
  }

  private static List<JsonNode> parseRecords(String serializedTrace) throws IOException {
    List<JsonNode> records = new ArrayList<>();
    for (String line : serializedTrace.lines().toList()) {
      if (!line.isBlank()) {
        records.add(JSON.readTree(line));
      }
    }
    return records;
  }

  private static Map<String, JsonNode> indexByCase(List<JsonNode> records) {
    Map<String, JsonNode> byCase = new HashMap<>();
    for (JsonNode record : records) {
      byCase.put(record.path("case_id").asText(), record);
    }
    assertEquals(records.size(), byCase.size(), "duplicate wire cases");
    assertEquals(10, records.size(), "wire probe omitted an expected request/stream case");
    return byCase;
  }

  private static void assertCaseOutcomes(Map<String, JsonNode> byCase) {
    assertRecord(byCase, "wire-skeleton-clientv3", "Unimplemented", "transport");
    assertRecord(byCase, "wire-skeleton-raw-range", "Unimplemented", "transport");
    assertRecord(byCase, "wire-skeleton-watch", "Unimplemented", "transport");
    assertRecord(byCase, "wire-skeleton-lease-keepalive", "Unimplemented", "transport");
    assertRecord(byCase, "wire-fixture-watch-cancel", "DeadlineExceeded", "transport-fixture");
    assertRecord(byCase, "wire-fixture-clientv3-auth", "InvalidArgument", "transport-fixture");
    assertRecord(byCase, "wire-fixture-raw-permission", "PermissionDenied", "transport-fixture");
    assertRecord(byCase, "wire-fixture-raw-unavailable", "Unavailable", "transport-fixture");
    assertRecord(
        byCase, "wire-fixture-clientv3-retry-deadline", "DeadlineExceeded", "transport-fixture");
    assertRecord(
        byCase, "wire-fixture-raw-deadline-cancel", "DeadlineExceeded", "transport-fixture");
  }

  private static void assertRangeRequests(Map<String, JsonNode> byCase) {
    for (String caseId :
        List.of(
            "wire-skeleton-clientv3",
            "wire-skeleton-raw-range",
            "wire-fixture-clientv3-auth",
            "wire-fixture-raw-permission",
            "wire-fixture-raw-unavailable",
            "wire-fixture-clientv3-retry-deadline",
            "wire-fixture-raw-deadline-cancel")) {
      JsonNode fields = byCase.get(caseId).path("request").path("fields");
      assertEquals("18014398509481984", fields.path("revision").asText(), caseId);
      assertTrue(fields.path("key").isTextual(), caseId + " did not preserve the byte key");
      assertEquals("etcdserverpb.KV/Range", byCase.get(caseId).path("method").asText(), caseId);
    }
  }

  private static void assertSkeletonWatch(JsonNode watch) {
    JsonNode watchRequest =
        watch.path("request").path("fields").path("createRequest").path("fields");
    assertEquals("etcdserverpb.Watch/Watch", watch.path("method").asText());
    assertEquals("AAD/", watchRequest.path("key").asText());
    assertEquals(Long.toString(EXPECTED_REVISION), watchRequest.path("startRevision").asText());
    assertEquals("grpc-status", watch.path("stream_terminal").path("kind").asText());
    assertEquals("Unimplemented", watch.path("stream_terminal").path("grpc_code").asText());
  }

  private static void assertLeaseKeepAlive(JsonNode keepAlive) {
    assertEquals("etcdserverpb.Lease/LeaseKeepAlive", keepAlive.path("method").asText());
    assertEquals(
        Long.toString(EXPECTED_REVISION),
        keepAlive.path("request").path("fields").path("ID").asText());
    assertEquals("grpc-status", keepAlive.path("stream_terminal").path("kind").asText());
    assertEquals("Unimplemented", keepAlive.path("stream_terminal").path("grpc_code").asText());
  }

  private static void assertWatchCancellation(JsonNode cancelledWatch) {
    JsonNode cancelledWatchRequest =
        cancelledWatch.path("request").path("fields").path("createRequest").path("fields");
    assertEquals("etcdserverpb.Watch/Watch", cancelledWatch.path("method").asText());
    assertEquals("AAD/", cancelledWatchRequest.path("key").asText());
    assertEquals(
        Long.toString(EXPECTED_REVISION), cancelledWatchRequest.path("startRevision").asText());
    assertEquals("context-deadline", cancelledWatch.path("stream_terminal").path("kind").asText());
  }

  private static void assertTracePrivacy(String serializedTrace) {
    assertFalse(
        serializedTrace.contains("password")
            || serializedTrace.contains("token")
            || serializedTrace.contains("user-secret"),
        "wire trace contains credential-like content");
  }

  private static void assertRecord(
      Map<String, JsonNode> records, String caseId, String grpcCode, String scope) {
    JsonNode record = records.get(caseId);
    assertTrue(record != null, "missing trace case " + caseId);
    assertEquals(grpcCode, record.path("error").path("grpc_code").asText(), caseId);
    assertEquals(scope, record.path("evidence_scope").asText(), caseId);
  }
}
