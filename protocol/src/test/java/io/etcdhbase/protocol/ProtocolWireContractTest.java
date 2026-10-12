package io.etcdhbase.protocol;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import etcdserverpb.Rpc;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import mvccpb.Kv;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtocolWireContractTest {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String SOURCE_REVISION = "1cf4bc40111fd95d1e14a90a09a146357b15eb13";
  private static final Map<String, String> SOURCE_SHA256 = Map.of(
      "etcd/api/etcdserverpb/rpc.proto", "7cf0d098aae57522a574bf591145e55539a15212885d43709536553833cd6f40",
      "etcd/api/mvccpb/kv.proto", "76b0d596ecc8dd1b535be683777910a5b4fe0705fb57baccacfcc7d98d11821e",
      "etcd/api/authpb/auth.proto", "443e661f9e423d3376d513e76446e957ae0132c6f63fff4510fde3c7a587be05");
  private static final List<String> CONTRACT_FILES = List.of(
      "etcd/api/etcdserverpb/rpc.proto",
      "etcd/api/mvccpb/kv.proto",
      "etcd/api/authpb/auth.proto");
  private static final Set<String> DESCRIPTOR_IMPORTS = Set.of(
      "etcd/api/etcdserverpb/rpc.proto",
      "etcd/api/mvccpb/kv.proto",
      "etcd/api/authpb/auth.proto",
      "gogoproto/gogo.proto",
      "google/api/annotations.proto",
      "google/api/http.proto");

  @Test
  void generatedDescriptorMatchesCheckedInWireContract() throws IOException {
    JsonNode actual = buildContract();
    Path contractFile = Path.of(System.getProperty(
        "wire.contract.file", "baseline/wire-contract.json"));
    if (Boolean.getBoolean("m0.wire-contract.update")) {
      Files.createDirectories(contractFile.getParent());
      JSON.writerWithDefaultPrettyPrinter().writeValue(contractFile.toFile(), actual);
      return;
    }

    assertTrue(Files.isRegularFile(contractFile), "missing protocol/baseline/wire-contract.json");
    JsonNode expected = JSON.readTree(contractFile.toFile());
    assertEquals(expected, actual,
        "generated Java descriptors differ from the locked etcd wire contract");
  }

  @Test
  void generatedDescriptorSetContainsProtocolAndTransitiveImports() throws IOException {
    Path descriptorFile = Path.of(System.getProperty(
        "protocol.descriptor.file",
        "target/generated-resources/protobuf/descriptor-sets/etcd-v3.5.23.protobin"));
    assertTrue(Files.isRegularFile(descriptorFile), "missing generated descriptor set");

    FileDescriptorSet descriptorSet = FileDescriptorSet.parseFrom(Files.readAllBytes(descriptorFile));
    Set<String> actual = new HashSet<>();
    descriptorSet.getFileList().forEach(file -> actual.add(file.getName()));
    assertEquals(descriptorSet.getFileCount(), actual.size(), "duplicate descriptor file names");
    assertTrue(actual.containsAll(DESCRIPTOR_IMPORTS),
        () -> "descriptor set is missing files: " + difference(DESCRIPTOR_IMPORTS, actual));
  }

  private static JsonNode buildContract() {
    Map<String, FileDescriptor> descriptors = new HashMap<>();
    collectFiles(Rpc.getDescriptor(), descriptors);
    collectFiles(Kv.KeyValue.getDescriptor().getFile(), descriptors);

    ObjectNode root = JSON.createObjectNode();
    root.put("schema_version", 1);
    root.put("protocol", "etcd-v3.5.23");
    root.put("source_revision", SOURCE_REVISION);
    ArrayNode files = root.putArray("files");
    for (String name : CONTRACT_FILES) {
      FileDescriptor descriptor = descriptors.get(name);
      if (descriptor == null) {
        throw new IllegalStateException("generated descriptors do not include " + name);
      }
      ObjectNode file = files.addObject();
      file.put("name", descriptor.getName());
      file.put("package", descriptor.getPackage());
      file.put("source_sha256", SOURCE_SHA256.get(name));
      appendServices(file.putArray("services"), descriptor);
      appendMessages(file.putArray("messages"), descriptor);
      appendEnums(file.putArray("enums"), descriptor);
    }
    return root;
  }

  private static void collectFiles(FileDescriptor file, Map<String, FileDescriptor> files) {
    if (files.putIfAbsent(file.getName(), file) != null) {
      return;
    }
    file.getDependencies().forEach(dependency -> collectFiles(dependency, files));
  }

  private static void appendServices(ArrayNode target, FileDescriptor file) {
    file.getServices().stream()
        .sorted(Comparator.comparing(service -> service.getFullName()))
        .forEach(service -> {
          ObjectNode serviceNode = target.addObject();
          serviceNode.put("name", service.getName());
          serviceNode.put("full_name", service.getFullName());
          ArrayNode methods = serviceNode.putArray("methods");
          service.getMethods().stream()
              .sorted(Comparator.comparing(MethodDescriptor::getFullName))
              .forEach(method -> {
                ObjectNode methodNode = methods.addObject();
                methodNode.put("name", method.getName());
                methodNode.put("full_name", method.getFullName());
                methodNode.put("input", method.getInputType().getFullName());
                methodNode.put("output", method.getOutputType().getFullName());
                methodNode.put("client_streaming", method.isClientStreaming());
                methodNode.put("server_streaming", method.isServerStreaming());
              });
        });
  }

  private static void appendMessages(ArrayNode target, FileDescriptor file) {
    List<Descriptor> messages = new ArrayList<>();
    file.getMessageTypes().forEach(message -> collectMessages(message, messages));
    messages.stream().sorted(Comparator.comparing(Descriptor::getFullName)).forEach(message -> {
      ObjectNode messageNode = target.addObject();
      messageNode.put("name", message.getFullName());
      ArrayNode fields = messageNode.putArray("fields");
      message.getFields().stream()
          .sorted(Comparator.comparingInt(FieldDescriptor::getNumber))
          .forEach(field -> appendField(fields.addObject(), field));

      ArrayNode oneofs = messageNode.putArray("oneofs");
      message.getOneofs().stream()
          .sorted(Comparator.comparing(OneofDescriptor::getName))
          .forEach(oneof -> {
            ObjectNode oneofNode = oneofs.addObject();
            oneofNode.put("name", oneof.getName());
            ArrayNode oneofFields = oneofNode.putArray("fields");
            oneof.getFields().stream().map(FieldDescriptor::getName).sorted()
                .forEach(oneofFields::add);
          });
    });
  }

  private static void appendField(ObjectNode target, FieldDescriptor field) {
    target.put("name", field.getName());
    target.put("number", field.getNumber());
    target.put("type", field.getType().name().toLowerCase(Locale.ROOT));
    target.put("label", field.isRepeated() ? "repeated" : field.isRequired() ? "required" : "optional");
    if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
      target.put("type_name", field.getMessageType().getFullName());
    } else if (field.getJavaType() == FieldDescriptor.JavaType.ENUM) {
      target.put("type_name", field.getEnumType().getFullName());
    } else {
      target.putNull("type_name");
    }
    if (field.getContainingOneof() == null) {
      target.putNull("oneof");
    } else {
      target.put("oneof", field.getContainingOneof().getName());
    }
    target.put("proto3_optional", field.toProto().getProto3Optional());
  }

  private static void appendEnums(ArrayNode target, FileDescriptor file) {
    List<EnumDescriptor> enums = new ArrayList<>(file.getEnumTypes());
    List<Descriptor> messages = new ArrayList<>();
    file.getMessageTypes().forEach(message -> collectMessages(message, messages));
    messages.forEach(message -> enums.addAll(message.getEnumTypes()));

    enums.stream().sorted(Comparator.comparing(EnumDescriptor::getFullName)).forEach(enumType -> {
      ObjectNode enumNode = target.addObject();
      enumNode.put("name", enumType.getFullName());
      ArrayNode values = enumNode.putArray("values");
      enumType.getValues().stream()
          .sorted(Comparator.comparingInt(EnumValueDescriptor::getNumber)
              .thenComparing(EnumValueDescriptor::getName))
          .forEach(value -> {
            ObjectNode valueNode = values.addObject();
            valueNode.put("name", value.getName());
            valueNode.put("number", value.getNumber());
          });
    });
  }

  private static void collectMessages(Descriptor message, List<Descriptor> target) {
    target.add(message);
    message.getNestedTypes().forEach(nested -> collectMessages(nested, target));
  }

  private static Set<String> difference(Set<String> expected, Set<String> actual) {
    Set<String> missing = new TreeSet<>(expected);
    missing.removeAll(actual);
    return missing;
  }
}
