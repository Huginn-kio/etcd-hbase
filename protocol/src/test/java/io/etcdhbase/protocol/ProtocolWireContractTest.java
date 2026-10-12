package io.etcdhbase.protocol;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.MethodDescriptorProto;
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto;
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
import mvccpb.Kv;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtocolWireContractTest {
  private static final ObjectMapper JSON = new ObjectMapper();
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
      "google/api/http.proto",
      "google/protobuf/descriptor.proto");

  @Test
  void generatedDescriptorMatchesCheckedInWireContract() throws IOException {
    JsonNode actual = buildProtocContract();
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
        "upstream protoc descriptors differ from the locked etcd wire contract");
  }

  @Test
  void generatedDescriptorSetContainsProtocolAndTransitiveImports() throws IOException {
    FileDescriptorSet descriptorSet = readDescriptorSet();
    Set<String> actual = new HashSet<>();
    descriptorSet.getFileList().forEach(file -> actual.add(file.getName()));
    assertEquals(descriptorSet.getFileCount(), actual.size(), "duplicate descriptor file names");
    assertEquals(DESCRIPTOR_IMPORTS, actual,
        "descriptor set differs from the locked proto import closure");
  }

  @Test
  void protocDescriptorSetMatchesCompiledJavaFileDescriptors() throws IOException {
    assertEquals(buildProtocContract(), buildJavaContract(),
        "protoc and generated Java differ in the etcd wire contract");
  }

  private static FileDescriptorSet readDescriptorSet() throws IOException {
    Path descriptorFile = Path.of(System.getProperty(
        "protocol.descriptor.file",
        "target/generated-resources/protobuf/descriptor-sets/etcd-v3.5.23.protobin"));
    assertTrue(Files.isRegularFile(descriptorFile), "missing generated descriptor set");
    return FileDescriptorSet.parseFrom(Files.readAllBytes(descriptorFile));
  }

  private static JsonNode buildJavaContract() throws IOException {
    Map<String, FileDescriptor> descriptors = new HashMap<>();
    collectFiles(Rpc.getDescriptor(), descriptors);
    collectFiles(Kv.KeyValue.getDescriptor().getFile(), descriptors);
    SourceLock sourceLock = readSourceLock();
    ObjectNode root = contractRoot(sourceLock);
    ArrayNode files = root.putArray("files");
    for (String name : CONTRACT_FILES) {
      FileDescriptor descriptor = descriptors.get(name);
      if (descriptor == null) {
        throw new IllegalStateException("generated descriptors do not include " + name);
      }
      ObjectNode file = fileHeader(files, sourceLock, descriptor.getName(), descriptor.getPackage());
      appendServices(file.putArray("services"), descriptor);
      appendMessages(file.putArray("messages"), descriptor);
      appendEnums(file.putArray("enums"), descriptor);
    }
    return root;
  }

  private static JsonNode buildProtocContract() throws IOException {
    Map<String, FileDescriptorProto> descriptors = new HashMap<>();
    readDescriptorSet().getFileList().forEach(file -> descriptors.put(file.getName(), file));
    SourceLock sourceLock = readSourceLock();
    ObjectNode root = contractRoot(sourceLock);
    ArrayNode files = root.putArray("files");
    for (String name : CONTRACT_FILES) {
      FileDescriptorProto descriptor = descriptors.get(name);
      if (descriptor == null) {
        throw new IllegalStateException("protoc descriptor set does not include " + name);
      }
      ObjectNode file = fileHeader(files, sourceLock, descriptor.getName(), descriptor.getPackage());
      appendProtoServices(file.putArray("services"), descriptor);
      appendProtoMessages(file.putArray("messages"), descriptor);
      appendProtoEnums(file.putArray("enums"), descriptor);
    }
    return root;
  }

  private static SourceLock readSourceLock() throws IOException {
    Path sourceLockPath = Path.of(System.getProperty(
        "protocol.source.lock", "upstream-lock.json"));
    JsonNode sourceLock = JSON.readTree(sourceLockPath.toFile());
    String protocol = sourceLock.path("protocol").asText();
    String revision = null;
    Map<String, String> hashes = new HashMap<>();
    for (JsonNode source : sourceLock.path("sources")) {
      if (!"etcd".equals(source.path("name").asText())) {
        continue;
      }
      revision = source.path("revision").asText(null);
      for (JsonNode file : source.path("files")) {
        hashes.put(file.path("path").asText(), file.path("sha256").asText());
      }
    }
    if (protocol.isBlank() || revision == null) {
      throw new IllegalStateException("upstream lock is missing the etcd protocol revision");
    }
    return new SourceLock(protocol, revision, hashes);
  }

  private static ObjectNode contractRoot(SourceLock sourceLock) {
    ObjectNode root = JSON.createObjectNode();
    root.put("schema_version", 1);
    root.put("protocol", sourceLock.protocol());
    root.put("source_revision", sourceLock.revision());
    return root;
  }

  private static ObjectNode fileHeader(
      ArrayNode files, SourceLock sourceLock, String name, String packageName) {
    ObjectNode file = files.addObject();
    file.put("name", name);
    file.put("package", packageName);
    String sourceHash = sourceLock.hashes().get(name);
    if (sourceHash == null || sourceHash.isBlank()) {
      throw new IllegalStateException("upstream lock is missing SHA-256 for " + name);
    }
    file.put("source_sha256", sourceHash);
    return file;
  }

  private static void appendProtoServices(ArrayNode target, FileDescriptorProto file) {
    file.getServiceList().stream()
        .sorted(Comparator.comparing(service -> fullName(file.getPackage(), service.getName())))
        .forEach(service -> {
          String serviceFullName = fullName(file.getPackage(), service.getName());
          ObjectNode serviceNode = target.addObject();
          serviceNode.put("name", service.getName());
          serviceNode.put("full_name", serviceFullName);
          ArrayNode methods = serviceNode.putArray("methods");
          service.getMethodList().stream()
              .sorted(Comparator.comparing(method -> fullName(serviceFullName, method.getName())))
              .forEach(method -> appendProtoMethod(methods.addObject(), method, serviceFullName));
        });
  }

  private static void appendProtoMethod(
      ObjectNode target, MethodDescriptorProto method, String serviceFullName) {
    target.put("name", method.getName());
    target.put("full_name", fullName(serviceFullName, method.getName()));
    target.put("input", stripLeadingDot(method.getInputType()));
    target.put("output", stripLeadingDot(method.getOutputType()));
    target.put("client_streaming", method.getClientStreaming());
    target.put("server_streaming", method.getServerStreaming());
  }

  private static void appendProtoMessages(ArrayNode target, FileDescriptorProto file) {
    List<NamedProtoMessage> messages = new ArrayList<>();
    file.getMessageTypeList().forEach(message ->
        collectProtoMessages(message, file.getPackage(), messages));
    messages.stream().sorted(Comparator.comparing(NamedProtoMessage::fullName)).forEach(named -> {
      DescriptorProto message = named.descriptor();
      ObjectNode messageNode = target.addObject();
      messageNode.put("name", named.fullName());
      ArrayNode fields = messageNode.putArray("fields");
      message.getFieldList().stream()
          .sorted(Comparator.comparingInt(FieldDescriptorProto::getNumber))
          .forEach(field -> appendProtoField(fields.addObject(), field, message));

      ArrayNode oneofs = messageNode.putArray("oneofs");
      message.getOneofDeclList().stream()
          .sorted(Comparator.comparing(oneof -> oneof.getName()))
          .forEach(oneof -> {
            int oneofIndex = message.getOneofDeclList().indexOf(oneof);
            ObjectNode oneofNode = oneofs.addObject();
            oneofNode.put("name", oneof.getName());
            ArrayNode oneofFields = oneofNode.putArray("fields");
            message.getFieldList().stream()
                .filter(field -> field.hasOneofIndex() && field.getOneofIndex() == oneofIndex)
                .map(FieldDescriptorProto::getName)
                .sorted()
                .forEach(oneofFields::add);
          });
    });
  }

  private static void appendProtoField(
      ObjectNode target, FieldDescriptorProto field, DescriptorProto containingMessage) {
    target.put("name", field.getName());
    target.put("number", field.getNumber());
    target.put("type", field.getType().name().substring("TYPE_".length()).toLowerCase(Locale.ROOT));
    target.put("label", field.getLabel().name().substring("LABEL_".length()).toLowerCase(Locale.ROOT));
    if (field.hasTypeName()) {
      target.put("type_name", stripLeadingDot(field.getTypeName()));
    } else {
      target.putNull("type_name");
    }
    if (field.hasOneofIndex()) {
      target.put("oneof", containingMessage.getOneofDecl(field.getOneofIndex()).getName());
    } else {
      target.putNull("oneof");
    }
    target.put("proto3_optional", field.getProto3Optional());
  }

  private static void collectProtoMessages(
      DescriptorProto message, String parentName, List<NamedProtoMessage> target) {
    String messageName = fullName(parentName, message.getName());
    target.add(new NamedProtoMessage(messageName, message));
    message.getNestedTypeList().forEach(nested -> collectProtoMessages(nested, messageName, target));
  }

  private static void appendProtoEnums(ArrayNode target, FileDescriptorProto file) {
    List<NamedProtoEnum> enums = new ArrayList<>();
    file.getEnumTypeList().forEach(enumType ->
        enums.add(new NamedProtoEnum(fullName(file.getPackage(), enumType.getName()), enumType)));
    List<NamedProtoMessage> messages = new ArrayList<>();
    file.getMessageTypeList().forEach(message ->
        collectProtoMessages(message, file.getPackage(), messages));
    messages.forEach(message -> message.descriptor().getEnumTypeList().forEach(enumType ->
        enums.add(new NamedProtoEnum(fullName(message.fullName(), enumType.getName()), enumType))));

    enums.stream().sorted(Comparator.comparing(NamedProtoEnum::fullName)).forEach(named -> {
      ObjectNode enumNode = target.addObject();
      enumNode.put("name", named.fullName());
      ArrayNode values = enumNode.putArray("values");
      named.descriptor().getValueList().stream()
          .sorted(Comparator.comparingInt(EnumValueDescriptorProto::getNumber)
              .thenComparing(EnumValueDescriptorProto::getName))
          .forEach(value -> {
            ObjectNode valueNode = values.addObject();
            valueNode.put("name", value.getName());
            valueNode.put("number", value.getNumber());
          });
    });
  }

  private static String fullName(String parent, String name) {
    return parent.isEmpty() ? name : parent + "." + name;
  }

  private static String stripLeadingDot(String name) {
    return name.startsWith(".") ? name.substring(1) : name;
  }

  private record SourceLock(String protocol, String revision, Map<String, String> hashes) {}

  private record NamedProtoMessage(String fullName, DescriptorProto descriptor) {}

  private record NamedProtoEnum(String fullName, EnumDescriptorProto descriptor) {}

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

}
