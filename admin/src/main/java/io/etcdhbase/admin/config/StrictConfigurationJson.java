package io.etcdhbase.admin.config;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** Strict, bounded JSON input shared by initialization and service configuration. */
public final class StrictConfigurationJson {
  public static final int MAX_CONFIG_BYTES = 65_536;
  private final ObjectMapper mapper =
      JsonMapper.builder(
              JsonFactory.builder()
                  .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                  .streamReadConstraints(
                      StreamReadConstraints.builder().maxNestingDepth(32).build())
                  .build())
          .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
          .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
          .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
          .enable(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS)
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
          .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
          .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
          .build();

  /** The caller owns the input; errors never echo its contents. */
  public JsonNode read(InputStream input) {
    if (input == null) {
      throw new IllegalArgumentException("configuration input is required");
    }
    try {
      byte[] bytes = input.readNBytes(MAX_CONFIG_BYTES + 1);
      if (bytes.length == 0 || bytes.length > MAX_CONFIG_BYTES) {
        throw new IllegalArgumentException("configuration must be 1..65536 bytes");
      }
      String json =
          StandardCharsets.UTF_8
              .newDecoder()
              .onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT)
              .decode(ByteBuffer.wrap(bytes))
              .toString();
      JsonNode tree = mapper.readTree(json);
      return tree;
    } catch (IOException failure) {
      throw new IllegalArgumentException(
          "configuration cannot be read or violates its JSON schema");
    }
  }

  /** Validates required record fields before Jackson can silently supply primitive defaults. */
  public <T> T bind(JsonNode tree, Class<T> recordType) {
    requireProperties(tree, recordType);
    try {
      return mapper.treeToValue(tree, recordType);
    } catch (IOException failure) {
      throw new IllegalArgumentException("configuration violates its JSON schema");
    }
  }

  private static void requireProperties(JsonNode node, Class<?> recordType) {
    if (node == null || !node.isObject()) {
      throw new IllegalArgumentException("configuration records must be objects");
    }
    for (RecordComponent component : recordType.getRecordComponents()) {
      String name = snakeCase(component.getName());
      boolean optional =
          recordType == InitializationConfiguration.HBaseProfile.class
              && component.getName().equals("credentialReference");
      JsonNode value = node.get(name);
      if (value == null && optional) {
        continue;
      }
      if (value == null || value.isNull()) {
        throw new IllegalArgumentException(
            "required configuration property is missing or null: " + name);
      }
      Class<?> elementType = listElementType(component);
      if (elementType == null) requireValue(value, component.getType());
      else requireArray(value, elementType);
    }
  }

  private static Class<?> listElementType(RecordComponent component) {
    if (List.class.isAssignableFrom(component.getType())
        && component.getGenericType() instanceof ParameterizedType listType
        && listType.getActualTypeArguments()[0] instanceof Class<?> elementType) {
      return elementType;
    }
    return null;
  }

  private static void requireArray(JsonNode value, Class<?> elementType) {
    if (!value.isArray())
      throw new IllegalArgumentException("configuration collection must be an array");
    for (JsonNode item : value) requireValue(item, elementType);
  }

  private static void requireValue(JsonNode value, Class<?> type) {
    if (value == null || value.isNull())
      throw new IllegalArgumentException("configuration value must not be null");
    if (type.isRecord()) {
      requireProperties(value, type);
      return;
    }
    boolean textType = type == String.class || type.isEnum();
    boolean integerType = type == int.class || type == long.class;
    boolean matches =
        (textType && value.isTextual())
            || (integerType && value.isIntegralNumber())
            || (type == boolean.class && value.isBoolean());
    if (!matches) throw new IllegalArgumentException("configuration value has the wrong JSON type");
  }

  private static String snakeCase(String value) {
    StringBuilder result = new StringBuilder();
    for (char character : value.toCharArray()) {
      if (Character.isUpperCase(character)) {
        result.append('_').append(Character.toLowerCase(character));
      } else {
        result.append(character);
      }
    }
    return result.toString();
  }
}
