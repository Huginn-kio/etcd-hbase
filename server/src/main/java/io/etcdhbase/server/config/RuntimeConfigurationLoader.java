package io.etcdhbase.server.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.MarkedYAMLException;
import org.yaml.snakeyaml.error.YAMLException;

/** Reads and validates a bounded YAML runtime configuration without exposing input values on error. */
public final class RuntimeConfigurationLoader {
  public static final int ABSOLUTE_MAX_CONFIG_BYTES = 64 * 1024;
  private static final int MAX_YAML_NESTING = 32;
  private final RuntimeConfigurationValidator validator;
  private final ObjectMapper objectMapper;

  public RuntimeConfigurationLoader(
      Set<String> supportedProviderIds, Set<String> supportedSaltStrategyIds) {
    this.validator = new RuntimeConfigurationValidator(supportedProviderIds, supportedSaltStrategyIds);
    this.objectMapper =
        JsonMapper.builder()
            .propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
            .build();
  }

  /** Reads up to the hard limit plus one byte. The caller retains ownership of the stream. */
  public RuntimeConfiguration load(InputStream input) {
    if (input == null) {
      throw invalid("configuration", "input is required");
    }
    try {
      byte[] bytes = input.readNBytes(ABSOLUTE_MAX_CONFIG_BYTES + 1);
      if (bytes.length > ABSOLUTE_MAX_CONFIG_BYTES) {
        throw invalid("configuration", "exceeds the 65536-byte limit");
      }
      return load(bytes);
    } catch (IOException exception) {
      throw invalid("configuration", "could not be read");
    }
  }

  /** Parses and validates one UTF-8 YAML document. */
  public RuntimeConfiguration load(byte[] input) {
    if (input == null) {
      throw invalid("configuration", "input is required");
    }
    if (input.length == 0) {
      throw invalid("configuration", "must not be empty");
    }
    if (input.length > ABSOLUTE_MAX_CONFIG_BYTES) {
      throw invalid("configuration", "exceeds the 65536-byte limit");
    }

    String yamlText = decodeUtf8(input);
    Object parsed = parseSingleDocument(yamlText);
    if (!(parsed instanceof Map<?, ?> rawRoot)) {
      throw invalid("configuration", "root must be a mapping");
    }

    Object safeTree = validateYamlTree(rawRoot, "configuration", 0);
    RuntimeConfiguration configuration = bind(safeTree);
    validator.validate(configuration, input.length);
    return configuration;
  }

  private static String decodeUtf8(byte[] input) {
    try {
      return StandardCharsets.UTF_8
          .newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(input))
          .toString();
    } catch (CharacterCodingException exception) {
      throw invalid("configuration", "must be valid UTF-8");
    }
  }

  private static Object parseSingleDocument(String yamlText) {
    LoaderOptions options = new LoaderOptions();
    options.setAllowDuplicateKeys(false);
    options.setAllowRecursiveKeys(false);
    options.setMaxAliasesForCollections(0);
    options.setNestingDepthLimit(MAX_YAML_NESTING);
    options.setCodePointLimit(ABSOLUTE_MAX_CONFIG_BYTES);
    options.setMergeOnCompose(false);

    try {
      Iterator<Object> documents = new Yaml(new SafeConstructor(options)).loadAll(yamlText).iterator();
      if (!documents.hasNext()) {
        throw invalid("configuration", "must contain one YAML document");
      }
      Object document = documents.next();
      if (documents.hasNext()) {
        throw invalid("configuration", "must contain exactly one YAML document");
      }
      return document;
    } catch (YAMLException exception) {
      String location = "configuration";
      if (exception instanceof MarkedYAMLException marked && marked.getProblemMark() != null) {
        location +=
            " at line "
                + (marked.getProblemMark().getLine() + 1)
                + ", column "
                + (marked.getProblemMark().getColumn() + 1);
      }
      throw invalid(location, "contains invalid or unsupported YAML");
    }
  }

  private static Object validateYamlTree(Object value, String path, int depth) {
    if (depth > MAX_YAML_NESTING) {
      throw invalid(path, "exceeds the nesting limit");
    }
    if (value == null || value instanceof String || value instanceof Boolean || value instanceof Number) {
      return value;
    }
    if (value instanceof List<?> list) {
      List<Object> copy = new ArrayList<>(list.size());
      for (int index = 0; index < list.size(); index++) {
        if (list.get(index) == null) {
          throw invalid(path + "[" + index + "]", "must not be null");
        }
        copy.add(validateYamlTree(list.get(index), path + "[" + index + "]", depth + 1));
      }
      return List.copyOf(copy);
    }
    if (value instanceof Map<?, ?> mapping) {
      Map<String, Object> copy = new LinkedHashMap<>();
      for (Map.Entry<?, ?> entry : mapping.entrySet()) {
        if (!(entry.getKey() instanceof String key)) {
          throw invalid(path, "mapping keys must be strings");
        }
        copy.put(key, validateYamlTree(entry.getValue(), appendPath(path, key), depth + 1));
      }
      return Collections.unmodifiableMap(copy);
    }
    throw invalid(path, "contains an unsupported YAML value");
  }

  private RuntimeConfiguration bind(Object safeTree) {
    try {
      return objectMapper.convertValue(safeTree, RuntimeConfiguration.class);
    } catch (RuntimeException exception) {
      JsonMappingException mappingException = findMappingException(exception);
      String path = mappingException == null ? "configuration" : sanitizedPath(mappingException);
      throw invalid(path, "has an invalid or unsupported value");
    }
  }

  private static JsonMappingException findMappingException(Throwable error) {
    Throwable current = error;
    while (current != null) {
      if (current instanceof JsonMappingException mappingException) {
        return mappingException;
      }
      current = current.getCause();
    }
    return null;
  }

  private static String sanitizedPath(JsonMappingException exception) {
    StringBuilder path = new StringBuilder("configuration");
    for (JsonMappingException.Reference reference : exception.getPath()) {
      if (reference.getFieldName() != null) {
        path.append('.').append(sanitizePathToken(reference.getFieldName()));
      } else if (reference.getIndex() >= 0) {
        path.append('[').append(reference.getIndex()).append(']');
      }
    }
    return path.toString();
  }

  private static String sanitizePathToken(String token) {
    StringBuilder sanitized = new StringBuilder();
    for (int index = 0; index < token.length() && sanitized.length() < 64; index++) {
      char character = token.charAt(index);
      if (Character.isLetterOrDigit(character) || character == '_' || character == '-') {
        sanitized.append(character);
      }
    }
    return sanitized.isEmpty() ? "field" : sanitized.toString();
  }

  private static String appendPath(String path, String token) {
    return path + "." + sanitizePathToken(token);
  }

  private static RuntimeConfigurationException invalid(String path, String problem) {
    return new RuntimeConfigurationException(path + " " + problem);
  }
}
