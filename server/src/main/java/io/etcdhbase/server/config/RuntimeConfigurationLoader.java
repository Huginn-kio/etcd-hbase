package io.etcdhbase.server.config;

import io.etcdhbase.admin.config.StrictConfigurationJson;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;

/** File first, documented environment overrides second, then shared and service validation. */
public final class RuntimeConfigurationLoader {
  private final StrictConfigurationJson json = new StrictConfigurationJson();

  public RuntimeConfiguration load(InputStream input) {
    return load(input, System.getenv());
  }

  /** The caller owns the input. Neither input text nor credential references enter errors. */
  public RuntimeConfiguration load(InputStream input, Map<String, String> environment) {
    Objects.requireNonNull(input, "configuration input");
    Objects.requireNonNull(environment, "environment");
    try {
      byte[] bytes = input.readNBytes(StrictConfigurationJson.MAX_CONFIG_BYTES + 1);
      var tree = json.read(new ByteArrayInputStream(bytes));
      long effectiveBytes = bytes.length + ConfigurationOverrides.apply(tree, environment);
      var configuration = json.bind(tree, RuntimeConfiguration.class);
      RuntimeConfigurationValidator.validate(configuration, effectiveBytes);
      return configuration;
    } catch (IOException failure) {
      throw new IllegalArgumentException("configuration cannot be read");
    }
  }
}
