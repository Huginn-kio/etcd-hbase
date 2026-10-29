package io.etcdhbase.admin.config;

import java.io.InputStream;

/** Reads the initialization subset with the same strict JSON contract as the service. */
public final class InitializationConfigurationLoader {
  public static final int MAX_CONFIG_BYTES = StrictConfigurationJson.MAX_CONFIG_BYTES;
  private final StrictConfigurationJson json = new StrictConfigurationJson();

  /** The caller owns the input; errors never echo its contents. */
  public InitializationConfiguration load(InputStream input) {
    var configuration = json.bind(json.read(input), InitializationConfiguration.class);
    InitializationConfigurationValidator.validate(configuration);
    return configuration;
  }
}
