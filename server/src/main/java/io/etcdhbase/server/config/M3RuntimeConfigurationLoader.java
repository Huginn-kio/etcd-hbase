package io.etcdhbase.server.config;

import io.etcdhbase.admin.config.StrictConfigurationJson;
import java.io.InputStream;

/** Loads the explicit M3 activation file without resolving credentials or contacting HBase. */
public final class M3RuntimeConfigurationLoader {
  private final StrictConfigurationJson json = new StrictConfigurationJson();

  public M3RuntimeConfiguration load(InputStream input) {
    return json.bind(json.read(input), M3RuntimeConfiguration.class);
  }
}
