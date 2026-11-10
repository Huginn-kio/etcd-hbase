package io.etcdhbase.server.config;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.etcdhbase.admin.config.StrictConfigurationJson;
import java.io.InputStream;

/** Loads the explicit M3 activation file without resolving credentials or contacting HBase. */
public final class M3RuntimeConfigurationLoader {
  private final StrictConfigurationJson json = new StrictConfigurationJson();

  public M3RuntimeConfiguration load(InputStream input) {
    var tree = json.read(input);
    if (tree instanceof ObjectNode object) {
      if (!object.has("read_cache_enabled")) object.put("read_cache_enabled", false);
      else if (object.path("read_cache_enabled").isBoolean() && object.path("read_cache_enabled").booleanValue()) {
        throw new M3RuntimeConfiguration.CacheActivationUnavailable();
      }
    }
    return json.bind(tree, M3RuntimeConfiguration.class);
  }
}
