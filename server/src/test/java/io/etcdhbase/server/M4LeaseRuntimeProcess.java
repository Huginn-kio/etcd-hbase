package io.etcdhbase.server;

import io.etcdhbase.server.config.M3RuntimeConfiguration;

/** Test-only process entrypoint that selects the public M4 profile from trusted test code. */
public final class M4LeaseRuntimeProcess {
  private M4LeaseRuntimeProcess() {}

  public static void main(String[] arguments) {
    int result = RuntimeServer.run(arguments, M3RuntimeConfiguration.ActivationProfile.PUBLIC_TEST);
    if (result != 0) System.exit(result);
  }
}
