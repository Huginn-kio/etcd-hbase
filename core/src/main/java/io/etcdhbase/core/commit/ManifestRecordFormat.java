package io.etcdhbase.core.commit;

/** Stable tag assignments embedded in the canonical identities of J22 manifest records. */
final class ManifestRecordFormat {
  static final byte MANIFEST_PAGE = 1;
  static final byte MANIFEST_DIRECTORY_PAGE = 2;
  static final byte MANIFEST_ROOT = 3;
  static final byte PREPARED_EVENT = 4;
  static final byte EVENT_PAGE = 5;
  static final byte EVENT_DIRECTORY_PAGE = 6;

  private ManifestRecordFormat() {}
}
