package org.gbif.collections.sync.ih;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Utils {

  private static final String GRSCICOLL_MIGRATION_USER = "registry-migration-grbio.gbif.org";

  /**
   * Encodes the IH IRN into the format stored on the GRSciColl identifier. E.g. 123 ->
   * gbif:ih:irn:123
   */
  public static String encodeIRN(String irn) {
    return "gbif:ih:irn:" + irn;
  }
}
