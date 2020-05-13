package org.gbif.collections.sync.idigbio;

import java.util.List;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import static org.gbif.collections.sync.parsers.DataParser.getStringList;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IDigBioUtils {

  static final String IDIGBIO_NAMESPACE = "iDigBio.org";
  static final String IH_SUFFIX_IDIGBIO = "<IH>";

  public static List<String> getIdigbioCodes(String idigbioCode) {
    return getStringList(idigbioCode).stream()
        .map(s -> s.replace(IH_SUFFIX_IDIGBIO, "").trim())
        .collect(Collectors.toList());
  }
}
