package org.gbif.collections.sync.idigbio;

import java.util.Set;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import static org.gbif.collections.sync.parsers.DataParser.getStringList;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IDigBioUtils {

  static final String IDIGBIO_NAMESPACE = "iDigBio.org";
  static final String IH_SUFFIX_IDIGBIO = "<IH>";

  public static Set<String> getIdigbioCode(String idigbioCode) {
    return getStringList(idigbioCode).stream()
        .map(s -> s.replace(IH_SUFFIX_IDIGBIO, ""))
        .collect(Collectors.toSet());
  }

}
