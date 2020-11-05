package org.gbif.collections.sync.idigbio;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.gbif.api.model.registry.MachineTag;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import static org.gbif.collections.sync.common.parsers.DataParser.getStringList;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IDigBioUtils {

  public static final String IDIGBIO_NAMESPACE = "iDigBio.org";
  public static final String IDIGBIO_COLLECTION_UUID = "CollectionUUID";
  public static final Predicate<MachineTag> IS_IDIGBIO_COLLECTION_UUID_MT =
      mt ->
          mt.getNamespace().equals(IDIGBIO_NAMESPACE) && mt.getName().equals(IDIGBIO_COLLECTION_UUID);
  public static final String IH_SUFFIX_IDIGBIO = "<IH>";
  public static final String IDIGBIO_NO_CODE = "<idigbio-no-code>";

  public static List<String> getIdigbioCodes(String idigbioCode) {
    return getStringList(idigbioCode).stream()
        .map(s -> s.replace(IH_SUFFIX_IDIGBIO, "").trim())
        .collect(Collectors.toList());
  }
}
