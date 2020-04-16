package org.gbif.collections.sync.idigbio;

import java.util.Date;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import static org.gbif.collections.sync.parsers.DataParser.TO_LOCAL_DATE_TIME_UTC;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IDigBioUtils {

  public static boolean isIDigBioMoreRecent(IDigBioRecord record, Date grSciCollEntityDate) {
    return record.getModifiedDate() == null
        || grSciCollEntityDate == null
        || record.getModifiedDate().isAfter(TO_LOCAL_DATE_TIME_UTC.apply(grSciCollEntityDate));
  }

  public static String removeUuidNamespace(String identifier) {
    return identifier.replace("urn:uuid:", "");
  }
}
