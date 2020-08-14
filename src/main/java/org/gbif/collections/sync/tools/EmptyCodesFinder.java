//package org.gbif.collections.sync.tools;
//
//import java.io.IOException;
//import java.nio.file.Paths;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Set;
//
//import org.gbif.collections.sync.idigbio.IDigBioRecord;
//import org.gbif.collections.sync.idigbio.IDigBioSync;
//
//import com.fasterxml.jackson.databind.DeserializationFeature;
//import com.fasterxml.jackson.databind.JavaType;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.SerializationFeature;
//import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
//import com.google.common.base.Strings;
//import lombok.extern.slf4j.Slf4j;
//
//@Slf4j
//public class EmptyCodesFinder {
//
//  public static void main(String[] args) {
//    ObjectMapper objectMapper =
//        new ObjectMapper()
//            .registerModule(new JavaTimeModule())
//            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
//            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
//            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//
//    JavaType idigbioType =
//        objectMapper.getTypeFactory().constructCollectionType(List.class, IDigBioRecord.class);
//
//    try {
//      List<IDigBioRecord> records =
//          objectMapper.readValue(
//              Paths.get("src/main/resources/manualMatches.json").toFile(), idigbioType);
//
//      Set<String> unique = new HashSet<>();
//      records.forEach(
//          r -> {
//            if (IDigBioSync.isInvalidRecord(r)) {
//              return;
//            }
//
//            if (Strings.isNullOrEmpty(r.getCollectionCode())
//                && Strings.isNullOrEmpty(r.getInstitutionCode())) {
//              log.warn("No codes");
//            }
//
//            if (Strings.isNullOrEmpty(r.getCollectionCode()) && r.getCollectionUuid() != null) {
//              if (!unique.contains(r.getCollectionUuid())) {
//                unique.add(r.getCollectionUuid());
////                log.warn("{};{}", r.getCollectionUuid(), r.getInstitutionCode());
//              }
//            } else if (!Strings.isNullOrEmpty(r.getCollectionCode())) {
//              //              log.warn(
//              //                  "code: {}, uuid: {} - institutionCode: {}",
//              //                  r.getCollectionCode(),
//              //                  r.getCollectionUuid(),
//              //                  r.getInstitutionCode());
//            }
//          });
//
//    } catch (IOException e) {
//      throw new IllegalArgumentException("Couldn't read iDigBio export file");
//    }
//  }
//}
