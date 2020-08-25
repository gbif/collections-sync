package org.gbif.collections.sync.idigbio;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.clients.http.GrSciCollHttpClient;
import org.gbif.collections.sync.common.DataLoader;
import org.gbif.collections.sync.config.IDigBioConfig;
import org.gbif.collections.sync.idigbio.IDigBioDataLoader.IDigBioData;
import org.gbif.collections.sync.idigbio.model.IDigBioRecord;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IDigBioDataLoader implements DataLoader<IDigBioData> {

  private final IDigBioConfig iDigBioConfig;
  private final GrSciCollHttpClient grSciCollHttpClient;

  private IDigBioDataLoader(IDigBioConfig iDigBioConfig) {
    this.iDigBioConfig = iDigBioConfig;
    grSciCollHttpClient =
        GrSciCollHttpClient.getInstance(iDigBioConfig.getSyncConfig().getRegistry());
  }

  public static IDigBioDataLoader create(IDigBioConfig iDigBioConfig) {
    return new IDigBioDataLoader(iDigBioConfig);
  }

  public IDigBioData loadData() {
    Objects.requireNonNull(grSciCollHttpClient);

    CompletableFuture<List<Institution>> institutionsFuture =
        CompletableFuture.supplyAsync(grSciCollHttpClient::getInstitutions);
    CompletableFuture<List<Collection>> collectionsFuture =
        CompletableFuture.supplyAsync(grSciCollHttpClient::getCollections);
    CompletableFuture<List<Person>> personsFuture =
        CompletableFuture.supplyAsync(grSciCollHttpClient::getPersons);
    CompletableFuture<List<IDigBioRecord>> iDigBioRecordsFuture =
        CompletableFuture.supplyAsync(() -> readIDigBioRecords(iDigBioConfig));

    log.info("Loading data from WSs");
    CompletableFuture.allOf(
            institutionsFuture, collectionsFuture, personsFuture, iDigBioRecordsFuture)
        .join();

    return new IDigBioData(
        institutionsFuture.join(),
        collectionsFuture.join(),
        personsFuture.join(),
        iDigBioRecordsFuture.join());
  }

  private List<IDigBioRecord> readIDigBioRecords(IDigBioConfig config) {
    ObjectMapper objectMapper =
        new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    JavaType idigbioType =
        objectMapper.getTypeFactory().constructCollectionType(List.class, IDigBioRecord.class);

    try {
      return objectMapper.readValue(Paths.get(config.getExportFilePath()).toFile(), idigbioType);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Couldn't read iDigBio export file in path " + config.getExportFilePath());
    }
  }

  @AllArgsConstructor
  @Getter
  public static class IDigBioData {
    List<Institution> institutions;
    List<Collection> collections;
    List<Person> persons;
    List<IDigBioRecord> iDigBioRecords;
  }
}
