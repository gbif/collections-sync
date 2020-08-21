package org.gbif.collections.sync.idigbio;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.common.DataLoader;
import org.gbif.collections.sync.config.IDigBioConfig;
import org.gbif.collections.sync.idigbio.match.MatchResult;
import org.gbif.collections.sync.idigbio.match.Matcher;
import org.gbif.collections.sync.idigbio.match.strategy.CollectionMatchStrategy;
import org.gbif.collections.sync.idigbio.match.strategy.ConflictStrategy;
import org.gbif.collections.sync.idigbio.match.strategy.InstitutionAndCollectionMatchStrategy;
import org.gbif.collections.sync.idigbio.match.strategy.InstitutionMatchStrategy;
import org.gbif.collections.sync.idigbio.match.strategy.NoMatchStrategy;
import org.gbif.collections.sync.notification.IDigBioIssueNotifier;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Strings;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IDigBioSync {

  private final IDigBioConfig iDigBioConfig;
  private final DataLoader dataLoader;

  @Builder
  private IDigBioSync(IDigBioConfig iDigBioConfig, DataLoader dataLoader) {
    this.iDigBioConfig = iDigBioConfig;
    if (dataLoader != null) {
      this.dataLoader = dataLoader;
    } else {
      this.dataLoader = DataLoader.create(iDigBioConfig.getSyncConfig());
    }
  }

  public SyncResult sync() {
    // load data
    IDigBioProxyClient proxyClient =
        IDigBioProxyClient.builder().dataLoader(dataLoader).iDigBioConfig(iDigBioConfig).build();
    Matcher matcher = new Matcher(proxyClient);

    SyncResult.SyncResultBuilder syncResultBuilder = SyncResult.builder();

    List<IDigBioRecord> records = readIDigBioExport(iDigBioConfig);
    for (IDigBioRecord record : records) {
      if (isInvalidRecord(record)) {
        continue;
      }

      MatchResult match = matcher.match(record);
      if (match.onlyCollectionMatch()) {
        CollectionMatchStrategy.create(proxyClient)
            .andThen(syncResultBuilder::collectionOnlyMatch)
            .apply(match);
      } else if (match.onlyInstitutionMatch()) {
        InstitutionMatchStrategy.create(proxyClient)
            .andThen(syncResultBuilder::institutionOnlyMatch)
            .apply(match);
      } else if (match.noMatches()) {
        if (!hasCodeAndName(match.getIDigBioRecord())) {
          syncResultBuilder.invalidEntity(match.getIDigBioRecord());
        }
        NoMatchStrategy.create(proxyClient).andThen(syncResultBuilder::noMatch).apply(match);
      } else if (match.institutionAndCollectionMatch()) {
        InstitutionAndCollectionMatchStrategy.create(proxyClient)
            .andThen(syncResultBuilder::instAndCollMatch)
            .apply(match);
      } else {
        ConflictStrategy.create(iDigBioConfig).andThen(syncResultBuilder::conflict).apply(match);
      }
    }

    SyncResult syncResult = syncResultBuilder.build();

    if (syncResult.getInvalidEntities() != null && !syncResult.getInvalidEntities().isEmpty()) {
      IDigBioIssueNotifier.create(iDigBioConfig)
          .createInvalidEntitiesIssue(syncResult.getInvalidEntities());
    }

    return syncResult;
  }

  private List<IDigBioRecord> readIDigBioExport(IDigBioConfig config) {
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

  private boolean isInvalidRecord(IDigBioRecord record) {
    return Strings.isNullOrEmpty(record.getInstitution())
        && Strings.isNullOrEmpty(record.getInstitutionCode())
        && Strings.isNullOrEmpty(record.getCollection())
        && Strings.isNullOrEmpty(record.getCollectionCode());
  }

  private boolean hasCodeAndName(IDigBioRecord iDigBioRecord) {
    return (!Strings.isNullOrEmpty(iDigBioRecord.getInstitution())
            || !Strings.isNullOrEmpty(iDigBioRecord.getCollection()))
        && (!Strings.isNullOrEmpty(iDigBioRecord.getInstitutionCode())
            || !Strings.isNullOrEmpty(iDigBioRecord.getCollectionCode()));
  }
}
