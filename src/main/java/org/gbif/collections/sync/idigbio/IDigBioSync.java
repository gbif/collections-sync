package org.gbif.collections.sync.idigbio;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.common.DataLoader;
import org.gbif.collections.sync.common.MatchResultStrategy;
import org.gbif.collections.sync.config.IDigBioConfig;
import org.gbif.collections.sync.idigbio.match.MatchResult;
import org.gbif.collections.sync.idigbio.match.MatchResult.MatchType;
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
  private final Map<MatchType, MatchResultStrategy> resultMatchStrategies = new HashMap<>();

  @Builder
  private IDigBioSync(IDigBioConfig iDigBioConfig, DataLoader dataLoader) {
    // TODO: remove this and create another constructor for testing
    this.iDigBioConfig = iDigBioConfig;
    if (dataLoader != null) {
      this.dataLoader = dataLoader;
    } else {
      this.dataLoader = DataLoader.create(iDigBioConfig.getSyncConfig());
    }
  }

  public SyncResult sync() {
    Matcher matcher = new Matcher(dataLoader);
    SyncResult.SyncResultBuilder syncResultBuilder = SyncResult.builder();

    IDigBioIssueNotifier issueNotifier =
        IDigBioIssueNotifier.create(iDigBioConfig, syncResultBuilder);

    createStrategies(iDigBioConfig, syncResultBuilder, matcher);

    List<IDigBioRecord> records = readIDigBioExport(iDigBioConfig);
    for (IDigBioRecord record : records) {
      if (isInvalidRecord(record)) {
        continue;
      }

      MatchResult match = matcher.match(record);
      resultMatchStrategies.get(match.getMatchType()).handle(match);
    }

    SyncResult syncResult = syncResultBuilder.build();

    if (syncResult.getInvalidEntities() != null && !syncResult.getInvalidEntities().isEmpty()) {
      issueNotifier.createInvalidEntitiesIssue(syncResult.getInvalidEntities());
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

  private void createStrategies(
      IDigBioConfig iDigBioConfig,
      SyncResult.SyncResultBuilder syncResultBuilder,
      Matcher matcher) {

    resultMatchStrategies.put(
        MatchType.ONLY_COLLECTION,
        CollectionMatchStrategy.builder()
            .syncConfig(iDigBioConfig.getSyncConfig())
            .syncResultBuilder(syncResultBuilder)
            .matcher(matcher)
            .build());
    resultMatchStrategies.put(
        MatchType.ONLY_INSTITUTION,
        InstitutionMatchStrategy.builder()
            .syncConfig(iDigBioConfig.getSyncConfig())
            .syncResultBuilder(syncResultBuilder)
            .matcher(matcher)
            .build());
    resultMatchStrategies.put(
        MatchType.NO_MATCH,
        NoMatchStrategy.builder()
            .syncConfig(iDigBioConfig.getSyncConfig())
            .syncResultBuilder(syncResultBuilder)
            .matcher(matcher)
            .build());
    resultMatchStrategies.put(
        MatchType.INST_AND_COLL,
        InstitutionAndCollectionMatchStrategy.builder()
            .syncConfig(iDigBioConfig.getSyncConfig())
            .syncResultBuilder(syncResultBuilder)
            .matcher(matcher)
            .build());
    resultMatchStrategies.put(
        MatchType.CONFLICT,
        ConflictStrategy.builder()
            .iDigBioConfig(iDigBioConfig)
            .syncResultBuilder(syncResultBuilder)
            .build());
  }
}
