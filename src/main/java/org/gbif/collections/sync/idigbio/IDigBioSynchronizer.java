package org.gbif.collections.sync.idigbio;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.SyncResult.Conflict;
import org.gbif.collections.sync.clients.proxy.IDigBioProxyClient;
import org.gbif.collections.sync.common.DataLoader;
import org.gbif.collections.sync.common.BaseSynchronizer;
import org.gbif.collections.sync.config.IDigBioConfig;
import org.gbif.collections.sync.idigbio.match.IDigBioMatchResult;
import org.gbif.collections.sync.idigbio.match.IDigBioStaffMatchResultHandler;
import org.gbif.collections.sync.idigbio.match.Matcher;
import org.gbif.collections.sync.idigbio.model.IDigBioRecord;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Strings;
import lombok.Builder;

public class IDigBioSynchronizer extends BaseSynchronizer<IDigBioRecord, IDigBioRecord> {

  private final IDigBioProxyClient proxyClient;
  private final IDigBioIssueNotifier issueNotifier;

  private IDigBioSynchronizer(
      IDigBioProxyClient proxyClient,
      IDigBioStaffMatchResultHandler staffResultHandler,
      IDigBioEntityConverter entityConverter) {
    super(proxyClient, staffResultHandler, entityConverter);
    this.proxyClient = proxyClient;
    this.issueNotifier = IDigBioIssueNotifier.create(proxyClient.getIDigBioConfig());
  }

  @Builder
  public static IDigBioSynchronizer create(IDigBioConfig iDigBioConfig, DataLoader dataLoader) {
    if (dataLoader == null) {
      dataLoader = DataLoader.create(iDigBioConfig.getSyncConfig());
    }
    IDigBioProxyClient proxyClient =
        IDigBioProxyClient.builder().dataLoader(dataLoader).iDigBioConfig(iDigBioConfig).build();

    return new IDigBioSynchronizer(
        proxyClient,
        new IDigBioStaffMatchResultHandler(proxyClient),
        IDigBioEntityConverter.create());
  }

  public SyncResult sync() {
    List<IDigBioRecord> records = readIDigBioExport(proxyClient.getIDigBioConfig());
    Matcher matcher = new Matcher(proxyClient);
    SyncResult.SyncResultBuilder resultBuilder = SyncResult.builder();
    records.stream()
        .filter(r -> !isInvalidRecord(r))
        .map(matcher::match)
        .forEach(m -> handleResult(m, resultBuilder));

    SyncResult syncResult = resultBuilder.build();

    if (syncResult.getInvalidEntities() != null && !syncResult.getInvalidEntities().isEmpty()) {
      issueNotifier.createInvalidEntitiesIssue(syncResult.getInvalidEntities());
    }

    return syncResult;
  }

  private void handleResult(
      IDigBioMatchResult matchResult, SyncResult.SyncResultBuilder syncResultBuilder) {
    if (matchResult.onlyOneCollectionMatch()) {
      syncResultBuilder.collectionOnlyMatch(handleCollectionMatch(matchResult));
    } else if (matchResult.onlyOneInstitutionMatch()) {
      syncResultBuilder.institutionOnlyMatch(handleInstitutionMatch(matchResult));
    } else if (matchResult.noMatches()) {
      if (!hasCodeAndName(matchResult.getSource())) {
        syncResultBuilder.invalidEntity(matchResult.getSource());
      }
      syncResultBuilder.noMatch(handleNoMatch(matchResult));
    } else if (matchResult.institutionAndCollectionMatch()) {
      syncResultBuilder.instAndCollMatch(handleInstAndCollMatch(matchResult));
    } else {
      issueNotifier.createConflict(matchResult.getAllMatches(), matchResult.getSource());
      syncResultBuilder.conflict(
          new Conflict(matchResult.getSource(), matchResult.getAllMatches()));
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
}
