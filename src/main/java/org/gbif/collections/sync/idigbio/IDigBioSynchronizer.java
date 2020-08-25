package org.gbif.collections.sync.idigbio;

import java.util.List;

import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.clients.proxy.IDigBioProxyClient;
import org.gbif.collections.sync.common.BaseSynchronizer;
import org.gbif.collections.sync.common.DataLoader;
import org.gbif.collections.sync.config.IDigBioConfig;
import org.gbif.collections.sync.idigbio.IDigBioDataLoader.IDigBioData;
import org.gbif.collections.sync.idigbio.match.IDigBioMatchResult;
import org.gbif.collections.sync.idigbio.match.IDigBioStaffMatchResultHandler;
import org.gbif.collections.sync.idigbio.match.Matcher;
import org.gbif.collections.sync.idigbio.model.IDigBioRecord;

import com.google.common.base.Strings;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IDigBioSynchronizer extends BaseSynchronizer<IDigBioRecord, IDigBioRecord> {

  private final IDigBioProxyClient iDigBioPoxyClient;
  private final IDigBioIssueNotifier issueNotifier;

  private IDigBioSynchronizer(
      IDigBioProxyClient proxyClient,
      IDigBioStaffMatchResultHandler staffResultHandler,
      IDigBioEntityConverter entityConverter) {
    super(proxyClient, staffResultHandler, entityConverter);
    this.iDigBioPoxyClient = proxyClient;
    this.issueNotifier = IDigBioIssueNotifier.getInstance(proxyClient.getIDigBioConfig());
  }

  @Builder
  public static IDigBioSynchronizer create(
      IDigBioConfig iDigBioConfig, DataLoader<IDigBioData> dataLoader) {
    if (dataLoader == null) {
      dataLoader = IDigBioDataLoader.create(iDigBioConfig);
    }
    IDigBioProxyClient proxyClient =
        IDigBioProxyClient.builder().dataLoader(dataLoader).iDigBioConfig(iDigBioConfig).build();

    return new IDigBioSynchronizer(
        proxyClient,
        new IDigBioStaffMatchResultHandler(proxyClient),
        IDigBioEntityConverter.create());
  }

  public SyncResult sync() {
    log.info("Starting the sync");
    List<IDigBioRecord> records = iDigBioPoxyClient.getIDigBioRecords();
    Matcher matcher = new Matcher(iDigBioPoxyClient);
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
      syncResultBuilder.conflict(handleConflict(matchResult));
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
