package org.gbif.collections.sync.ih;

import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.common.DataLoader;
import org.gbif.collections.sync.common.DataLoader.GrSciCollAndIHData;
import org.gbif.collections.sync.config.IHConfig;
import org.gbif.collections.sync.ih.match.MatchResult;
import org.gbif.collections.sync.ih.match.Matcher;
import org.gbif.collections.sync.ih.match.strategy.CollectionMatchStrategy;
import org.gbif.collections.sync.ih.match.strategy.ConflictStrategy;
import org.gbif.collections.sync.ih.match.strategy.InstitutionAndCollectionMatchStrategy;
import org.gbif.collections.sync.ih.match.strategy.InstitutionMatchStrategy;
import org.gbif.collections.sync.ih.match.strategy.NoMatchStrategy;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.notification.IHIssueNotifier;

import com.google.common.base.Strings;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

/** Syncs IH entities with GrSciColl ones present in GBIF registry. */
@Slf4j
public class IHSync {

  private final IHConfig ihConfig;
  private final DataLoader dataLoader;

  @Builder
  private IHSync(IHConfig ihConfig, EntityConverter entityConverter, DataLoader dataLoader) {
    this.ihConfig = ihConfig;

    if (dataLoader != null) {
      this.dataLoader = dataLoader;
    } else {
      this.dataLoader = DataLoader.create(ihConfig);
    }

    log.info(
        "Sync created with dryRun {} and sendNotifications {}",
        this.ihConfig.getSyncConfig().isDryRun(),
        this.ihConfig.getSyncConfig().isSendNotifications());
  }

  public SyncResult sync() {
    // load the data from the WS
    GrSciCollAndIHData data = DataLoader.create(ihConfig).fetchGrSciCollAndIHData();

    IHProxyClient proxyClient =
        IHProxyClient.builder().dataLoader(dataLoader).ihConfig(ihConfig).build();

    // do the sync
    log.info("Starting the sync");
    Matcher matcher = Matcher.create(proxyClient);
    SyncResult.SyncResultBuilder syncResultBuilder = SyncResult.builder();
    IHIssueNotifier issueNotifier = IHIssueNotifier.create(ihConfig);
    data.getIhInstitutions()
        .forEach(
            ihInstitution -> {
              if (!isValidIhInstitution(ihInstitution, issueNotifier)) {
                syncResultBuilder.invalidEntity(ihInstitution);
                return;
              }

              MatchResult match = matcher.match(ihInstitution);
              if (match.onlyOneCollectionMatch()) {
                CollectionMatchStrategy.create(proxyClient)
                    .andThen(syncResultBuilder::collectionOnlyMatch)
                    .apply(match);
              } else if (match.onlyOneInstitutionMatch()) {
                InstitutionMatchStrategy.create(proxyClient)
                    .andThen(syncResultBuilder::institutionOnlyMatch)
                    .apply(match);
              } else if (match.noMatches()) {
                NoMatchStrategy.create(proxyClient)
                    .andThen(syncResultBuilder::noMatch)
                    .apply(match);
              } else if (match.institutionAndCollectionMatch()) {
                InstitutionAndCollectionMatchStrategy.create(proxyClient)
                    .andThen(syncResultBuilder::instAndCollMatch)
                    .apply(match);
              } else {
                ConflictStrategy.create(ihConfig).andThen(syncResultBuilder::conflict).apply(match);
              }
            });

    SyncResult result = syncResultBuilder.build();

    // create a notification with all the fails
    if (!result.getFailedActions().isEmpty()) {
      issueNotifier.createFailsNotification(result.getFailedActions());
    }

    return result;
  }

  private boolean isValidIhInstitution(IHInstitution ihInstitution, IHIssueNotifier issueNotifier) {
    if (Strings.isNullOrEmpty(ihInstitution.getOrganization())
        || Strings.isNullOrEmpty(ihInstitution.getCode())) {
      issueNotifier.createInvalidEntity(
          ihInstitution, "Not valid institution - name and code are required");
      return false;
    }
    return true;
  }
}
