package org.gbif.collections.sync.ih;

import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.common.DataLoader;
import org.gbif.collections.sync.common.DataLoader.GrSciCollAndIHData;
import org.gbif.collections.sync.config.IHConfig;
import org.gbif.collections.sync.http.clients.IHHttpClient;
import org.gbif.collections.sync.ih.match.MatchResult;
import org.gbif.collections.sync.ih.match.Matcher;
import org.gbif.collections.sync.ih.match.strategy.CollectionMatchStrategy;
import org.gbif.collections.sync.ih.match.strategy.ConflictStrategy;
import org.gbif.collections.sync.ih.match.strategy.InstitutionAndCollectionMatchStrategy;
import org.gbif.collections.sync.ih.match.strategy.InstitutionMatchStrategy;
import org.gbif.collections.sync.ih.match.strategy.NoMatchStrategy;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.notification.IHIssueNotifier;
import org.gbif.collections.sync.parsers.CountryParser;

import com.google.common.base.Strings;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

/** Syncs IH entities with GrSciColl ones present in GBIF registry. */
@Slf4j
public class IHSync {

  private final IHConfig ihConfig;
  private final EntityConverter entityConverter;
  private final CountryParser countryParser;
  private final DataLoader dataLoader;

  @Builder
  private IHSync(
      IHConfig ihConfig,
      EntityConverter entityConverter,
      CountryParser countryParser,
      DataLoader dataLoader) {
    this.ihConfig = ihConfig;
    if (countryParser != null) {
      this.countryParser = countryParser;
    } else {
      this.countryParser =
          CountryParser.from(IHHttpClient.getInstance(ihConfig.getIhWsUrl()).getCountries());
    }

    if (entityConverter != null) {
      this.entityConverter = entityConverter;
    } else {
      this.entityConverter = EntityConverter.create(countryParser);
    }

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

    Matcher matcher =
        Matcher.builder().proxyClient(proxyClient).countryParser(countryParser).build();

    // do the sync
    log.info("Starting the sync");
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
                CollectionMatchStrategy.builder()
                    .entityConverter(entityConverter)
                    .ihConfig(ihConfig)
                    .proxyClient(proxyClient)
                    .build()
                    .andThen(syncResultBuilder::collectionOnlyMatch)
                    .apply(match);
              } else if (match.onlyOneInstitutionMatch()) {
                InstitutionMatchStrategy.builder()
                    .entityConverter(entityConverter)
                    .ihConfig(ihConfig)
                    .proxyClient(proxyClient)
                    .build()
                    .andThen(syncResultBuilder::institutionOnlyMatch)
                    .apply(match);
              } else if (match.noMatches()) {
                NoMatchStrategy.builder()
                    .entityConverter(entityConverter)
                    .ihConfig(ihConfig)
                    .proxyClient(proxyClient)
                    .build()
                    .andThen(syncResultBuilder::noMatch)
                    .apply(match);
              } else if (match.institutionAndCollectionMatch()) {
                InstitutionAndCollectionMatchStrategy.builder()
                    .entityConverter(entityConverter)
                    .ihConfig(ihConfig)
                    .proxyClient(proxyClient)
                    .build()
                    .andThen(syncResultBuilder::instAndCollMatch)
                    .apply(match);
              } else {
                ConflictStrategy.builder()
                    .ihConfig(ihConfig)
                    .build()
                    .andThen(syncResultBuilder::conflict)
                    .apply(match);
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
