package org.gbif.collections.sync.ih;

import java.util.HashMap;
import java.util.Map;

import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.common.DataLoader;
import org.gbif.collections.sync.common.DataLoader.GrSciCollAndIHData;
import org.gbif.collections.sync.common.MatchResultStrategy;
import org.gbif.collections.sync.config.IHConfig;
import org.gbif.collections.sync.http.clients.IHHttpClient;
import org.gbif.collections.sync.idigbio.match.MatchResult.MatchType;
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
  private final Map<MatchType, MatchResultStrategy> resultMatchStrategies = new HashMap<>();

  @Builder
  private IHSync(
      IHConfig ihConfig,
      EntityConverter entityConverter,
      CountryParser countryParser,
      DataLoader dataLoader) {
    // TODO: remove this and create another constructor for testing
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
      this.entityConverter =
          EntityConverter.create(countryParser, ihConfig.getSyncConfig().getRegistryWsUser());
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

    Matcher matcher = Matcher.builder().dataLoader(dataLoader).countryParser(countryParser).build();

    // do the sync
    log.info("Starting the sync");
    SyncResult.SyncResultBuilder syncResultBuilder = SyncResult.builder();
    IHIssueNotifier issueNotifier = IHIssueNotifier.create(ihConfig, syncResultBuilder);
    createStrategies(ihConfig, entityConverter, syncResultBuilder);
    data.getIhInstitutions()
        .forEach(
            ihInstitution -> {
              if (!isValidIhInstitution(ihInstitution, issueNotifier)) {
                syncResultBuilder.invalidEntity(ihInstitution);
                return;
              }

              MatchResult match = matcher.match(ihInstitution);
              resultMatchStrategies.get(match.getMatchType()).handle(match);
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

  private void createStrategies(
      IHConfig ihConfig,
      EntityConverter entityConverter,
      SyncResult.SyncResultBuilder syncResultBuilder) {

    resultMatchStrategies.put(
        MatchType.ONLY_COLLECTION,
        CollectionMatchStrategy.builder()
            .ihConfig(ihConfig)
            .entityConverter(entityConverter)
            .syncResultBuilder(syncResultBuilder)
            .build());
    resultMatchStrategies.put(
        MatchType.ONLY_INSTITUTION,
        InstitutionMatchStrategy.builder()
            .ihConfig(ihConfig)
            .entityConverter(entityConverter)
            .syncResultBuilder(syncResultBuilder)
            .build());
    resultMatchStrategies.put(
        MatchType.NO_MATCH,
        NoMatchStrategy.builder()
            .ihConfig(ihConfig)
            .entityConverter(entityConverter)
            .syncResultBuilder(syncResultBuilder)
            .build());
    resultMatchStrategies.put(
        MatchType.INST_AND_COLL,
        InstitutionAndCollectionMatchStrategy.builder()
            .ihConfig(ihConfig)
            .entityConverter(entityConverter)
            .syncResultBuilder(syncResultBuilder)
            .build());
    resultMatchStrategies.put(
        MatchType.CONFLICT,
        ConflictStrategy.builder().ihConfig(ihConfig).syncResultBuilder(syncResultBuilder).build());
  }
}
