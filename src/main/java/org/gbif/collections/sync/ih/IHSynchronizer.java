package org.gbif.collections.sync.ih;

import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.SyncResult.Conflict;
import org.gbif.collections.sync.clients.proxy.IHProxyClient;
import org.gbif.collections.sync.common.DataLoader;
import org.gbif.collections.sync.common.BaseSynchronizer;
import org.gbif.collections.sync.common.parsers.CountryParser;
import org.gbif.collections.sync.config.IHConfig;
import org.gbif.collections.sync.ih.match.IHMatchResult;
import org.gbif.collections.sync.ih.match.IHStaffMatchResultHandler;
import org.gbif.collections.sync.ih.match.Matcher;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;

import com.google.common.base.Strings;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IHSynchronizer extends BaseSynchronizer<IHInstitution, IHStaff> {

  private final IHIssueNotifier issueNotifier;
  private final IHProxyClient proxyClient;

  private IHSynchronizer(
      IHProxyClient proxyClient,
      IHStaffMatchResultHandler staffResultHandler,
      IHEntityConverter entityConverter) {
    super(proxyClient, staffResultHandler, entityConverter);
    this.proxyClient = proxyClient;
    this.issueNotifier = IHIssueNotifier.create(proxyClient.getIhConfig());
  }

  @Builder
  public static IHSynchronizer create(IHConfig ihConfig, DataLoader dataLoader) {
    if (dataLoader == null) {
      dataLoader = DataLoader.create(ihConfig.getSyncConfig());
    }

    IHProxyClient proxyClient =
        IHProxyClient.builder().dataLoader(dataLoader).ihConfig(ihConfig).build();

    IHEntityConverter entityConverter =
        IHEntityConverter.create(CountryParser.from(proxyClient.getCountries()));
    IHStaffMatchResultHandler staffMatchResultHandler =
        new IHStaffMatchResultHandler(ihConfig, proxyClient, entityConverter);

    return new IHSynchronizer(proxyClient, staffMatchResultHandler, entityConverter);
  }

  public SyncResult sync() {
    Matcher matcher = Matcher.create(proxyClient);
    SyncResult.SyncResultBuilder resultBuilder = SyncResult.builder();

    // do the sync
    log.info("Starting the sync");
    proxyClient
        .getIhInstitutions()
        .forEach(
            ihInstitution -> {
              if (!isValidIhInstitution(ihInstitution, issueNotifier)) {
                resultBuilder.invalidEntity(ihInstitution);
                return;
              }

              IHMatchResult match = matcher.match(ihInstitution);
              handleResult(match, resultBuilder);
            });

    SyncResult result = resultBuilder.build();

    // create a notification with all the fails
    if (!result.getFailedActions().isEmpty()) {
      issueNotifier.createFailsNotification(result.getFailedActions());
    }

    return result;
  }

  private void handleResult(
      IHMatchResult matchResult, SyncResult.SyncResultBuilder syncResultBuilder) {
    if (matchResult.onlyOneCollectionMatch()) {
      syncResultBuilder.collectionOnlyMatch(handleCollectionMatch(matchResult));
    } else if (matchResult.onlyOneInstitutionMatch()) {
      syncResultBuilder.institutionOnlyMatch(handleInstitutionMatch(matchResult));
    } else if (matchResult.noMatches()) {
      syncResultBuilder.noMatch(handleNoMatch(matchResult));
    } else if (matchResult.institutionAndCollectionMatch()) {
      syncResultBuilder.instAndCollMatch(handleInstAndCollMatch(matchResult));
    } else {
      issueNotifier.createConflict(matchResult.getAllMatches(), matchResult.getSource());
      syncResultBuilder.conflict(
          new Conflict(matchResult.getSource(), matchResult.getAllMatches()));
    }
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
