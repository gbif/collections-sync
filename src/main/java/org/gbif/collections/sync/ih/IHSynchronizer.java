package org.gbif.collections.sync.ih;

import java.util.stream.Collectors;
import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.clients.proxy.IHProxyClient;
import org.gbif.collections.sync.common.BaseSynchronizer;
import org.gbif.collections.sync.common.DataLoader;
import org.gbif.collections.sync.common.parsers.CountryParser;
import org.gbif.collections.sync.common.parsers.DataParser;
import org.gbif.collections.sync.config.IHConfig;
import org.gbif.collections.sync.ih.IHDataLoader.IHData;
import org.gbif.collections.sync.ih.match.IHMatchResult;
import org.gbif.collections.sync.ih.match.IHStaffMatchResultHandler;
import org.gbif.collections.sync.ih.match.Matcher;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Strings;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.collections.sync.common.parsers.DataParser.parseStringList;

@Slf4j
public class IHSynchronizer extends BaseSynchronizer<IHInstitution, IHStaff> {

  private final IHIssueNotifier issueNotifier;
  private final IHProxyClient ihProxyClient;
  private final List<String> skippedEntries;

  private IHSynchronizer(
      IHProxyClient proxyClient,
      IHStaffMatchResultHandler staffResultHandler,
      IHEntityConverter entityConverter) {
    super(proxyClient, staffResultHandler, entityConverter);
    this.ihProxyClient = proxyClient;
    this.issueNotifier = IHIssueNotifier.getInstance(proxyClient.getIhConfig());
    skippedEntries = proxyClient.getIhConfig().getIhSkippedEntries();
  }

  @Builder
  public static IHSynchronizer create(IHConfig ihConfig, DataLoader<IHData> dataLoader) {
    if (dataLoader == null) {
      dataLoader = IHDataLoader.create(ihConfig);
    }

    IHProxyClient proxyClient =
        IHProxyClient.builder().dataLoader(dataLoader).ihConfig(ihConfig).build();
    IHEntityConverter entityConverter =
        IHEntityConverter.create(
            CountryParser.from(proxyClient.getCountries()), IHIssueNotifier.getInstance(ihConfig));
    IHStaffMatchResultHandler staffMatchResultHandler =
        new IHStaffMatchResultHandler(ihConfig, proxyClient, entityConverter);

    return new IHSynchronizer(proxyClient, staffMatchResultHandler, entityConverter);
  }

  public SyncResult sync() {
    Matcher matcher = Matcher.create(ihProxyClient);
    SyncResult.SyncResultBuilder resultBuilder = SyncResult.builder();

    detectDeletedIHInstitutions();

    // do the sync
    log.info("Starting the sync");
    ihProxyClient.getIhInstitutions().stream()
        .filter(i -> !skippedEntries.contains(i.getIrn()))
        .forEach(
            ihInstitution -> {
              if (!isValidIhInstitution(ihInstitution, issueNotifier)) {
                resultBuilder.invalidEntity(ihInstitution);
                return;
              }

              handleInvalidEmails(ihInstitution,issueNotifier);
              IHMatchResult match = matcher.match(ihInstitution);
              handleResult(match, resultBuilder);
            });

    SyncResult result = resultBuilder.build();

    // create a notification with all the fails
    if (!result.getFailedActions().isEmpty()) {
      log.info("Creating fails notifications");
      issueNotifier.createFailsNotification(result.getFailedActions());
    }

    return result;
  }

  private void detectDeletedIHInstitutions() {
    Map<String, Set<CollectionEntity>> deletedEntities = new HashMap<>();

    ihProxyClient
        .getInstitutionsMapByIrn()
        .forEach(
            (k, v) -> {
              if (!ihProxyClient.getIhInstitutionsMapByIrn().containsKey(k)) {
                // IH institution not found
                deletedEntities.computeIfAbsent(k, val -> new HashSet<>()).addAll(v);
              }
            });

    ihProxyClient
        .getCollectionsMapByIrn()
        .forEach(
            (k, v) -> {
              if (!ihProxyClient.getIhInstitutionsMapByIrn().containsKey(k)) {
                // IH institution not found
                deletedEntities.computeIfAbsent(k, val -> new HashSet<>()).addAll(v);
              }
            });

    deletedEntities.forEach((k, v) -> issueNotifier.createIHDeletedEntityIssue(v, k));
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
      syncResultBuilder.conflict(handleConflict(matchResult));
    }
  }

  private void handleInvalidEmails(IHInstitution ihInstitution, IHIssueNotifier issueNotifier) {
    if (ihInstitution.getContact() != null && !Strings.isNullOrEmpty(ihInstitution.getContact().getEmail())) {
      List<String> emails = parseStringList(ihInstitution.getContact().getEmail());
      List<String> validEmails = emails.stream()
          .filter(DataParser::isValidEmail)
          .collect(Collectors.toList());

      // If there are no valid emails, notify and set email to null
      if (validEmails.isEmpty()) {
        issueNotifier.createInvalidEntity(ihInstitution, "Not valid institution - emails are not valid");
        ihInstitution.getContact().setEmail(null);
      } else {
        // Set only valid emails back
        ihInstitution.getContact().setEmail(String.join(",", validEmails));
      }
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
