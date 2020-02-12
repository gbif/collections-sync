package org.gbif.collections.sync.ih;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.SyncConfig;
import org.gbif.collections.sync.http.clients.GithubClient;
import org.gbif.collections.sync.http.clients.GrSciCollHttpClient;
import org.gbif.collections.sync.http.clients.IHHttpClient;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;
import org.gbif.collections.sync.notification.Issue;
import org.gbif.collections.sync.notification.IssueFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.collections.sync.ih.IHSyncResult.*;
import static org.gbif.collections.sync.ih.Matcher.Match;

@Slf4j
public class IHSync {

  private final boolean dryRun;
  private final boolean sendNotifications;
  private final EntityConverter entityConverter;
  private final IssueFactory issueFactory;
  private final GrSciCollHttpClient grSciCollHttpClient;
  private final IHHttpClient ihHttpClient;
  private final GithubClient githubClient;
  private IHSyncResult.IHSyncResultBuilder syncResultBuilder = IHSyncResult.builder();

  @Builder
  private IHSync(SyncConfig config, EntityConverter entityConverter) {
    if (entityConverter == null) {
      this.entityConverter =
          EntityConverter.builder()
              .countries(IHHttpClient.getInstance(config.getIhWsUrl()).getCountries())
              .creationUser(config.getRegistryWsUser())
              .build();
    } else {
      this.entityConverter = entityConverter;
    }

    if (config != null) {
      this.dryRun = config.isDryRun();
      this.sendNotifications = config.isSendNotifications();
      this.issueFactory = IssueFactory.getInstance(config);
      this.grSciCollHttpClient = GrSciCollHttpClient.getInstance(config);
      this.ihHttpClient = IHHttpClient.getInstance(config.getIhWsUrl());
      this.githubClient = GithubClient.getInstance(config);
    } else {
      this.dryRun = false;
      this.sendNotifications = false;
      this.issueFactory = IssueFactory.fromDefaults();
      this.grSciCollHttpClient = null;
      this.ihHttpClient = null;
      this.githubClient = null;
    }
  }

  public IHSyncResult sync() {
    Objects.requireNonNull(ihHttpClient);
    Objects.requireNonNull(grSciCollHttpClient);

    // load the data from the WS
    CompletableFuture<List<IHInstitution>> ihInstitutionsFuture =
        CompletableFuture.supplyAsync(ihHttpClient::getInstitutions);
    CompletableFuture<List<IHStaff>> ihStaffFuture =
        CompletableFuture.supplyAsync(ihHttpClient::getStaff);
    CompletableFuture<List<Institution>> institutionsFuture =
        CompletableFuture.supplyAsync(grSciCollHttpClient::getInstitutions);
    CompletableFuture<List<Collection>> collectionsFuture =
        CompletableFuture.supplyAsync(grSciCollHttpClient::getCollections);
    CompletableFuture<List<Person>> personsFuture =
        CompletableFuture.supplyAsync(grSciCollHttpClient::getPersons);

    log.info("Loading data from WSs");
    CompletableFuture.allOf(
            ihInstitutionsFuture,
            ihStaffFuture,
            institutionsFuture,
            collectionsFuture,
            personsFuture)
        .join();

    List<IHInstitution> ihInstitutions = ihInstitutionsFuture.join();
    Matcher matcher =
        Matcher.builder()
            .allGrSciCollPersons(personsFuture.join())
            .collections(collectionsFuture.join())
            .entityConverter(entityConverter)
            .ihStaff(ihStaffFuture.join())
            .institutions(institutionsFuture.join())
            .build();

    // do the sync
    log.info("Starting the sync");
    this.syncResultBuilder = IHSyncResult.builder();
    ihInstitutions.forEach(
        ihInstitution -> {
          Match match = matcher.match(ihInstitution);

          if (match.onlyOneCollectionMatch()) {
            log.info("Only one collection match for IH institution {}", ihInstitution.getIrn());
            syncResultBuilder.collectionOnlyMatch(handleCollectionMatch(match));
          } else if (match.onlyOneInstitutionMatch()) {
            log.info("Only one institution match for IH institution {}", ihInstitution.getIrn());
            syncResultBuilder.institutionOnlyMatch(handleInstitutionMatch(match));
          } else if (match.noMatches()) {
            log.info("No match for IH institution {}", ihInstitution.getIrn());
            syncResultBuilder.noMatch(handleNoMatches(match));
          } else if (match.institutionAndCollectionMatch()) {
            log.info(
                "One collection and one institution match for IH institution {}",
                ihInstitution.getIrn());
            syncResultBuilder.instAndCollMatch(handleInstitutionAndCollectionMatch(match));
          } else {
            log.info("Conflict for IH institution {}", ihInstitution.getIrn());
            syncResultBuilder.conflict(handleConflict(match));
          }
        });

    return syncResultBuilder.build();
  }

  @VisibleForTesting
  CollectionOnlyMatch handleCollectionMatch(Match match) {
    Collection existing = match.collections.iterator().next();

    Collection mergedCollection =
        entityConverter.convertToCollection(match.ihInstitution, existing);

    EntityMatch.EntityMatchBuilder<Collection> entityMatchBuilder =
        EntityMatch.<Collection>builder().matched(existing).merged(mergedCollection);
    if (!mergedCollection.lenientEquals(existing)) {
      executeOrAddFail(
          () -> grSciCollHttpClient.updateCollection(mergedCollection),
          e ->
              new FailedAction(mergedCollection, "Failed to update collection: " + e.getMessage()));
      entityMatchBuilder.update(true);
    }

    StaffMatch staffMatch =
        handleStaff(match, existing.getContacts(), Collections.singletonList(mergedCollection));

    return CollectionOnlyMatch.builder()
        .matchedCollection(entityMatchBuilder.build())
        .staffMatch(staffMatch)
        .build();
  }

  @VisibleForTesting
  InstitutionOnlyMatch handleInstitutionMatch(Match match) {
    Institution existing = match.institutions.iterator().next();

    Institution mergedInstitution =
        entityConverter.convertToInstitution(match.ihInstitution, existing);

    EntityMatch.EntityMatchBuilder<Institution> entityMatchBuilder =
        EntityMatch.<Institution>builder().matched(existing).merged(mergedInstitution);
    if (!mergedInstitution.lenientEquals(existing)) {
      executeOrAddFail(
          () -> grSciCollHttpClient.updateInstitution(mergedInstitution),
          e ->
              new FailedAction(
                  mergedInstitution, "Failed to update institution: " + e.getMessage()));
      entityMatchBuilder.update(true);
    }

    // create new collection and link it
    Collection newCollection = entityConverter.convertToCollection(match.ihInstitution);
    executeOrAddFail(
        () -> grSciCollHttpClient.createCollection(newCollection),
        e -> new FailedAction(mergedInstitution, "Failed to create collection: " + e.getMessage()));

    // same staff for both entities
    StaffMatch staffMatch =
        handleStaff(match, existing.getContacts(), Arrays.asList(mergedInstitution, newCollection));

    return InstitutionOnlyMatch.builder()
        .matchedInstitution(entityMatchBuilder.build())
        .newCollection(newCollection)
        .staffMatch(staffMatch)
        .build();
  }

  @VisibleForTesting
  NoEntityMatch handleNoMatches(Match match) {
    // create institution
    Institution newInstitution = entityConverter.convertToInstitution(match.ihInstitution);
    executeOrAddFail(
        () -> grSciCollHttpClient.createInstitution(newInstitution),
        e -> new FailedAction(newInstitution, "Failed to create institution: " + e.getMessage()));

    // create collection
    Collection newCollection = entityConverter.convertToCollection(match.ihInstitution);
    executeOrAddFail(
        () -> grSciCollHttpClient.createCollection(newCollection),
        e -> new FailedAction(newCollection, "Failed to create collection: " + e.getMessage()));

    // same staff for both entities
    StaffMatch staffMatch =
        handleStaff(match, Collections.emptyList(), Arrays.asList(newInstitution, newCollection));

    return NoEntityMatch.builder()
        .newCollection(newCollection)
        .newInstitution(newInstitution)
        .staffMatch(staffMatch)
        .build();
  }

  @VisibleForTesting
  InstitutionAndCollectionMatch handleInstitutionAndCollectionMatch(Match match) {
    CollectionOnlyMatch colMatch = handleCollectionMatch(match);
    InstitutionOnlyMatch instMatch = handleInstitutionMatch(match);

    return InstitutionAndCollectionMatch.builder()
        .matchedCollection(colMatch.getMatchedCollection())
        .staffMatchCollection(colMatch.getStaffMatch())
        .matchedInstitution(instMatch.getMatchedInstitution())
        .staffMatchInstitution(instMatch.getStaffMatch())
        .build();
  }

  @VisibleForTesting
  <T extends CollectionEntity> StaffMatch handleStaff(
      Match match, List<Person> contacts, List<T> entities) {

    BiConsumer<T, Person> addPersonToEntity =
        (e, p) -> {
          if (e instanceof Collection) {
            grSciCollHttpClient.addPersonToCollection(p.getKey(), e.getKey());
          } else if (e instanceof Institution) {
            grSciCollHttpClient.addPersonToInstitution(p.getKey(), e.getKey());
          }
        };

    List<Person> contactsCopy = contacts != null ? new ArrayList<>(contacts) : new ArrayList<>();
    List<IHStaff> ihStaffList = match.ihStaff != null ? match.ihStaff : Collections.emptyList();
    StaffMatch.StaffMatchBuilder staffMatchBuilder = StaffMatch.builder();
    for (IHStaff ihStaff : ihStaffList) {
      Set<Person> matches = match.staffMatcher.apply(ihStaff, contacts);

      if (matches.isEmpty()) {
        // create person and link it
        log.info("No match for IH Staff {}", ihStaff.getIrn());
        Person newPerson = entityConverter.convertToPerson(ihStaff);
        executeOrAddFail(
            () -> {
              grSciCollHttpClient.createPerson(newPerson);
              entities.forEach(e -> addPersonToEntity.accept(e, newPerson));
            },
            e -> new FailedAction(newPerson, "Failed to create person: " + e.getMessage()));
        staffMatchBuilder.newPerson(newPerson);
      } else if (matches.size() > 1) {
        // conflict
        log.info("Conflict for IH Staff {}", ihStaff.getIrn());
        contactsCopy.removeAll(matches);
        createGHIssue(issueFactory.createStaffConflict(matches, ihStaff));
        staffMatchBuilder.conflict(new IHSyncResult.Conflict(ihStaff, new ArrayList<>(matches)));
      } else {
        // there is one match
        log.info("One match for IH Staff {}", ihStaff.getIrn());
        Person matchedPerson = matches.iterator().next();
        contactsCopy.remove(matchedPerson);
        Person mergedPerson = entityConverter.convertToPerson(ihStaff, matchedPerson);

        EntityMatch.EntityMatchBuilder<Person> entityMatchBuilder =
            EntityMatch.<Person>builder().matched(matchedPerson).merged(mergedPerson);
        if (!mergedPerson.lenientEquals(matchedPerson)) {
          executeOrAddFail(
              () -> {
                grSciCollHttpClient.updatePerson(mergedPerson);
                // add identifiers if needed
                mergedPerson.getIdentifiers().stream()
                    .filter(i -> i.getKey() == null)
                    .forEach(
                        i -> grSciCollHttpClient.addIdentifierToPerson(mergedPerson.getKey(), i));
              },
              e -> new FailedAction(mergedPerson, "Failed to update person: " + e.getMessage()));
          entityMatchBuilder.update(true);
        }
        staffMatchBuilder.matchedPerson(entityMatchBuilder.build());
      }
    }

    // remove contacts
    BiConsumer<T, Person> removePersonFromEntity =
        (e, p) -> {
          if (e instanceof Collection) {
            grSciCollHttpClient.removePersonFromCollection(p.getKey(), e.getKey());
          } else if (e instanceof Institution) {
            grSciCollHttpClient.removePersonFromInstitution(p.getKey(), e.getKey());
          }
        };

    contactsCopy.forEach(
        personToRemove -> {
          log.info("Removing contact {}", personToRemove.getKey());
          executeOrAddFail(
              () -> entities.forEach(e -> removePersonFromEntity.accept(e, personToRemove)),
              e -> new FailedAction(personToRemove, "Failed to remove person: " + e.getMessage()));
          staffMatchBuilder.removedPerson(personToRemove);
        });

    return staffMatchBuilder.build();
  }

  @VisibleForTesting
  Conflict handleConflict(Match match) {
    createGHIssue(issueFactory.createConflict(match.getAllMatches(), match.ihInstitution));
    return new Conflict(match.ihInstitution, match.getAllMatches());
  }

  private void executeOrAddFail(Runnable runnable, Function<Exception, FailedAction> failCreator) {
    if (!dryRun) {
      try {
        runnable.run();
      } catch (Exception e) {
        syncResultBuilder.failedAction(failCreator.apply(e));
      }
    }
  }

  private void createGHIssue(Issue issue) {
    if (sendNotifications) {
      try {
        githubClient.createIssue(issue);
      } catch (Exception e) {
        syncResultBuilder.failedAction(
            new FailedAction(issue, "Failed to create issue: " + e.getMessage()));
      }
    }
  }
}