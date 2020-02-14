package org.gbif.collections.sync.ih;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.SyncConfig;
import org.gbif.collections.sync.http.clients.GithubClient;
import org.gbif.collections.sync.http.clients.GrSciCollHttpClient;
import org.gbif.collections.sync.http.clients.IHHttpClient;
import org.gbif.collections.sync.ih.IHSyncResult.*;
import org.gbif.collections.sync.ih.Matcher.Match;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;
import org.gbif.collections.sync.notification.Issue;
import org.gbif.collections.sync.notification.IssueFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

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
            handleInstitutionMatch(match).ifPresent(syncResultBuilder::institutionOnlyMatch);
          } else if (match.noMatches()) {
            log.info("No match for IH institution {}", ihInstitution.getIrn());
            handleNoMatches(match).ifPresent(syncResultBuilder::noMatch);
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

    IHSyncResult result = syncResultBuilder.build();

    // create a notification with all the fails
    if (!result.getFailedActions().isEmpty()) {
      createGHIssue(issueFactory.createFailsNotification(result.getFailedActions()));
    }

    return result;
  }

  @VisibleForTesting
  CollectionOnlyMatch handleCollectionMatch(Match match) {
    EntityMatch<Collection> collectionEntityMatch = updateCollection(match);

    StaffMatch staffMatch =
        handleStaff(
            match,
            collectionEntityMatch.getMatched().getContacts(),
            Collections.singletonList(collectionEntityMatch.getMerged()));

    return CollectionOnlyMatch.builder()
        .matchedCollection(collectionEntityMatch)
        .staffMatch(staffMatch)
        .build();
  }

  @VisibleForTesting
  Optional<InstitutionOnlyMatch> handleInstitutionMatch(Match match) {
    EntityMatch<Institution> institutionEntityMatch = updateInstitution(match);

    // create new collection linked to the institution
    Collection newCollection =
        entityConverter.convertToCollection(
            match.ihInstitution, institutionEntityMatch.getMatched().getKey());
    if (!isValidCollection(newCollection)) {
      createGHIssue(issueFactory.createInvalidEntity(match.ihInstitution, "Not valid institution"));
      return Optional.empty();
    }

    UUID createdKey =
        executeCreateEntityOrAddFail(
            () -> grSciCollHttpClient.createCollection(newCollection),
            e -> new FailedAction(newCollection, "Failed to create collection: " + e.getMessage()));
    newCollection.setKey(createdKey);

    // same staff for both entities
    StaffMatch staffMatch =
        handleStaff(
            match,
            institutionEntityMatch.getMatched().getContacts(),
            Arrays.asList(institutionEntityMatch.getMerged(), newCollection));

    return Optional.of(
        InstitutionOnlyMatch.builder()
            .matchedInstitution(institutionEntityMatch)
            .newCollection(newCollection)
            .staffMatch(staffMatch)
            .build());
  }

  @VisibleForTesting
  Optional<NoEntityMatch> handleNoMatches(Match match) {
    // create institution
    Institution newInstitution = entityConverter.convertToInstitution(match.ihInstitution);
    if (!isValidInstitution(newInstitution)) {
      createGHIssue(
          issueFactory.createInvalidEntity(
              match.ihInstitution, "Not valid institution - name is required"));
      return Optional.empty();
    }

    UUID institutionKey =
        executeCreateEntityOrAddFail(
            () -> grSciCollHttpClient.createInstitution(newInstitution),
            e ->
                new FailedAction(
                    newInstitution, "Failed to create institution : " + e.getMessage()));
    newInstitution.setKey(institutionKey);

    // create collection
    Collection newCollection =
        entityConverter.convertToCollection(match.ihInstitution, institutionKey);
    if (!isValidCollection(newCollection)) {
      createGHIssue(
          issueFactory.createInvalidEntity(
              match.ihInstitution, "Not valid institution - name is required"));
      return Optional.empty();
    }

    UUID collectionKey =
        executeCreateEntityOrAddFail(
            () -> grSciCollHttpClient.createCollection(newCollection),
            e ->
                new FailedAction(
                    newCollection,
                    "Failed to create institution and collection: " + e.getMessage()));
    newCollection.setKey(collectionKey);

    // same staff for both entities
    StaffMatch staffMatch =
        handleStaff(match, Collections.emptyList(), Arrays.asList(newInstitution, newCollection));

    return Optional.of(
        NoEntityMatch.builder()
            .newCollection(newCollection)
            .newInstitution(newInstitution)
            .staffMatch(staffMatch)
            .build());
  }

  @VisibleForTesting
  InstitutionAndCollectionMatch handleInstitutionAndCollectionMatch(Match match) {
    EntityMatch<Institution> institutionEntityMatch = updateInstitution(match);
    EntityMatch<Collection> collectionEntityMatch = updateCollection(match);

    Institution institution = institutionEntityMatch.getMerged();
    Collection collection = collectionEntityMatch.getMerged();

    // look for common contacts not to handle the same staff twice
    List<Person> commonContacts = new ArrayList<>();
    List<Person> institutionOnlyContacts = new ArrayList<>();
    List<Person> collectionOnlyContacts =
        collection.getContacts() != null ? collection.getContacts() : new ArrayList<>();
    if (institution.getContacts() != null) {
      for (Person person : institution.getContacts()) {
        if (collectionOnlyContacts.contains(person)) {
          commonContacts.add(person);
          collectionOnlyContacts.remove(person);
        } else {
          institutionOnlyContacts.add(person);
        }
      }
    }

    // handle staff
    StaffMatch staffMatch = StaffMatch.builder().build();
    if (!commonContacts.isEmpty()) {
      staffMatch = handleStaff(match, commonContacts, Arrays.asList(institution, collection));
    }

    if (!institutionOnlyContacts.isEmpty()) {
      StaffMatch instContactsMatch =
          handleStaff(match, institutionOnlyContacts, Collections.singletonList(institution));
      staffMatch.getConflicts().addAll(instContactsMatch.getConflicts());
      staffMatch.getMatchedPersons().addAll(instContactsMatch.getMatchedPersons());
      staffMatch.getNewPersons().addAll(instContactsMatch.getNewPersons());
      staffMatch.getRemovedPersons().addAll(instContactsMatch.getRemovedPersons());
    }

    if (!collectionOnlyContacts.isEmpty()) {
      StaffMatch colContactsMatch =
          handleStaff(match, collectionOnlyContacts, Collections.singletonList(collection));
      staffMatch.getConflicts().addAll(colContactsMatch.getConflicts());
      staffMatch.getMatchedPersons().addAll(colContactsMatch.getMatchedPersons());
      staffMatch.getNewPersons().addAll(colContactsMatch.getNewPersons());
      staffMatch.getRemovedPersons().addAll(colContactsMatch.getRemovedPersons());
    }

    return InstitutionAndCollectionMatch.builder()
        .matchedInstitution(institutionEntityMatch)
        .matchedCollection(collectionEntityMatch)
        .staffMatch(staffMatch)
        .build();
  }

  private EntityMatch<Institution> updateInstitution(Match match) {
    Institution existing = match.institutions.iterator().next();

    Institution mergedInstitution =
        entityConverter.convertToInstitution(match.ihInstitution, existing);

    EntityMatch.EntityMatchBuilder<Institution> entityMatchBuilder =
        EntityMatch.<Institution>builder().matched(existing).merged(mergedInstitution);
    if (!mergedInstitution.lenientEquals(existing)) {
      executeOrAddFailAsync(
          () -> grSciCollHttpClient.updateInstitution(mergedInstitution),
          e ->
              new FailedAction(
                  mergedInstitution, "Failed to update institution: " + e.getMessage()));
      entityMatchBuilder.update(true);
    }

    return entityMatchBuilder.build();
  }

  private EntityMatch<Collection> updateCollection(Match match) {
    Collection existing = match.collections.iterator().next();

    Collection mergedCollection =
        entityConverter.convertToCollection(match.ihInstitution, existing);

    EntityMatch.EntityMatchBuilder<Collection> entityMatchBuilder =
        EntityMatch.<Collection>builder().matched(existing).merged(mergedCollection);
    if (!mergedCollection.lenientEquals(existing)) {
      executeOrAddFailAsync(
          () -> grSciCollHttpClient.updateCollection(mergedCollection),
          e ->
              new FailedAction(mergedCollection, "Failed to update collection: " + e.getMessage()));
      entityMatchBuilder.update(true);
    }

    return entityMatchBuilder.build();
  }

  @VisibleForTesting
  <T extends CollectionEntity> StaffMatch handleStaff(
      Match match, List<Person> contacts, List<T> entities) {

    BiConsumer<T, UUID> addPersonToEntity =
        (e, personKey) -> {
          // they can be null in dry runs or if the creation of a collection/institution fails
          if (personKey == null || e.getKey() == null) {
            return;
          }

          if (e instanceof Collection) {
            grSciCollHttpClient.addPersonToCollection(personKey, e.getKey());
          } else if (e instanceof Institution) {
            grSciCollHttpClient.addPersonToInstitution(personKey, e.getKey());
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
        if (!isValidPerson(newPerson)) {
          createGHIssue(
              issueFactory.createInvalidEntity(
                  ihStaff, "Not valid person - first name is required"));
          continue;
        }

        executeOrAddFailAsync(
            () -> {
              UUID createdKey = grSciCollHttpClient.createPerson(newPerson);
              entities.forEach(e -> addPersonToEntity.accept(e, createdKey));
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
          executeOrAddFailAsync(
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
          // they can be null in dry runs or if the creation of a collection/institution fails
          if (p.getKey() == null || e.getKey() == null) {
            return;
          }

          if (e instanceof Collection) {
            grSciCollHttpClient.removePersonFromCollection(p.getKey(), e.getKey());
          } else if (e instanceof Institution) {
            grSciCollHttpClient.removePersonFromInstitution(p.getKey(), e.getKey());
          }
        };

    contactsCopy.forEach(
        personToRemove -> {
          log.info("Removing contact {}", personToRemove.getKey());
          executeOrAddFailAsync(
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

  private static boolean isValidCollection(Collection collection) {
    return !Strings.isNullOrEmpty(collection.getName());
  }

  private static boolean isValidInstitution(Institution institution) {
    return !Strings.isNullOrEmpty(institution.getName());
  }

  private static boolean isValidPerson(Person person) {
    return !Strings.isNullOrEmpty(person.getFirstName());
  }

  private UUID executeCreateEntityOrAddFail(
      Supplier<UUID> execution, Function<Throwable, FailedAction> failCreator) {
    if (!dryRun) {
      try {
        return execution.get();
      } catch (Exception e) {
        syncResultBuilder.failedAction(failCreator.apply(e));
      }
    }

    return null;
  }

  private void executeOrAddFailAsync(
      Runnable runnable, Function<Throwable, FailedAction> failCreator) {
    if (!dryRun) {
      CompletableFuture.runAsync(runnable)
          .whenCompleteAsync(
              (r, e) -> {
                if (e != null) {
                  syncResultBuilder.failedAction(failCreator.apply(e));
                }
              });
    }
  }

  private void createGHIssue(Issue issue) {
    if (sendNotifications) {
      CompletableFuture.runAsync(() -> githubClient.createIssue(issue))
          .whenCompleteAsync(
              (r, e) -> {
                if (e != null) {
                  syncResultBuilder.failedAction(
                      new FailedAction(issue, "Failed to create issue: " + e.getMessage()));
                }
              });
    }
  }
}
