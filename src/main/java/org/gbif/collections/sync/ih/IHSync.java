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

/** Syncs IH entities with GrSciColl ones present in GBIF registry. */
@Slf4j
public class IHSync {

  private final boolean dryRun;
  private final boolean sendNotifications;
  private final EntityConverter entityConverter;
  private final IssueFactory issueFactory;
  private final GrSciCollHttpClient grSciCollHttpClient;
  private final IHHttpClient ihHttpClient;
  private final GithubClient githubClient;
  private IHSyncResultBuilder syncResultBuilder = IHSyncResult.builder();

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
            .allGrSciCollPersons(new HashSet<>(personsFuture.join()))
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
        handleStaffForSameEntity(
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
    if (isInvalidCollection(newCollection)) {
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
        handleStaffForSameEntity(
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
    if (isInvalidInstitution(newInstitution)) {
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
    if (isInvalidCollection(newCollection)) {
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
        handleStaffForSameEntity(
            match, Collections.emptyList(), Arrays.asList(newInstitution, newCollection));

    return Optional.of(
        NoEntityMatch.builder()
            .newCollection(newCollection)
            .newInstitution(newInstitution)
            .staffMatch(staffMatch)
            .build());
  }

  @VisibleForTesting
  <T extends CollectionEntity> InstitutionAndCollectionMatch handleInstitutionAndCollectionMatch(
      Match match) {
    // first we see if we need to update any of the entities
    EntityMatch<Institution> institutionEntityMatch = updateInstitution(match);
    EntityMatch<Collection> collectionEntityMatch = updateCollection(match);

    Institution institution = institutionEntityMatch.getMerged();
    Collection collection = collectionEntityMatch.getMerged();

    // then we handle the staff of both entities at the same time to avoid creating duplicates
    StaffMatch staffMatch =
        handleStaffFromDifferentEntities(
            match,
            collection.getContacts(),
            institution.getContacts(),
            Arrays.asList(institution, collection));

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

  /**
   * This method handles the staff for entities that have the same contacts. Therefore, the sync is
   * the same for both.
   *
   * <p>This happens when we're creating a new instituion and collection at once or when we're just
   * updating one entity (one institution or one collection).
   */
  @VisibleForTesting
  <T extends CollectionEntity> StaffMatch handleStaffForSameEntity(
      Match match, List<Person> contacts, List<T> entities) {

    BiConsumer<T, Person> addPersonToEntity =
        (e, p) -> {
          // they can be null in dry runs or if the creation of a collection/institution fails
          if (p == null || p.getKey() == null || e.getKey() == null || contacts.contains(p)) {
            return;
          }

          if (e instanceof Collection) {
            grSciCollHttpClient.addPersonToCollection(p.getKey(), e.getKey());
          } else if (e instanceof Institution) {
            grSciCollHttpClient.addPersonToInstitution(p.getKey(), e.getKey());
          }
        };

    BiConsumer<T, Person> removePersonFromEntity =
        (e, p) -> {
          // they can be null in dry runs or if the creation of a collection/institution fails
          if (p == null || p.getKey() == null || e.getKey() == null || !contacts.contains(p)) {
            return;
          }

          if (e instanceof Collection) {
            grSciCollHttpClient.removePersonFromCollection(p.getKey(), e.getKey());
          } else if (e instanceof Institution) {
            grSciCollHttpClient.removePersonFromInstitution(p.getKey(), e.getKey());
          }
        };

    return handleStaff(
        match, new HashSet<>(contacts), entities, addPersonToEntity, removePersonFromEntity);
  }

  /**
   * This method handles the staff of 2 entities which initially have different staff but will have
   * the same after the sync.
   *
   * <p>This happens when a IH institution matches to an institution and a collection. Both are
   * syncing to the same IH insitution but their initial contacts can be different.
   */
  @VisibleForTesting
  <T extends CollectionEntity> StaffMatch handleStaffFromDifferentEntities(
      Match match,
      List<Person> collectionContacts,
      List<Person> institutionContacts,
      List<T> entities) {

    BiConsumer<T, Person> addPersonToEntity =
        (e, p) -> {
          // they can be null in dry runs or if the creation of a collection/institution fails
          if (p == null || p.getKey() == null || e.getKey() == null) {
            return;
          }

          if (e instanceof Collection && !collectionContacts.contains(p)) {
            grSciCollHttpClient.addPersonToCollection(p.getKey(), e.getKey());
          } else if (e instanceof Institution && !institutionContacts.contains(p)) {
            grSciCollHttpClient.addPersonToInstitution(p.getKey(), e.getKey());
          }
        };

    BiConsumer<T, Person> removePersonFromEntity =
        (e, p) -> {
          // they can be null in dry runs or if the creation of a collection/institution fails
          if (p == null || p.getKey() == null || e.getKey() == null) {
            return;
          }

          if (e instanceof Collection && collectionContacts.contains(p)) {
            grSciCollHttpClient.removePersonFromCollection(p.getKey(), e.getKey());
          } else if (e instanceof Institution && institutionContacts.contains(p)) {
            grSciCollHttpClient.removePersonFromInstitution(p.getKey(), e.getKey());
          }
        };

    Set<Person> allContacts = new HashSet<>();
    if (collectionContacts != null) {
      allContacts.addAll(collectionContacts);
    }
    if (institutionContacts != null) {
      allContacts.addAll(institutionContacts);
    }

    return handleStaff(match, allContacts, entities, addPersonToEntity, removePersonFromEntity);
  }

  private <T extends CollectionEntity> StaffMatch handleStaff(
      Match match,
      Set<Person> contacts,
      List<T> entities,
      BiConsumer<T, Person> addPersonToEntity,
      BiConsumer<T, Person> removePersonFromEntity) {

    List<Person> contactsCopy = contacts != null ? new ArrayList<>(contacts) : new ArrayList<>();
    List<IHStaff> ihStaffList = match.ihStaff != null ? match.ihStaff : Collections.emptyList();
    StaffMatch.StaffMatchBuilder staffSyncBuilder = StaffMatch.builder();
    for (IHStaff ihStaff : ihStaffList) {
      Set<Person> staffMatches = match.staffMatcher.apply(ihStaff, contacts);

      if (staffMatches.isEmpty()) {
        // create person and link it to the entity
        log.info("No match for IH Staff {}", ihStaff.getIrn());
        Person newPerson = entityConverter.convertToPerson(ihStaff);
        if (isInvalidPerson(newPerson)) {
          createGHIssue(
              issueFactory.createInvalidEntity(
                  ihStaff, "Not valid person - first name is required"));
          continue;
        }

        executeOrAddFailAsync(
            () -> {
              UUID createdKey = grSciCollHttpClient.createPerson(newPerson);
              newPerson.setKey(createdKey);
              entities.forEach(e -> addPersonToEntity.accept(e, newPerson));
            },
            e -> new FailedAction(newPerson, "Failed to create person: " + e.getMessage()));
        staffSyncBuilder.newPerson(newPerson);
      } else if (staffMatches.size() > 1) {
        // conflict. Multiple candidates matched
        log.info("Conflict for IH Staff {}", ihStaff.getIrn());
        contactsCopy.removeAll(staffMatches);
        createGHIssue(issueFactory.createStaffConflict(staffMatches, ihStaff));
        staffSyncBuilder.conflict(new Conflict(ihStaff, new ArrayList<>(staffMatches)));
      } else {
        // there is one match
        log.info("One match for IH Staff {}", ihStaff.getIrn());
        Person matchedPerson = staffMatches.iterator().next();
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
                // if the match was global we'd need to link it to the entity. The same if we're
                // syncing staff from different entities: one entity could have the contact already
                // but not the other
                entities.forEach(e -> addPersonToEntity.accept(e, mergedPerson));
              },
              e -> new FailedAction(mergedPerson, "Failed to update person: " + e.getMessage()));
          entityMatchBuilder.update(true);
        }

        staffSyncBuilder.matchedPerson(entityMatchBuilder.build());
      }
    }

    contactsCopy.forEach(
        personToRemove -> {
          log.info("Removing contact {}", personToRemove.getKey());
          executeOrAddFailAsync(
              () -> entities.forEach(e -> removePersonFromEntity.accept(e, personToRemove)),
              e -> new FailedAction(personToRemove, "Failed to remove person: " + e.getMessage()));
          staffSyncBuilder.removedPerson(personToRemove);
        });

    return staffSyncBuilder.build();
  }

  @VisibleForTesting
  Conflict handleConflict(Match match) {
    createGHIssue(issueFactory.createConflict(match.getAllMatches(), match.ihInstitution));
    return new Conflict(match.ihInstitution, match.getAllMatches());
  }

  private static boolean isInvalidCollection(Collection collection) {
    return Strings.isNullOrEmpty(collection.getName());
  }

  private static boolean isInvalidInstitution(Institution institution) {
    return Strings.isNullOrEmpty(institution.getName());
  }

  private static boolean isInvalidPerson(Person person) {
    return Strings.isNullOrEmpty(person.getFirstName());
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
