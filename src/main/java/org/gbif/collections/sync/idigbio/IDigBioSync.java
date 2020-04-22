package org.gbif.collections.sync.idigbio;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Contactable;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.api.model.registry.Identifiable;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.vocabulary.IdentifierType;
import org.gbif.collections.sync.SyncConfig;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.http.clients.GithubClient;
import org.gbif.collections.sync.http.clients.GrSciCollHttpClient;
import org.gbif.collections.sync.http.clients.IHHttpClient;
import org.gbif.collections.sync.idigbio.match.MatchResult;
import org.gbif.collections.sync.idigbio.match.Matcher;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.notification.IDigBioIssueFactory;
import org.gbif.collections.sync.notification.Issue;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.collections.sync.SyncResult.CollectionOnlyMatch;
import static org.gbif.collections.sync.SyncResult.Conflict;
import static org.gbif.collections.sync.SyncResult.EntityMatch;
import static org.gbif.collections.sync.SyncResult.FailedAction;
import static org.gbif.collections.sync.SyncResult.InstitutionAndCollectionMatch;
import static org.gbif.collections.sync.SyncResult.InstitutionOnlyMatch;
import static org.gbif.collections.sync.SyncResult.NoEntityMatch;
import static org.gbif.collections.sync.SyncResult.OutdatedEntity;
import static org.gbif.collections.sync.SyncResult.StaffMatch;
import static org.gbif.collections.sync.SyncResult.SyncResultBuilder;
import static org.gbif.collections.sync.Utils.containsIrnIdentifier;
import static org.gbif.collections.sync.Utils.decodeIRN;
import static org.gbif.collections.sync.Utils.isPersonInContacts;
import static org.gbif.collections.sync.parsers.DataParser.TO_LOCAL_DATE_TIME_UTC;
import static org.gbif.collections.sync.parsers.DataParser.cleanString;
import static org.gbif.collections.sync.parsers.DataParser.parseDate;

@Slf4j
public class IDigBioSync {

  private final boolean dryRun;
  private final boolean sendNotifications;
  private final SyncConfig.IDigBioConfig iDigBioConfig;
  private final GrSciCollHttpClient grSciCollHttpClient;
  private final IHHttpClient ihHttpClient;
  private final GithubClient githubClient;
  private final IDigBioIssueFactory issueFactory;
  private SyncResultBuilder syncResultBuilder;
  private Matcher matcher;
  private Set<Person> grscicollPersons;
  private Map<String, IHInstitution> ihInstitutionsByIrn;

  @Builder
  private IDigBioSync(SyncConfig config) {
    if (config != null) {
      this.dryRun = config.isDryRun();
      this.sendNotifications = config.isSendNotifications();
      this.grSciCollHttpClient = GrSciCollHttpClient.getInstance(config);
      this.ihHttpClient = IHHttpClient.getInstance(config.getIhConfig().getIhWsUrl());
      this.githubClient = GithubClient.getInstance(config);
      this.issueFactory = IDigBioIssueFactory.getInstance(config);
      this.iDigBioConfig = config.getIDigBioConfig();
    } else {
      this.dryRun = true;
      this.sendNotifications = false;
      this.grSciCollHttpClient = null;
      this.ihHttpClient = null;
      this.githubClient = null;
      this.issueFactory = IDigBioIssueFactory.fromDefaults();
      this.iDigBioConfig = null;
    }
  }

  public SyncResult sync() {
    CompletableFuture<List<Institution>> institutionsFuture =
        CompletableFuture.supplyAsync(grSciCollHttpClient::getInstitutions);
    CompletableFuture<List<Collection>> collectionsFuture =
        CompletableFuture.supplyAsync(grSciCollHttpClient::getCollections);
    CompletableFuture<List<Person>> personsFuture =
        CompletableFuture.supplyAsync(grSciCollHttpClient::getPersons);
    CompletableFuture<List<IHInstitution>> ihInstitutionFuture =
        CompletableFuture.supplyAsync(ihHttpClient::getInstitutions);

    log.info("Loading data from WSs");
    CompletableFuture.allOf(institutionsFuture, collectionsFuture, personsFuture).join();

    this.matcher =
        Matcher.builder()
            .institutions(institutionsFuture.join())
            .collections(collectionsFuture.join())
            .build();
    this.grscicollPersons = new HashSet<>(personsFuture.join());
    this.ihInstitutionsByIrn =
        ihInstitutionFuture.join().stream()
            .collect(Collectors.toMap(IHInstitution::getIrn, v -> v));
    this.syncResultBuilder = SyncResult.builder();

    List<IDigBioRecord> records = readIDigBioExport(iDigBioConfig);
    for (IDigBioRecord record : records) {
      if (!isValidIDigBioRecord(record)) {
        continue;
      }

      MatchResult match = matcher.match(record);
      if (match.onlyCollectionMatch()) {
        syncResultBuilder.collectionOnlyMatch(handleCollectionMatch(match));
      } else if (match.onlyInstitutionMatch()) {
        syncResultBuilder.institutionOnlyMatch(handleInstitutionMatch(match));
      } else if (match.noMatches()) {
        syncResultBuilder.noMatch(handleNoMatches(match));
      } else if (match.institutionAndCollectionMatch()) {
        syncResultBuilder.instAndCollMatch(handleInstitutionAndCollectionMatch(match));
      } else {
        syncResultBuilder.conflict(handleConflict(match));
      }
    }

    return syncResultBuilder.build();
  }

  private List<IDigBioRecord> readIDigBioExport(SyncConfig.IDigBioConfig config) {
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

  @VisibleForTesting
  CollectionOnlyMatch handleCollectionMatch(MatchResult match) {
    SyncResult.EntityMatch<Collection> collectionEntityMatch = updateCollection(match);

    StaffMatch staffMatch =
        handleStaff(match, Collections.singletonList(match.getCollectionMatched()));

    return CollectionOnlyMatch.builder()
        .matchedCollection(collectionEntityMatch)
        .staffMatch(staffMatch)
        .build();
  }

  @VisibleForTesting
  InstitutionOnlyMatch handleInstitutionMatch(MatchResult match) {
    EntityMatch<Institution> institutionEntityMatch = updateInstitution(match);

    // create new collection linked to the institution
    Collection newCollection =
        EntityConverter.convertToCollection(
            match.getIDigBioRecord(), institutionEntityMatch.getMatched().getKey());

    UUID createdKey =
        executeAndReturnOrAddFail(
            () -> grSciCollHttpClient.createCollection(newCollection),
            e -> new FailedAction(newCollection, "Failed to create collection: " + e.getMessage()));
    newCollection.setKey(createdKey);

    // same staff for both entities
    StaffMatch staffMatch =
        handleStaff(match, Arrays.asList(institutionEntityMatch.getMatched(), newCollection));

    return InstitutionOnlyMatch.builder()
        .matchedInstitution(institutionEntityMatch)
        .newCollection(newCollection)
        .staffMatch(staffMatch)
        .build();
  }

  @VisibleForTesting
  NoEntityMatch handleNoMatches(MatchResult match) {
    // create institution
    Institution newInstitution = EntityConverter.convertToInstitution(match.getIDigBioRecord());
    UUID institutionKey =
        executeAndReturnOrAddFail(
            () -> grSciCollHttpClient.createInstitution(newInstitution),
            e ->
                new FailedAction(
                    newInstitution, "Failed to create institution : " + e.getMessage()));
    newInstitution.setKey(institutionKey);

    // create collection
    Collection newCollection =
        EntityConverter.convertToCollection(match.getIDigBioRecord(), institutionKey);

    UUID collectionKey =
        executeAndReturnOrAddFail(
            () -> grSciCollHttpClient.createCollection(newCollection),
            e ->
                new FailedAction(
                    newCollection,
                    "Failed to create institution and collection: " + e.getMessage()));
    newCollection.setKey(collectionKey);

    // same staff for both entities
    StaffMatch staffMatch = handleStaff(match, Arrays.asList(newInstitution, newCollection));

    return NoEntityMatch.builder()
        .newCollection(newCollection)
        .newInstitution(newInstitution)
        .staffMatch(staffMatch)
        .build();
  }

  @VisibleForTesting
  InstitutionAndCollectionMatch handleInstitutionAndCollectionMatch(MatchResult match) {
    // first we see if we need to update any of the entities
    EntityMatch<Institution> institutionEntityMatch = updateInstitution(match);
    EntityMatch<Collection> collectionEntityMatch = updateCollection(match);

    Institution institution = institutionEntityMatch.getMerged();
    Collection collection = collectionEntityMatch.getMerged();

    // then we handle the staff of both entities at the same time to avoid creating duplicates
    StaffMatch staffMatch = handleStaff(match, Arrays.asList(institution, collection));

    return InstitutionAndCollectionMatch.builder()
        .matchedInstitution(institutionEntityMatch)
        .matchedCollection(collectionEntityMatch)
        .staffMatch(staffMatch)
        .build();
  }

  @VisibleForTesting
  Conflict handleConflict(MatchResult match) {
    createGHIssue(issueFactory.createConflict(match.getAllMatches(), match.getIDigBioRecord()));
    return new Conflict(match.getIDigBioRecord(), match.getAllMatches());
  }

  private EntityMatch<Institution> updateInstitution(MatchResult match) {
    checkOutdatedIHInstitution(match.getInstitutionMatched(), match.getIDigBioRecord());

    Institution mergedInstitution =
        EntityConverter.convertToInstitution(
            match.getInstitutionMatched(), match.getIDigBioRecord());

    EntityMatch.EntityMatchBuilder<Institution> entityMatchBuilder =
        EntityMatch.<Institution>builder()
            .matched(match.getInstitutionMatched())
            .merged(mergedInstitution);
    if (!mergedInstitution.lenientEquals(match.getInstitutionMatched())) {
      executeOrAddFailAsync(
          () -> {
            grSciCollHttpClient.updateInstitution(mergedInstitution);
            mergedInstitution.getIdentifiers().stream()
                .filter(i -> i.getKey() == null)
                .forEach(
                    i ->
                        grSciCollHttpClient.addIdentifierToInstitution(
                            mergedInstitution.getKey(), i));
            mergedInstitution.getMachineTags().stream()
                .filter(mt -> mt.getKey() == null)
                .forEach(
                    mt ->
                        grSciCollHttpClient.addMachineTagToInstitution(
                            mergedInstitution.getKey(), mt));
          },
          e ->
              new FailedAction(
                  mergedInstitution, "Failed to update institution: " + e.getMessage()));
      entityMatchBuilder.update(true);
    }

    return entityMatchBuilder.build();
  }

  private SyncResult.EntityMatch<Collection> updateCollection(MatchResult match) {
    // if it's a IH entity and iDigBio is more up to date we create an issue for IH to check
    checkOutdatedIHInstitution(match.getCollectionMatched(), match.getIDigBioRecord());

    Collection mergedCollection =
        EntityConverter.convertToCollection(match.getCollectionMatched(), match.getIDigBioRecord());

    EntityMatch.EntityMatchBuilder<Collection> entityMatchBuilder =
        EntityMatch.<Collection>builder()
            .matched(match.getCollectionMatched())
            .merged(mergedCollection);
    if (!mergedCollection.lenientEquals(match.getCollectionMatched())) {
      executeOrAddFailAsync(
          () -> {
            grSciCollHttpClient.updateCollection(mergedCollection);
            mergedCollection.getIdentifiers().stream()
                .filter(i -> i.getKey() == null)
                .forEach(
                    i ->
                        grSciCollHttpClient.addIdentifierToCollection(
                            mergedCollection.getKey(), i));
            mergedCollection.getMachineTags().stream()
                .filter(mt -> mt.getKey() == null)
                .forEach(
                    mt ->
                        grSciCollHttpClient.addMachineTagToCollection(
                            mergedCollection.getKey(), mt));
          },
          e ->
              new FailedAction(mergedCollection, "Failed to update collection: " + e.getMessage()));
      entityMatchBuilder.update(true);
    }

    return entityMatchBuilder.build();
  }

  private <T extends CollectionEntity & Identifiable> void checkOutdatedIHInstitution(
      T entity, IDigBioRecord iDigBioRecord) {
    // if it's a IH entity and iDigBio is more up to date we create an issue for IH to check
    if (containsIrnIdentifier(entity)) {
      Optional<Identifier> irnIdentifier =
          entity.getIdentifiers().stream()
              .filter(i -> i.getType() == IdentifierType.IH_IRN)
              .findFirst();
      if (irnIdentifier.isPresent()) {
        IHInstitution ihInstitution =
            ihInstitutionsByIrn.get(decodeIRN(irnIdentifier.get().getIdentifier()));
        if (isIDigBioMoreRecent(iDigBioRecord, parseDate(ihInstitution.getDateModified()))) {
          createGHIssue(
              issueFactory.createOutdatedIHInstitutionIssue(ihInstitution, iDigBioRecord));
          syncResultBuilder.outdatedEntity(new OutdatedEntity(ihInstitution, iDigBioRecord));
        }
      }
    }
  }

  private <T extends CollectionEntity & Contactable> StaffMatch handleStaff(
      MatchResult match, List<T> entities) {
    if (!containsContact(match.getIDigBioRecord())) {
      return StaffMatch.builder().build();
    }

    if (Strings.isNullOrEmpty(match.getIDigBioRecord().getContact())
        && !Strings.isNullOrEmpty(match.getIDigBioRecord().getContactEmail())) {
      // if it doesn't have name we just add the email to the entity emails. The position is discarded in this case
      entities.forEach(
          e -> {
            if (e instanceof Institution) {
              ((Institution) e)
                  .getEmail()
                  .add(cleanString(match.getIDigBioRecord().getContactEmail()));
            } else if (e instanceof Collection) {
              ((Collection) e)
                  .getEmail()
                  .add(cleanString(match.getIDigBioRecord().getContactEmail()));
            }
          });
      return StaffMatch.builder().build();
    }

    Set<Person> matches = Matcher.matchContact(match.getIDigBioRecord(), grscicollPersons);

    BiConsumer<T, Person> addPersonToEntity =
        (e, p) -> {
          // they can be null in dry runs or if the creation of a collection/institution fails
          if (isPersonInContacts(p.getKey(), e.getContacts())) {
            return;
          }

          if (e instanceof Collection) {
            grSciCollHttpClient.addPersonToCollection(p.getKey(), e.getKey());
          } else if (e instanceof Institution) {
            grSciCollHttpClient.addPersonToInstitution(p.getKey(), e.getKey());
          }

          // we add it to the contacts to avoid adding it again if there are duplicates in IH
          e.getContacts().add(p);
        };

    StaffMatch.StaffMatchBuilder staffSyncBuilder = StaffMatch.builder();
    if (matches.isEmpty()) {
      // create person and link it to the entity
      log.info("No match for staff for record: {}", match.getIDigBioRecord());
      Person newPerson = EntityConverter.convertToPerson(match.getIDigBioRecord());

      // create new person in the registry and link it to the entities
      Person createdPerson =
          executeAndReturnOrAddFail(
              () -> {
                UUID createdKey = grSciCollHttpClient.createPerson(newPerson);
                newPerson.setKey(createdKey);
                entities.forEach(e -> addPersonToEntity.accept(e, newPerson));

                // return the newly created person in order to add it to the set with all persons
                return grSciCollHttpClient.getPerson(createdKey);
              },
              e -> new FailedAction(newPerson, "Failed to create person: " + e.getMessage()));

      if (createdPerson == null) {
        // this is needed for dry runs
        createdPerson = newPerson;
      }
      grscicollPersons.add(createdPerson);

      staffSyncBuilder.newPerson(newPerson);
    } else if (matches.size() > 1) {
      // conflict. Multiple candidates matched
      log.info("Conflict for iDigBio staff: {}", match.getIDigBioRecord());
      createGHIssue(issueFactory.createStaffConflict(matches, match.getIDigBioRecord()));
      staffSyncBuilder.conflict(new Conflict(match.getIDigBioRecord(), new ArrayList<>(matches)));
    } else {
      // there is one match
      log.info("One match for iDigBio Staff {}", match.getIDigBioRecord());
      Person matchedPerson = matches.iterator().next();
      Person mergedPerson =
          EntityConverter.convertToPerson(matchedPerson, match.getIDigBioRecord());

      EntityMatch.EntityMatchBuilder<Person> entityMatchBuilder =
          EntityMatch.<Person>builder().matched(matchedPerson).merged(mergedPerson);
      if (!mergedPerson.lenientEquals(matchedPerson)) {
        // update person
        Person updatedPerson =
            executeAndReturnOrAddFail(
                () -> {
                  grSciCollHttpClient.updatePerson(mergedPerson);
                  return grSciCollHttpClient.getPerson(mergedPerson.getKey());
                },
                e -> new FailedAction(mergedPerson, "Failed to update person: " + e.getMessage()));

        // update the person in the set with all persons
        if (updatedPerson == null) {
          // needed for dry runs
          updatedPerson = mergedPerson;
        }
        grscicollPersons.remove(matchedPerson);
        grscicollPersons.add(updatedPerson);

        // add identifiers if needed
        executeOrAddFailAsync(
            () ->
                mergedPerson.getIdentifiers().stream()
                    .filter(i -> i.getKey() == null)
                    .forEach(
                        i -> grSciCollHttpClient.addIdentifierToPerson(mergedPerson.getKey(), i)),
            e ->
                new FailedAction(
                    mergedPerson, "Failed to add identifiers to person: " + e.getMessage()));

        // add to the entity if needed
        executeOrAddFailAsync(
            () -> entities.forEach(e -> addPersonToEntity.accept(e, mergedPerson)),
            e ->
                new FailedAction(
                    mergedPerson, "Failed to add persons to entity: " + e.getMessage()));
        entityMatchBuilder.update(true);
      }

      staffSyncBuilder.matchedPerson(entityMatchBuilder.build());
    }

    return staffSyncBuilder.build();
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

  private void executeOrAddFail(Runnable runnable, Function<Throwable, FailedAction> failCreator) {
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
      Optional<Issue> existingIssueOpt = githubClient.findIssueWithSameTitle(issue.getTitle());
      Runnable runnable;
      String errorMsg;
      if (existingIssueOpt.isPresent()) {
        // if it exists we update the labels to add the one of this sync. We also merge the
        // assignees in case the original ones were modified in Github
        Issue existingIssue = existingIssueOpt.get();
        issue.setNumber(existingIssue.getNumber());
        issue.getLabels().addAll(existingIssue.getLabels());
        issue.getAssignees().addAll(existingIssue.getAssignees());

        runnable = () -> githubClient.updateIssue(issue);
        errorMsg = "Failed to add sync timestamp label to issue: ";
      } else {
        // if it doesn't exist we create it
        runnable = () -> githubClient.createIssue(issue);
        errorMsg = "Failed to create issue: ";
      }

      // do the call
      CompletableFuture.runAsync(runnable)
          .whenCompleteAsync(
              (r, e) -> {
                if (e != null) {
                  syncResultBuilder.failedAction(
                      new FailedAction(issue, errorMsg + e.getMessage()));
                }
              });
    }
  }

  private <T> T executeAndReturnOrAddFail(
      Supplier<T> execution, Function<Throwable, FailedAction> failCreator) {
    if (!dryRun) {
      try {
        return execution.get();
      } catch (Exception e) {
        syncResultBuilder.failedAction(failCreator.apply(e));
      }
    }

    return null;
  }

  private boolean isValidIDigBioRecord(IDigBioRecord iDigBioRecord) {
    if (Strings.isNullOrEmpty(iDigBioRecord.getInstitutionCode())
        && Strings.isNullOrEmpty(iDigBioRecord.getInstitution())
        && Strings.isNullOrEmpty(iDigBioRecord.getCollectionCode())
        && Strings.isNullOrEmpty(iDigBioRecord.getCollection())) {
      syncResultBuilder.invalidEntity(iDigBioRecord);
      return false;
    }
    return true;
  }

  private boolean containsContact(IDigBioRecord iDigBioRecord) {
    return !Strings.isNullOrEmpty(iDigBioRecord.getContact())
        || !Strings.isNullOrEmpty(iDigBioRecord.getContactEmail())
        || !Strings.isNullOrEmpty(iDigBioRecord.getContactRole());
  }

  private static boolean isIDigBioMoreRecent(IDigBioRecord record, Date grSciCollEntityDate) {
    return record.getModifiedDate() != null
        && grSciCollEntityDate != null
        && record.getModifiedDate().isAfter(TO_LOCAL_DATE_TIME_UTC.apply(grSciCollEntityDate));
  }
}
