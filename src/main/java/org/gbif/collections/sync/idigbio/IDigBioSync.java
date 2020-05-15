package org.gbif.collections.sync.idigbio;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Contactable;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.SyncResult.CollectionOnlyMatch;
import org.gbif.collections.sync.SyncResult.Conflict;
import org.gbif.collections.sync.SyncResult.EntityMatch;
import org.gbif.collections.sync.SyncResult.FailedAction;
import org.gbif.collections.sync.SyncResult.InstitutionAndCollectionMatch;
import org.gbif.collections.sync.SyncResult.InstitutionOnlyMatch;
import org.gbif.collections.sync.SyncResult.NoEntityMatch;
import org.gbif.collections.sync.SyncResult.StaffMatch;
import org.gbif.collections.sync.config.IDigBioConfig;
import org.gbif.collections.sync.http.clients.GithubClient;
import org.gbif.collections.sync.http.clients.GrSciCollHttpClient;
import org.gbif.collections.sync.idigbio.match.MatchData;
import org.gbif.collections.sync.idigbio.match.MatchResult;
import org.gbif.collections.sync.idigbio.match.Matcher;
import org.gbif.collections.sync.notification.IDigBioIssueFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.collections.sync.Utils.isPersonInContacts;
import static org.gbif.collections.sync.http.Executor.createGHIssue;
import static org.gbif.collections.sync.http.Executor.executeAndReturnOrAddFail;
import static org.gbif.collections.sync.http.Executor.executeOrAddFail;
import static org.gbif.collections.sync.http.Executor.executeOrAddFailAsync;
import static org.gbif.collections.sync.parsers.DataParser.cleanString;

@Slf4j
public class IDigBioSync {

  private final boolean dryRun;
  private final boolean sendNotifications;
  private final IDigBioConfig iDigBioConfig;
  private final GrSciCollHttpClient grSciCollHttpClient;
  private final GithubClient githubClient;
  private final IDigBioIssueFactory issueFactory;
  private SyncResult.SyncResultBuilder syncResultBuilder;
  private Matcher matcher;
  private MatchData matchData = new MatchData();

  @Builder
  private IDigBioSync(IDigBioConfig iDigBioConfig) {
    if (iDigBioConfig != null && iDigBioConfig.getSyncConfig() != null) {
      this.dryRun = iDigBioConfig.getSyncConfig().isDryRun();
      this.sendNotifications = iDigBioConfig.getSyncConfig().isSendNotifications();
      this.grSciCollHttpClient = GrSciCollHttpClient.create(iDigBioConfig.getSyncConfig());
      this.githubClient = GithubClient.create(iDigBioConfig.getSyncConfig().getNotification());
      this.issueFactory = IDigBioIssueFactory.create(iDigBioConfig);
      this.iDigBioConfig = iDigBioConfig;
    } else {
      this.dryRun = true;
      this.sendNotifications = false;
      this.grSciCollHttpClient = null;
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

    log.info("Loading data from WSs");
    CompletableFuture.allOf(institutionsFuture, collectionsFuture, personsFuture).join();

    matchData =
        MatchData.builder()
            .institutions(institutionsFuture.join())
            .collections(collectionsFuture.join())
            .persons(personsFuture.join())
            .build();
    matcher = Matcher.builder().matchData(matchData).build();
    syncResultBuilder = SyncResult.builder();

    List<IDigBioRecord> records = readIDigBioExport(iDigBioConfig);
    for (IDigBioRecord record : records) {
      if (isInvalidRecord(record)) {
        continue;
      }

      MatchResult match = matcher.match(record);
      if (match.onlyCollectionMatch()) {
        syncResultBuilder.collectionOnlyMatch(handleCollectionMatch(match));
      } else if (match.onlyInstitutionMatch()) {
        syncResultBuilder.institutionOnlyMatch(handleInstitutionMatch(match));
      } else if (match.noMatches()) {
        if (hasCodeAndName(record)) {
          syncResultBuilder.noMatch(handleNoMatches(match));
        }
      } else if (match.institutionAndCollectionMatch()) {
        syncResultBuilder.instAndCollMatch(handleInstitutionAndCollectionMatch(match));
      } else {
        syncResultBuilder.conflict(handleConflict(match));
      }
    }

    SyncResult syncResult = syncResultBuilder.build();

    if (syncResult.getInvalidEntities() != null && !syncResult.getInvalidEntities().isEmpty()) {
      createGHIssue(
          issueFactory.createInvalidEntitiesIssue(syncResult.getInvalidEntities()),
          sendNotifications,
          githubClient,
          syncResultBuilder);
    }

    return syncResult;
  }

  private List<IDigBioRecord> readIDigBioExport(IDigBioConfig config) {
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
    EntityMatch<Collection> collectionEntityMatch = updateCollection(match);

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
            match.getIDigBioRecord(), institutionEntityMatch.getMatched());

    UUID createdKey =
        executeAndReturnOrAddFail(
            () -> grSciCollHttpClient.createCollection(newCollection),
            e -> new FailedAction(newCollection, "Failed to create collection: " + e.getMessage()),
            dryRun,
            syncResultBuilder);
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

    Institution createdInstitution =
        executeAndReturnOrAddFail(
            () -> {
              UUID createdKey = grSciCollHttpClient.createInstitution(newInstitution);
              return grSciCollHttpClient.getInstitution(createdKey);
            },
            e ->
                new FailedAction(
                    newInstitution, "Failed to create institution : " + e.getMessage()),
            dryRun,
            syncResultBuilder,
            newInstitution);
    matchData.addNewlyCreatedIDigBioInstitution(createdInstitution);

    // create collection
    Collection newCollection =
        EntityConverter.convertToCollection(match.getIDigBioRecord(), createdInstitution);

    UUID collectionKey =
        executeAndReturnOrAddFail(
            () -> grSciCollHttpClient.createCollection(newCollection),
            e ->
                new FailedAction(
                    newCollection,
                    "Failed to create institution and collection: " + e.getMessage()),
            dryRun,
            syncResultBuilder);
    newCollection.setKey(collectionKey);

    // same staff for both entities
    StaffMatch staffMatch = handleStaff(match, Arrays.asList(createdInstitution, newCollection));

    return NoEntityMatch.builder()
        .newCollection(newCollection)
        .newInstitution(createdInstitution)
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
    createGHIssue(
        issueFactory.createConflict(match.getAllMatches(), match.getIDigBioRecord()),
        sendNotifications,
        githubClient,
        syncResultBuilder);
    return new Conflict(match.getIDigBioRecord(), match.getAllMatches());
  }

  private EntityMatch<Institution> updateInstitution(MatchResult match) {
    Institution mergedInstitution =
        EntityConverter.convertToInstitution(
            match.getInstitutionMatched(), match.getIDigBioRecord());

    EntityMatch.EntityMatchBuilder<Institution> entityMatchBuilder =
        EntityMatch.<Institution>builder()
            .matched(match.getInstitutionMatched())
            .merged(mergedInstitution);
    if (!mergedInstitution.equals(match.getInstitutionMatched())) {
      // check if we need to update the entity
      if (!mergedInstitution.lenientEquals(match.getInstitutionMatched())) {
        executeOrAddFail(
            () -> grSciCollHttpClient.updateInstitution(mergedInstitution),
            e ->
                new FailedAction(
                    mergedInstitution, "Failed to update institution: " + e.getMessage()),
            dryRun,
            syncResultBuilder);
      }
      // create identifiers and machine tags if needed
      executeOrAddFail(
          () -> {
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
                  mergedInstitution,
                  "Failed to add identifiers and machine tags of institution: " + e.getMessage()),
          dryRun,
          syncResultBuilder);

      // update institution in our match data
      Institution updatedInstitution =
          executeAndReturnOrAddFail(
              () -> grSciCollHttpClient.getInstitution(mergedInstitution.getKey()),
              e ->
                  new FailedAction(
                      mergedInstitution, "Failed to get updated institution: " + e.getMessage()),
              dryRun,
              syncResultBuilder);
      matchData.updateInsitution(updatedInstitution);

      entityMatchBuilder.update(true);
    }

    return entityMatchBuilder.build();
  }

  private SyncResult.EntityMatch<Collection> updateCollection(MatchResult match) {
    Collection mergedCollection =
        EntityConverter.convertToCollection(match.getCollectionMatched(), match.getIDigBioRecord());

    EntityMatch.EntityMatchBuilder<Collection> entityMatchBuilder =
        EntityMatch.<Collection>builder()
            .matched(match.getCollectionMatched())
            .merged(mergedCollection);
    if (!mergedCollection.equals(match.getCollectionMatched())) {
      // check if we need to update the entity
      if (!mergedCollection.lenientEquals(match.getCollectionMatched())) {
        executeOrAddFail(
            () -> grSciCollHttpClient.updateCollection(mergedCollection),
            e ->
                new FailedAction(
                    mergedCollection, "Failed to update collection: " + e.getMessage()),
            dryRun,
            syncResultBuilder);
      }
      // create indentifiers and machine tags if needed
      executeOrAddFailAsync(
          () -> {
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
              new FailedAction(
                  mergedCollection,
                  "Failed to add identifiers and machine tags of collection: " + e.getMessage()),
          dryRun,
          syncResultBuilder);

      // update collection in our match data
      Collection updatedCollection =
          executeAndReturnOrAddFail(
              () -> grSciCollHttpClient.getCollection(mergedCollection.getKey()),
              e ->
                  new FailedAction(
                      mergedCollection, "Failed to get updated collection: " + e.getMessage()),
              dryRun,
              syncResultBuilder);
      matchData.updateCollection(match.getCollectionMatched(), updatedCollection);

      entityMatchBuilder.update(true);
    }

    return entityMatchBuilder.build();
  }

  private <T extends CollectionEntity & Contactable> StaffMatch handleStaff(
      MatchResult match, List<T> entities) {
    if (!containsContact(match.getIDigBioRecord())) {
      return StaffMatch.builder().build();
    }

    if (Strings.isNullOrEmpty(match.getIDigBioRecord().getContact())
        && !Strings.isNullOrEmpty(match.getIDigBioRecord().getContactEmail())) {
      // if it doesn't have name we just add the email to the entity emails. The position is
      // discarded in this case
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

    Set<Person> contacts =
        entities.stream()
            .filter(e -> e.getContacts() != null)
            .flatMap(e -> e.getContacts().stream())
            .collect(Collectors.toSet());

    Optional<Person> personMatch = matcher.matchContact(match.getIDigBioRecord(), contacts);
    StaffMatch.StaffMatchBuilder staffSyncBuilder = StaffMatch.builder();
    if (personMatch.isPresent()) {
      log.info("One match for iDigBio Staff {}", match.getIDigBioRecord());
      Person matchedPerson = personMatch.get();
      Person mergedPerson =
          EntityConverter.convertToPerson(matchedPerson, match.getIDigBioRecord());

      EntityMatch.EntityMatchBuilder<Person> entityMatchBuilder =
          EntityMatch.<Person>builder().matched(matchedPerson).merged(mergedPerson);
      if (!mergedPerson.lenientEquals(matchedPerson)) {
        Person updatedPerson =
            executeAndReturnOrAddFail(
                () -> {
                  grSciCollHttpClient.updatePerson(mergedPerson);
                  return grSciCollHttpClient.getPerson(mergedPerson.getKey());
                },
                e -> new FailedAction(mergedPerson, "Failed to update person: " + e.getMessage()),
                dryRun,
                syncResultBuilder,
                mergedPerson);

        // update the person in the set with all persons
        matchData.updatePerson(matchedPerson, updatedPerson);

        entityMatchBuilder.update(true);
      } else {
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
                e -> new FailedAction(newPerson, "Failed to create person: " + e.getMessage()),
                dryRun,
                syncResultBuilder,
                newPerson);
        matchData.addNewPerson(createdPerson);

        staffSyncBuilder.newPerson(newPerson);
      }

      // add to the entity if needed
      executeOrAddFailAsync(
          () -> entities.forEach(e -> addPersonToEntity.accept(e, mergedPerson)),
          e -> new FailedAction(mergedPerson, "Failed to add persons to entity: " + e.getMessage()),
          dryRun,
          syncResultBuilder);

      staffSyncBuilder.matchedPerson(entityMatchBuilder.build());
    }

    return staffSyncBuilder.build();
  }

  private boolean isInvalidRecord(IDigBioRecord record) {
    return Strings.isNullOrEmpty(record.getInstitution())
        && Strings.isNullOrEmpty(record.getInstitutionCode())
        && Strings.isNullOrEmpty(record.getCollection())
        && Strings.isNullOrEmpty(record.getCollectionCode());
  }

  private boolean hasCodeAndName(IDigBioRecord iDigBioRecord) {
    if ((!Strings.isNullOrEmpty(iDigBioRecord.getInstitution())
            || !Strings.isNullOrEmpty(iDigBioRecord.getCollection()))
        && (!Strings.isNullOrEmpty(iDigBioRecord.getInstitutionCode())
            || !Strings.isNullOrEmpty(iDigBioRecord.getCollectionCode()))) {
      return true;
    }
    syncResultBuilder.invalidEntity(iDigBioRecord);
    return false;
  }

  private boolean containsContact(IDigBioRecord iDigBioRecord) {
    return !Strings.isNullOrEmpty(iDigBioRecord.getContact())
            && !"NA".equals(iDigBioRecord.getContact())
        || !Strings.isNullOrEmpty(iDigBioRecord.getContactEmail())
            && !"NA".equals(iDigBioRecord.getContactEmail())
        || !Strings.isNullOrEmpty(iDigBioRecord.getContactRole());
  }
}
