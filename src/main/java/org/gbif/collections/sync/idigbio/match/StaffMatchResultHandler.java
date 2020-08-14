package org.gbif.collections.sync.idigbio.match;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Contactable;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.SyncResult.EntityMatch;
import org.gbif.collections.sync.SyncResult.StaffMatch;
import org.gbif.collections.sync.config.SyncConfig;
import org.gbif.collections.sync.handler.PersonHandler;
import org.gbif.collections.sync.idigbio.EntityConverter;
import org.gbif.collections.sync.idigbio.IDigBioRecord;

import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.collections.sync.parsers.DataParser.cleanString;

@Slf4j
public class StaffMatchResultHandler {

  private final Matcher matcher;
  private final PersonHandler personHandler;

  public StaffMatchResultHandler(
      SyncConfig syncConfig, Matcher matcher, SyncResult.SyncResultBuilder syncResultBuilder) {
    this.matcher = matcher;
    this.personHandler =
        PersonHandler.builder().syncConfig(syncConfig).syncResultBuilder(syncResultBuilder).build();
  }

  public <T extends CollectionEntity & Contactable> StaffMatch handleStaff(
      MatchResult match, List<T> entities) {
    if (!containsContact(match.getIDigBioRecord())) {
      return StaffMatch.builder().build();
    }

    if (Strings.isNullOrEmpty(match.getIDigBioRecord().getContact())
        && !Strings.isNullOrEmpty(match.getIDigBioRecord().getContactEmail())) {
      return handleOnlyContactEmail(match, entities);
    }

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

      EntityMatch<Person> entityMatch = personHandler.updateEntity(matchedPerson, mergedPerson);
      Person updatedPerson = personHandler.getEntity(mergedPerson);

      // update the person in the set with all persons
      matcher.getMatchData().updatePerson(matchedPerson, updatedPerson);

      // add to the entity if needed
      personHandler.linkPersonToEntity(updatedPerson, entities);

      staffSyncBuilder.matchedPerson(entityMatch);
    } else {
      // create person and link it to the entity
      log.info("No match for staff for record: {}", match.getIDigBioRecord());
      Person newPerson = EntityConverter.convertToPerson(match.getIDigBioRecord());

      // create new person in the registry and link it to the entities
      Person createdPerson = personHandler.createPersonAndLinkToEntities(newPerson, entities);
      matcher.getMatchData().addNewPerson(createdPerson);

      staffSyncBuilder.newPerson(newPerson);
    }

    return staffSyncBuilder.build();
  }

  private static <T extends CollectionEntity & Contactable> StaffMatch handleOnlyContactEmail(
      MatchResult match, List<T> entities) {
    // if it doesn't have name we just add the email to the entity emails. The position is
    // discarded in this case
    entities.forEach(
        e -> {
          String cleanEmail = cleanString(match.getIDigBioRecord().getContactEmail());
          if (e instanceof Institution) {
            Institution institution = (Institution) e;
            if (!institution.getEmail().contains(cleanEmail)) {
              institution.getEmail().add(cleanEmail);
            }
          } else if (e instanceof Collection) {
            Collection collection = (Collection) e;
            if (!collection.getEmail().contains(cleanEmail)) {
              collection.getEmail().add(cleanEmail);
            }
          }
        });
    return StaffMatch.builder().build();
  }

  private static boolean containsContact(IDigBioRecord iDigBioRecord) {
    return !Strings.isNullOrEmpty(iDigBioRecord.getContact())
            && !"NA".equals(iDigBioRecord.getContact())
        || !Strings.isNullOrEmpty(iDigBioRecord.getContactEmail())
            && !"NA".equals(iDigBioRecord.getContactEmail())
        || !Strings.isNullOrEmpty(iDigBioRecord.getContactRole());
  }
}
