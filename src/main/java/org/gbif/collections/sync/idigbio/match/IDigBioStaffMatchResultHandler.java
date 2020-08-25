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
import org.gbif.collections.sync.SyncResult.EntityMatch;
import org.gbif.collections.sync.SyncResult.StaffMatch;
import org.gbif.collections.sync.clients.proxy.IDigBioProxyClient;
import org.gbif.collections.sync.common.match.MatchResult;
import org.gbif.collections.sync.common.match.StaffResultHandler;
import org.gbif.collections.sync.idigbio.IDigBioEntityConverter;
import org.gbif.collections.sync.idigbio.model.IDigBioRecord;

import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.collections.sync.common.parsers.DataParser.cleanString;

@Slf4j
public class IDigBioStaffMatchResultHandler
    implements StaffResultHandler<IDigBioRecord, IDigBioRecord> {

  private final IDigBioProxyClient proxyClient;
  private final IDigBioEntityConverter entityConverter = IDigBioEntityConverter.create();

  public IDigBioStaffMatchResultHandler(IDigBioProxyClient proxyClient) {
    this.proxyClient = proxyClient;
  }

  @Override
  public <T extends CollectionEntity & Contactable> StaffMatch handleStaff(
      MatchResult<IDigBioRecord, IDigBioRecord> match, List<T> entities) {
    if (!containsContact(match.getSource())) {
      return StaffMatch.builder().build();
    }

    if (Strings.isNullOrEmpty(match.getSource().getContact())
        && !Strings.isNullOrEmpty(match.getSource().getContactEmail())) {
      return handleOnlyContactEmail(match, entities);
    }

    Set<Person> contacts =
        entities.stream()
            .filter(e -> e.getContacts() != null)
            .flatMap(e -> e.getContacts().stream())
            .collect(Collectors.toSet());

    // there should be only 1 match at most
    Optional<Person> personMatch =
        Optional.ofNullable(match.getStaffMatcher().apply(match.getSource(), contacts))
            .filter(p -> !p.isEmpty())
            .map(p -> p.iterator().next());

    StaffMatch.StaffMatchBuilder staffSyncBuilder = StaffMatch.builder();
    if (personMatch.isPresent()) {
      log.info("One match for iDigBio Staff {}", match.getSource());
      Person matchedPerson = personMatch.get();
      Person mergedPerson = entityConverter.convertToPerson(match.getSource(), matchedPerson);

      boolean updated = proxyClient.updatePerson(matchedPerson, mergedPerson);

      EntityMatch<Person> entityMatch =
          EntityMatch.<Person>builder()
              .merged(matchedPerson)
              .merged(mergedPerson)
              .update(updated)
              .build();
      Person updatedPerson =
          proxyClient.getPersonsByKey().getOrDefault(mergedPerson.getKey(), mergedPerson);

      // add to the entity if needed
      proxyClient.linkPersonToEntity(updatedPerson, entities);

      staffSyncBuilder.matchedPerson(entityMatch);
    } else {
      // create person and link it to the entity
      log.info("No match for staff for record: {}", match.getSource());
      Person newPerson = entityConverter.convertToPerson(match.getSource());

      // create new person in the registry and link it to the entities
      Person createdPerson = proxyClient.createPerson(newPerson);
      proxyClient.linkPersonToEntity(createdPerson, entities);

      staffSyncBuilder.newPerson(newPerson);
    }

    return staffSyncBuilder.build();
  }

  private static <T extends CollectionEntity & Contactable> StaffMatch handleOnlyContactEmail(
      MatchResult<IDigBioRecord, IDigBioRecord> match, List<T> entities) {
    // if it doesn't have name we just add the email to the entity emails. The position is
    // discarded in this case
    entities.forEach(
        e -> {
          String cleanEmail = cleanString(match.getSource().getContactEmail());
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
