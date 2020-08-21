package org.gbif.collections.sync.ih.match;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Contactable;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.SyncResult.Conflict;
import org.gbif.collections.sync.SyncResult.EntityMatch;
import org.gbif.collections.sync.SyncResult.StaffMatch;
import org.gbif.collections.sync.config.IHConfig;
import org.gbif.collections.sync.ih.EntityConverter;
import org.gbif.collections.sync.ih.IHProxyClient;
import org.gbif.collections.sync.ih.model.IHStaff;
import org.gbif.collections.sync.notification.IHIssueNotifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StaffMatchResultHandler {

  private final IHIssueNotifier issueNotifier;
  private final EntityConverter entityConverter;
  private final IHProxyClient proxyClient;

  public StaffMatchResultHandler(
      IHConfig ihConfig, IHProxyClient proxyClient, EntityConverter entityConverter) {
    // TODO: move to proxy??
    issueNotifier = IHIssueNotifier.create(ihConfig);
    this.entityConverter = entityConverter;
    this.proxyClient = proxyClient;
  }

  @VisibleForTesting
  public <T extends CollectionEntity & Contactable> StaffMatch handleStaff(
      MatchResult match, List<T> entities) {

    // merge contacts from all entities
    Set<Person> contacts =
        entities.stream()
            .filter(e -> e.getContacts() != null)
            .flatMap(e -> e.getContacts().stream())
            .collect(Collectors.toSet());

    // copy contacts to keep track of the matched ones in order to remove the left ones at the end
    Set<Person> contactsCopy = new HashSet<>(contacts);
    StaffMatch.StaffMatchBuilder staffSyncBuilder = StaffMatch.builder();

    // we sort the ihStaff list to process first the ones that have more values filled, hence the
    // match will be easier
    List<IHStaff> ihStaffList = new ArrayList<>(match.getIhStaff());
    ihStaffList.sort(IHStaff.COMPARATOR_BY_COMPLETENESS.reversed());

    for (IHStaff ihStaff : ihStaffList) {
      if (!isValidIhStaff(ihStaff)) {
        issueNotifier.createInvalidEntity(ihStaff, "Not valid person - first name is required");
        continue;
      }

      Set<Person> staffMatches = match.getStaffMatcher().apply(ihStaff, contacts);

      if (staffMatches.isEmpty()) {
        // create person and link it to the entity
        log.info("No match for IH Staff {}", ihStaff.getIrn());
        Person newPerson = entityConverter.convertToPerson(ihStaff);
        proxyClient.createPerson(newPerson);
        proxyClient.linkPersonToEntity(newPerson, entities);
        staffSyncBuilder.newPerson(newPerson);
      } else if (staffMatches.size() > 1) {
        // conflict. Multiple candidates matched
        log.info("Conflict for IH Staff {}", ihStaff.getIrn());
        contactsCopy.removeAll(staffMatches);
        issueNotifier.createStaffConflict(staffMatches, ihStaff);
        staffSyncBuilder.conflict(new Conflict(ihStaff, new ArrayList<>(staffMatches)));
      } else {
        // there is one match
        log.info("One match for IH Staff {}", ihStaff.getIrn());
        Person matchedPerson = staffMatches.iterator().next();
        contactsCopy.remove(matchedPerson);
        Person mergedPerson = entityConverter.convertToPerson(ihStaff, matchedPerson);

        boolean updated = proxyClient.updatePerson(matchedPerson, mergedPerson);

        EntityMatch<Person> entityMatch =
            EntityMatch.<Person>builder()
                .matched(matchedPerson)
                .merged(mergedPerson)
                .update(updated)
                .build();

        // if the match was global we'd need to link it to the entity. The same if we're
        // syncing staff from different entities: one entity could have the contact already
        // but not the other
        proxyClient.linkPersonToEntity(mergedPerson, entities);

        staffSyncBuilder.matchedPerson(entityMatch);
      }
    }

    // now we remove all the contacts that are not present in IH
    contactsCopy.forEach(
        personToRemove -> {
          log.info("Removing contact {}", personToRemove.getKey());
          proxyClient.unlinkPersonFromEntity(personToRemove, entities);
          staffSyncBuilder.removedPerson(personToRemove);
        });

    return staffSyncBuilder.build();
  }

  private static boolean isValidIhStaff(IHStaff ihStaff) {
    return !Strings.isNullOrEmpty(ihStaff.getFirstName())
        || !Strings.isNullOrEmpty(ihStaff.getMiddleName());
  }
}
