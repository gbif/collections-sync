package org.gbif.collections.sync.ih.match;

import org.gbif.api.model.collections.*;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.SyncResult.Conflict;
import org.gbif.collections.sync.SyncResult.EntityMatch;
import org.gbif.collections.sync.SyncResult.StaffMatch;
import org.gbif.collections.sync.clients.proxy.IHProxyClient;
import org.gbif.collections.sync.common.match.MatchResult;
import org.gbif.collections.sync.common.match.StaffResultHandler;
import org.gbif.collections.sync.config.IHConfig;
import org.gbif.collections.sync.ih.IHEntityConverter;
import org.gbif.collections.sync.ih.IHIssueNotifier;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.collections.sync.SyncResult.ContactMatch;

@Slf4j
public class IHStaffMatchResultHandler implements StaffResultHandler<IHInstitution, IHStaff> {

  private final IHIssueNotifier issueNotifier;
  private final IHEntityConverter entityConverter;
  private final IHProxyClient proxyClient;

  @Builder
  public IHStaffMatchResultHandler(
      IHConfig ihConfig, IHProxyClient proxyClient, IHEntityConverter entityConverter) {
    issueNotifier = IHIssueNotifier.getInstance(ihConfig);
    this.entityConverter = entityConverter;
    this.proxyClient = proxyClient;
  }

  @Deprecated
  @Override
  public <T extends CollectionEntity & Contactable> StaffMatch handleStaff(
      MatchResult<IHInstitution, IHStaff> matchResult, List<T> entities) {

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
    List<IHStaff> ihStaffList = new ArrayList<>(matchResult.getStaff());
    ihStaffList.sort(IHStaff.COMPARATOR_BY_COMPLETENESS.reversed());

    for (IHStaff ihStaff : ihStaffList) {
      if (!isValidIhStaff(ihStaff)) {
        issueNotifier.createInvalidEntity(ihStaff, "Not valid person - first name is required");
        continue;
      }

      Set<Person> staffMatches = matchResult.getStaffMatcher().apply(ihStaff, contacts);

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

  @Override
  public <T extends CollectionEntity & Contactable> ContactMatch handleStaff(
      MatchResult<IHInstitution, IHStaff> matchResult, T entity) {

    ContactMatch.ContactMatchBuilder contactSyncBuilder = ContactMatch.builder();
    Set<IHStaff> ihStaffList = matchResult.getStaff();
    Set<Contact> contactsCopy =
        entity.getContactPersons() != null
            ? new HashSet<>(entity.getContactPersons())
            : new HashSet<>();
    for (IHStaff ihStaff : ihStaffList) {
      if (!isValidIhStaff(ihStaff)) {
        issueNotifier.createInvalidEntity(ihStaff, "Not valid person - first name is required");
        continue;
      }

      Set<Contact> contactsMatched =
          entity.getContactPersons().stream()
              .filter(
                  cp ->
                      cp.getUserIds().stream()
                          .anyMatch(
                              userId ->
                                  userId.getType() == IdType.IH_IRN
                                      && userId.getId().equals(ihStaff.getIrn())))
              .collect(Collectors.toSet());

      if (contactsMatched.isEmpty()) {
        // create
        log.info("No contact match for IH Staff {}", ihStaff.getIrn());
        Contact newContact = entityConverter.convertToContact(ihStaff);
        addContactToEntity(entity, newContact);
        contactSyncBuilder.newContact(newContact);
      } else if (contactsMatched.size() > 1) {
        // conflict
        log.info("Conflict in contacts for IH Staff {}", ihStaff.getIrn());
        contactsCopy.removeAll(contactsMatched);
        issueNotifier.createContactConflict(contactsMatched, ihStaff);
        contactSyncBuilder.conflict(new Conflict(ihStaff, new ArrayList<>(contactsMatched)));
      } else {
        // update the match
        Contact contactMatched = contactsMatched.iterator().next();
        contactsCopy.remove(contactMatched);

        Contact updatedContact = entityConverter.convertToContact(ihStaff, contactMatched);
        boolean update = updateContactInEntity(entity, contactMatched, updatedContact);

        SyncResult.EntityMatch<Contact> entityMatch =
            EntityMatch.<Contact>builder()
                .matched(contactMatched)
                .merged(updatedContact)
                .update(update)
                .build();
        contactSyncBuilder.matchedContact(entityMatch);
      }
    }

    // now we remove all the contacts that are not present in IH
    contactsCopy.forEach(
        contactToRemove -> {
          log.info("Removing contact {}", contactToRemove.getKey());
          removeContactFromEntity(entity, contactToRemove.getKey());
          contactSyncBuilder.removedContact(contactToRemove);
        });

    return contactSyncBuilder.build();
  }

  private <T extends CollectionEntity & Contactable> void addContactToEntity(
      T entity, Contact contact) {
    if (entity instanceof Institution) {
      proxyClient.addContactToInstitution(entity.getKey(), contact);
    } else if (entity instanceof Collection) {
      proxyClient.addContactToCollection(entity.getKey(), contact);
    }
  }

  private <T extends CollectionEntity & Contactable> boolean updateContactInEntity(
      T entity, Contact oldContact, Contact newContact) {
    if (entity instanceof Institution) {
      return proxyClient.updateContactInInstitution(entity.getKey(), oldContact, newContact);
    } else if (entity instanceof Collection) {
      return proxyClient.updateContactInCollection(entity.getKey(), oldContact, newContact);
    }
    return false;
  }

  private <T extends CollectionEntity & Contactable> void removeContactFromEntity(
      T entity, int contactKey) {
    if (entity instanceof Institution) {
      proxyClient.removeContactFromInstitution(entity.getKey(), contactKey);
    } else if (entity instanceof Collection) {
      proxyClient.removeContactFromCollection(entity.getKey(), contactKey);
    }
  }

  private static boolean isValidIhStaff(IHStaff ihStaff) {
    return !Strings.isNullOrEmpty(ihStaff.getFirstName())
        || !Strings.isNullOrEmpty(ihStaff.getMiddleName());
  }
}
