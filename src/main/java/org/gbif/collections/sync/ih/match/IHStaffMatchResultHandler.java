package org.gbif.collections.sync.ih.match;

import java.util.List;
import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Contact;
import org.gbif.api.model.collections.Contactable;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.vocabulary.collections.IdType;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.SyncResult.Conflict;
import org.gbif.collections.sync.SyncResult.EntityMatch;
import org.gbif.collections.sync.clients.proxy.IHProxyClient;
import org.gbif.collections.sync.common.match.MatchResult;
import org.gbif.collections.sync.common.match.StaffResultHandler;
import org.gbif.collections.sync.common.parsers.DataParser;
import org.gbif.collections.sync.config.IHConfig;
import org.gbif.collections.sync.ih.IHEntityConverter;
import org.gbif.collections.sync.ih.IHIssueNotifier;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.collections.sync.SyncResult.ContactMatch;
import static org.gbif.collections.sync.common.parsers.DataParser.parseStringList;

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

  @Override
  public <T extends CollectionEntity & Contactable> ContactMatch handleStaff(
      MatchResult<IHInstitution, IHStaff> matchResult, T entity) {

    ContactMatch.ContactMatchBuilder contactSyncBuilder = ContactMatch.builder();
    Set<IHStaff> ihStaffList =
        matchResult.getStaff().stream()
            .filter(
                s ->
                    Strings.isNullOrEmpty(s.getCurrentStatus())
                        || "Active".equals(s.getCurrentStatus()))
            .collect(Collectors.toSet());
    Set<Contact> contactsCopy =
        entity.getContactPersons() != null
            ? new HashSet<>(entity.getContactPersons())
            : new HashSet<>();
    for (IHStaff ihStaff : ihStaffList) {
      if (isInvalidIhStaff(ihStaff)) {
        issueNotifier.createInvalidEntity(
            ihStaff, "Not valid person - first name is required");
        continue;
      }

      // Check and handle invalid emails
      handleInvalidEmails(ihStaff);

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

  @VisibleForTesting
  static boolean isInvalidIhStaff(IHStaff ihStaff) {
    return Strings.isNullOrEmpty(ihStaff.getFirstName())
        && Strings.isNullOrEmpty(ihStaff.getMiddleName());
  }

  @VisibleForTesting
  public void handleInvalidEmails(IHStaff ihStaff) {
    if (ihStaff.getContact() != null && !Strings.isNullOrEmpty(ihStaff.getContact().getEmail())) {
      List<String> emails = parseStringList(ihStaff.getContact().getEmail());
      List<String> validEmails = emails.stream()
          .filter(DataParser::isValidEmail)
          .collect(Collectors.toList());

      // If there are no valid emails, notify and set email to null
      if (validEmails.isEmpty()) {
        issueNotifier.createInvalidEntity(ihStaff, "Invalid email address(es)");
        ihStaff.getContact().setEmail(null);
      } else {
        // Set only valid emails back
        ihStaff.getContact().setEmail(String.join(",", validEmails));
      }
    }
  }

}
