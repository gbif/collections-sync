package org.gbif.collections.sync;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Contact;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.suggestions.CollectionChangeSuggestion;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Singular;

/**
 * Holds the result of a sync.
 *
 * <p>These results are mainly used to export results after a run in order to verify how the process
 * went. They are also useful in dry runs to know what changes it'd be done if we ran the sync
 * process.
 */
@Data
@Builder
public class SyncResult {

  @Singular(value = "collectionOnlyMatch")
  private List<CollectionOnlyMatch> collectionOnlyMatches;

  @Singular(value = "institutionOnlyMatch")
  private List<InstitutionOnlyMatch> institutionOnlyMatches;

  @Singular(value = "instAndCollMatch")
  private List<InstitutionAndCollectionMatch> instAndCollMatches;

  @Singular(value = "noMatch")
  private List<NoEntityMatch> noMatches;

  @Singular(value = "conflict")
  private List<Conflict> conflicts;

  @Singular(value = "failedAction")
  private List<FailedAction> failedActions;

  @Singular(value = "invalidEntity")
  private List<Object> invalidEntities;

  @Data
  @Builder
  public static class CollectionOnlyMatch {
    private EntityMatch<Collection> matchedCollection;
    private ContactMatch contactMatch;
  }

  @Data
  @Builder
  public static class InstitutionOnlyMatch {
    private EntityMatch<Institution> matchedInstitution;
    private Collection newCollection;
    private ContactMatch contactMatch;
  }

  @Data
  @Builder
  public static class InstitutionAndCollectionMatch {
    private EntityMatch<Institution> matchedInstitution;
    private EntityMatch<Collection> matchedCollection;
    private ContactMatch contactMatch;
  }

  @Data
  @Builder
  public static class NoEntityMatch {
    private CollectionChangeSuggestion newChangeSuggestion;
  }

  @Data
  @Builder
  public static class ContactMatch {
    @Singular(value = "newContact")
    private List<Contact> newContacts;

    @Singular(value = "matchedContact")
    private List<EntityMatch<Contact>> matchedContacts;

    @Singular(value = "removedContact")
    private List<Contact> removedContacts;

    @Singular(value = "conflict")
    private List<Conflict> conflicts;
  }

  @Data
  @Builder
  public static class EntityMatch<T> {
    private T matched;
    private T merged;
    // false if no change, true otherwise
    private boolean update;
  }

  @Data
  @AllArgsConstructor
  public static class FailedAction {
    private Object entity;
    private String message;
  }

  @Data
  @AllArgsConstructor
  public static class Conflict {
    private Object entity;
    private List<?> grSciCollEntities;
  }
}
