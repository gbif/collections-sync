package org.gbif.collections.sync;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;

import java.util.List;
import java.util.Set;

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

  @Singular(value = "outdatedEntity")
  private List<OutdatedEntity> outdatedEntities;

  @Data
  @Builder
  public static class CollectionOnlyMatch {
    private EntityMatch<Collection> matchedCollection;
    private StaffMatch staffMatch;
  }

  @Data
  @Builder
  public static class InstitutionOnlyMatch {
    private EntityMatch<Institution> matchedInstitution;
    private Collection newCollection;
    private StaffMatch staffMatch;
  }

  @Data
  @Builder
  public static class InstitutionAndCollectionMatch {
    private EntityMatch<Institution> matchedInstitution;
    private EntityMatch<Collection> matchedCollection;
    private StaffMatch staffMatch;
  }

  @Data
  @Builder
  public static class NoEntityMatch {
    private Institution newInstitution;
    private Collection newCollection;
    private StaffMatch staffMatch;
  }

  @Data
  @Builder
  public static class StaffMatch {
    @Singular(value = "newPerson")
    private Set<Person> newPersons;

    @Singular(value = "matchedPerson")
    private Set<EntityMatch<Person>> matchedPersons;

    @Singular(value = "removedPerson")
    private Set<Person> removedPersons;

    @Singular(value = "conflict")
    private Set<Conflict> conflicts;
  }

  @Data
  @Builder
  public static class EntityMatch<T extends CollectionEntity> {
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
    private List<CollectionEntity> grSciCollEntities;
  }

  @Data
  @AllArgsConstructor
  public static class OutdatedEntity {
    private Object outdated;
    private Object updated;
  }
}
