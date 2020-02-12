package org.gbif.collections.sync.ih;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.ih.model.IHEntity;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Singular;

@Data
@Builder
public class IHSyncResult {

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
    private StaffMatch staffMatchInstitution;
    private EntityMatch<Collection> matchedCollection;
    private StaffMatch staffMatchCollection;
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
    private List<Person> newPersons;

    @Singular(value = "matchedPerson")
    private List<EntityMatch<Person>> matchedPersons;

    @Singular(value = "removedPerson")
    private List<Person> removedPersons;

    @Singular(value = "conflict")
    private List<Conflict> conflicts;
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
    private IHEntity ihEntity;
    private List<CollectionEntity> grSciCollEntities;
  }
}
