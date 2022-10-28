package org.gbif.collections.sync.common.match;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Institution;

import java.util.*;

public interface MatchResult<S, R> {

  S getSource();

  Set<R> getStaff();

  Set<Institution> getInstitutionMatches();

  Set<Collection> getCollectionMatches();

  default List<CollectionEntity> getAllMatches() {
    List<CollectionEntity> all = new ArrayList<>();
    if (getInstitutionMatches() != null) {
      all.addAll(getInstitutionMatches());
    }
    if (getCollectionMatches() != null) {
      all.addAll(getCollectionMatches());
    }
    return all;
  }

  default boolean onlyOneInstitutionMatch() {
    return getInstitutionMatches().size() == 1 && getCollectionMatches().isEmpty();
  }

  default boolean onlyOneCollectionMatch() {
    return getCollectionMatches().size() == 1 && getInstitutionMatches().isEmpty();
  }

  default boolean noMatches() {
    return getInstitutionMatches().isEmpty() && getCollectionMatches().isEmpty();
  }

  default boolean institutionAndCollectionMatch() {
    if (getInstitutionMatches().size() != 1 || getCollectionMatches().size() != 1) {
      return false;
    }

    // check that the collection belongs to the institution
    Institution institution = getInstitutionMatches().iterator().next();
    Collection collection = getCollectionMatches().iterator().next();

    UUID institutionKey = institution != null ? institution.getKey() : null;
    UUID collectionInstitutionKey = collection != null ? collection.getInstitutionKey() : null;

    return Objects.equals(institutionKey, collectionInstitutionKey);
  }
}
