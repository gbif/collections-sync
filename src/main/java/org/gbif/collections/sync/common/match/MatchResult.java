package org.gbif.collections.sync.common.match;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;

public interface MatchResult<S, R> {

  S getSource();

  Set<R> getStaff();

  Set<Institution> getInstitutionMatches();

  Set<Collection> getCollectionMatches();

  BiFunction<R, Set<Person>, Set<Person>> getStaffMatcher();

  default List<CollectionEntity> getAllMatches() {
    List<CollectionEntity> all = new ArrayList<>();
    all.addAll(getInstitutionMatches());
    all.addAll(getCollectionMatches());
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
