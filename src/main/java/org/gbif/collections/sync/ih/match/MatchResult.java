package org.gbif.collections.sync.ih.match;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

@Builder
@Getter
public class MatchResult {
  IHInstitution ihInstitution;

  @Singular(value = "ihStaff")
  List<IHStaff> ihStaff;

  @Singular(value = "institution")
  Set<Institution> institutions;

  @Singular(value = "collection")
  Set<Collection> collections;

  // it's a bifunction just to make tests easier
  BiFunction<IHStaff, Set<Person>, Set<Person>> staffMatcher;

  public List<CollectionEntity> getAllMatches() {
    List<CollectionEntity> all = new ArrayList<>();
    all.addAll(institutions);
    all.addAll(collections);
    return all;
  }

  public boolean onlyOneInstitutionMatch() {
    return institutions.size() == 1 && collections.isEmpty();
  }

  public boolean onlyOneCollectionMatch() {
    return collections.size() == 1 && institutions.isEmpty();
  }

  public boolean noMatches() {
    return institutions.isEmpty() && collections.isEmpty();
  }

  public boolean institutionAndCollectionMatch() {
    if (institutions.size() != 1 || collections.size() != 1) {
      return false;
    }

    // check that the collection belongs to the institution
    Institution institution = institutions.iterator().next();
    Collection collection = collections.iterator().next();
    return institution.getKey().equals(collection.getInstitutionKey());
  }
}
