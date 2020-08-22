package org.gbif.collections.sync.ih.match;

import java.util.Set;
import java.util.function.BiFunction;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.common.match.MatchResult;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;

import lombok.Builder;
import lombok.Singular;

@Builder
public class IHMatchResult implements MatchResult<IHInstitution, IHStaff> {
  IHInstitution ihInstitution;

  @Singular(value = "ihStaff")
  Set<IHStaff> ihStaff;

  @Singular(value = "institution")
  Set<Institution> institutions;

  @Singular(value = "collection")
  Set<Collection> collections;

  // it's a bifunction just to make tests easier
  BiFunction<IHStaff, Set<Person>, Set<Person>> staffMatcher;

  @Override
  public IHInstitution getSource() {
    return ihInstitution;
  }

  @Override
  public Set<IHStaff> getStaff() {
    return ihStaff;
  }

  @Override
  public Set<Institution> getInstitutionMatches() {
    return institutions;
  }

  @Override
  public Set<Collection> getCollectionMatches() {
    return collections;
  }

  @Override
  public BiFunction<IHStaff, Set<Person>, Set<Person>> getStaffMatcher() {
    return staffMatcher;
  }
}
