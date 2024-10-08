package org.gbif.collections.sync.ih.match;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.common.match.MatchResult;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;

import java.util.Set;

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
}
