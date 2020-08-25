package org.gbif.collections.sync.idigbio.match;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiFunction;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.common.match.MatchResult;
import org.gbif.collections.sync.idigbio.model.IDigBioRecord;

import lombok.Builder;

@Builder
public class IDigBioMatchResult implements MatchResult<IDigBioRecord, IDigBioRecord> {

  IDigBioRecord iDigBioRecord;
  Institution institutionMatched;
  Collection collectionMatched;
  BiFunction<IDigBioRecord, Set<Person>, Set<Person>> staffMatcher;

  @Override
  public IDigBioRecord getSource() {
    return iDigBioRecord;
  }

  @Override
  public Set<IDigBioRecord> getStaff() {
    return Collections.singleton(iDigBioRecord);
  }

  @Override
  public Set<Institution> getInstitutionMatches() {
    return institutionMatched != null
        ? Collections.singleton(institutionMatched)
        : Collections.emptySet();
  }

  @Override
  public Set<Collection> getCollectionMatches() {
    return collectionMatched != null
        ? Collections.singleton(collectionMatched)
        : Collections.emptySet();
  }

  @Override
  public BiFunction<IDigBioRecord, Set<Person>, Set<Person>> getStaffMatcher() {
    return staffMatcher;
  }
}
