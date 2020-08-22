package org.gbif.collections.sync.idigbio.match;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.CollectionEntity;
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

  public List<CollectionEntity> getAllMatches() {
    List<CollectionEntity> all = new ArrayList<>();
    all.add(institutionMatched);
    all.add(collectionMatched);
    return all;
  }

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
    return Collections.singleton(institutionMatched);
  }

  @Override
  public Set<Collection> getCollectionMatches() {
    return Collections.singleton(collectionMatched);
  }

  @Override
  public BiFunction<IDigBioRecord, Set<Person>, Set<Person>> getStaffMatcher() {
    return staffMatcher;
  }
}
