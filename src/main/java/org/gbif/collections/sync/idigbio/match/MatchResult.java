package org.gbif.collections.sync.idigbio.match;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.idigbio.IDigBioRecord;

import java.util.ArrayList;
import java.util.List;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class MatchResult {

  IDigBioRecord iDigBioRecord;
  // TODO: when create maps check that there is only one match per record. If not I have to create a
  // set here
  Institution institutionMatched;
  Collection collectionMatched;

  public boolean onlyInstitutionMatch() {
    return institutionMatched != null && collectionMatched == null;
  }

  public boolean onlyCollectionMatch() {
    return collectionMatched != null && institutionMatched == null;
  }

  public boolean noMatches() {
    return institutionMatched == null && collectionMatched == null;
  }

  public boolean institutionAndCollectionMatch() {
    if (institutionMatched == null || collectionMatched == null) {
      return false;
    }

    // check that the collection belongs to the institution
    return institutionMatched.getKey().equals(collectionMatched.getInstitutionKey());
  }

  public List<CollectionEntity> getAllMatches() {
    List<CollectionEntity> all = new ArrayList<>();
    all.add(institutionMatched);
    all.add(collectionMatched);
    return all;
  }
}
