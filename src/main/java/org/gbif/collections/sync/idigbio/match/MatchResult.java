package org.gbif.collections.sync.idigbio.match;

import java.util.ArrayList;
import java.util.List;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.idigbio.IDigBioRecord;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class MatchResult {

  IDigBioRecord iDigBioRecord;
  Institution institutionMatched;
  Collection collectionMatched;

  public List<CollectionEntity> getAllMatches() {
    List<CollectionEntity> all = new ArrayList<>();
    all.add(institutionMatched);
    all.add(collectionMatched);
    return all;
  }

  public MatchType getMatchType() {
    if (onlyCollectionMatch()) {
      return MatchType.ONLY_COLLECTION;
    }
    if (onlyInstitutionMatch()) {
      return MatchType.ONLY_INSTITUTION;
    }
    if (noMatches()) {
      return MatchType.NO_MATCH;
    }
    if (institutionAndCollectionMatch()) {
      return MatchType.INST_AND_COLL;
    }

    return MatchType.CONFLICT;
  }

  private boolean onlyInstitutionMatch() {
    return institutionMatched != null && collectionMatched == null;
  }

  private boolean onlyCollectionMatch() {
    return collectionMatched != null && institutionMatched == null;
  }

  private boolean noMatches() {
    return institutionMatched == null && collectionMatched == null;
  }

  private boolean institutionAndCollectionMatch() {
    if (institutionMatched == null || collectionMatched == null) {
      return false;
    }

    // check that the collection belongs to the institution
    return institutionMatched.getKey().equals(collectionMatched.getInstitutionKey());
  }

  public enum MatchType {
    ONLY_INSTITUTION,
    ONLY_COLLECTION,
    INST_AND_COLL,
    NO_MATCH,
    CONFLICT;
  }
}
