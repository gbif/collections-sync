package org.gbif.collections.sync.idigbio.match;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.clients.proxy.IDigBioProxyClient;
import org.gbif.collections.sync.common.match.MatchResult;
import org.gbif.collections.sync.idigbio.model.IDigBioRecord;

import java.util.Collections;
import java.util.Set;

import lombok.Builder;

@Builder
public class IDigBioMatchResult implements MatchResult<IDigBioRecord, IDigBioRecord> {

  IDigBioRecord iDigBioRecord;
  Institution institutionMatched;
  Collection collectionMatched;
  IDigBioProxyClient proxyClient;

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
    if (institutionMatched == null) {
      return Collections.emptySet();
    }

    if (proxyClient == null) {
      return Collections.singleton(institutionMatched);
    }

    return Collections.singleton(
        proxyClient
            .getInstitutionsByKey()
            .getOrDefault(institutionMatched.getKey(), institutionMatched));
  }

  @Override
  public Set<Collection> getCollectionMatches() {
    if (collectionMatched == null) {
      return Collections.emptySet();
    }

    if (proxyClient == null) {
      return Collections.singleton(collectionMatched);
    }

    return Collections.singleton(
        proxyClient
            .getCollectionsByKey()
            .getOrDefault(collectionMatched.getKey(), collectionMatched));
  }
}
