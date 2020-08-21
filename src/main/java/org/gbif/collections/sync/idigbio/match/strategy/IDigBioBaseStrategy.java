package org.gbif.collections.sync.idigbio.match.strategy;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.SyncResult.EntityMatch;
import org.gbif.collections.sync.idigbio.EntityConverter;
import org.gbif.collections.sync.idigbio.IDigBioProxyClient;
import org.gbif.collections.sync.idigbio.match.MatchResult;
import org.gbif.collections.sync.idigbio.match.StaffMatchResultHandler;

public class IDigBioBaseStrategy {

  protected IDigBioProxyClient proxyClient;
  protected final StaffMatchResultHandler staffMatchResultHandler;

  protected IDigBioBaseStrategy(IDigBioProxyClient proxyClient) {
    this.proxyClient = proxyClient;
    this.staffMatchResultHandler = new StaffMatchResultHandler(proxyClient);
  }

  protected EntityMatch<Collection> updateCollection(MatchResult matchResult) {
    Collection mergedCollection =
        EntityConverter.convertToCollection(
            matchResult.getCollectionMatched(), matchResult.getIDigBioRecord());

    boolean updated =
        proxyClient.updateCollection(matchResult.getCollectionMatched(), mergedCollection);

    return EntityMatch.<Collection>builder()
        .matched(matchResult.getCollectionMatched())
        .merged(mergedCollection)
        .update(updated)
        .build();
  }

  protected EntityMatch<Institution> updateInstitution(MatchResult matchResult) {
    Institution mergedInstitution =
        EntityConverter.convertToInstitution(
            matchResult.getInstitutionMatched(), matchResult.getIDigBioRecord());

    boolean updated =
        proxyClient.updateInstitution(matchResult.getInstitutionMatched(), mergedInstitution);

    return EntityMatch.<Institution>builder()
        .matched(matchResult.getInstitutionMatched())
        .merged(mergedInstitution)
        .update(updated)
        .build();
  }

  protected Collection createCollection(MatchResult matchResult) {
    Collection newCollection =
        EntityConverter.convertToCollection(
            matchResult.getIDigBioRecord(), matchResult.getInstitutionMatched());

    return proxyClient.createCollection(newCollection);
  }
}
