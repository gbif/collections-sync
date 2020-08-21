package org.gbif.collections.sync.ih.match.strategy;

import java.util.UUID;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.SyncResult.EntityMatch;
import org.gbif.collections.sync.ih.EntityConverter;
import org.gbif.collections.sync.ih.IHProxyClient;
import org.gbif.collections.sync.ih.match.MatchResult;
import org.gbif.collections.sync.ih.match.StaffMatchResultHandler;
import org.gbif.collections.sync.parsers.CountryParser;

public abstract class IHBaseStrategy {

  // TODO: se puede parametrizar y hacer template con metodo q sea supplier del converter y usar el
  // mismo pa idigbio y ih

  protected final IHProxyClient proxyClient;
  protected final EntityConverter entityConverter;
  protected final StaffMatchResultHandler staffMatchResultHandler;

  protected IHBaseStrategy(IHProxyClient proxyClient) {
    this.proxyClient = proxyClient;
    this.entityConverter = EntityConverter.create(CountryParser.from(proxyClient.getCountries()));
    staffMatchResultHandler =
        new StaffMatchResultHandler(proxyClient.getIhConfig(), proxyClient, entityConverter);
  }

  protected EntityMatch<Institution> updateInstitution(MatchResult matchResult) {
    Institution existingInstitution = matchResult.getInstitutions().iterator().next();
    Institution mergedInstitution =
        entityConverter.convertToInstitution(matchResult.getIhInstitution(), existingInstitution);

    boolean updated = proxyClient.updateInstitution(existingInstitution, mergedInstitution);

    return EntityMatch.<Institution>builder()
        .matched(existingInstitution)
        .merged(mergedInstitution)
        .update(updated)
        .build();
  }

  protected EntityMatch<Collection> updateCollection(MatchResult matchResult) {
    Collection existingCollection = matchResult.getCollections().iterator().next();
    Collection mergedCollection =
        entityConverter.convertToCollection(matchResult.getIhInstitution(), existingCollection);

    boolean updated = proxyClient.updateCollection(existingCollection, mergedCollection);

    return EntityMatch.<Collection>builder()
        .matched(existingCollection)
        .merged(mergedCollection)
        .update(updated)
        .build();
  }

  protected Collection createCollection(MatchResult matchResult, UUID institutionKey) {
    // create new collection linked to the institution
    Collection newCollection =
        entityConverter.convertToCollection(matchResult.getIhInstitution(), institutionKey);

    return proxyClient.createCollection(newCollection);
  }
}
