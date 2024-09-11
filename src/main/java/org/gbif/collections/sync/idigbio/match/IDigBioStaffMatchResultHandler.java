package org.gbif.collections.sync.idigbio.match;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.CollectionEntity;
import org.gbif.api.model.collections.Contactable;
import org.gbif.api.model.collections.suggestions.CollectionChangeSuggestion;
import org.gbif.collections.sync.SyncResult;
import org.gbif.collections.sync.clients.proxy.IDigBioProxyClient;
import org.gbif.collections.sync.common.match.MatchResult;
import org.gbif.collections.sync.common.match.StaffResultHandler;
import org.gbif.collections.sync.idigbio.IDigBioEntityConverter;
import org.gbif.collections.sync.idigbio.model.IDigBioRecord;

import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IDigBioStaffMatchResultHandler
    implements StaffResultHandler<IDigBioRecord, IDigBioRecord> {

  private final IDigBioProxyClient proxyClient;
  private final IDigBioEntityConverter entityConverter = IDigBioEntityConverter.create();

  public IDigBioStaffMatchResultHandler(IDigBioProxyClient proxyClient) {
    this.proxyClient = proxyClient;
  }

  @Override
  public <T extends CollectionEntity & Contactable> SyncResult.ContactMatch handleStaff(
      MatchResult<IDigBioRecord, IDigBioRecord> matchResult, T entity) {
    // Not implemented since this was a one-time import and the contacts change was introduced after
    // it
    return null;
  }

  @Override
  public CollectionChangeSuggestion handleStaffForCollectionChangeSuggestion(
      MatchResult<IDigBioRecord, IDigBioRecord> matchResult, Collection entity,
      CollectionChangeSuggestion collectionChangeSuggestion) {
    // Not implemented since this was a one-time import
    return null;
  }

  private static boolean containsContact(IDigBioRecord iDigBioRecord) {
    return !Strings.isNullOrEmpty(iDigBioRecord.getContact())
            && !"NA".equals(iDigBioRecord.getContact())
        || !Strings.isNullOrEmpty(iDigBioRecord.getContactEmail())
            && !"NA".equals(iDigBioRecord.getContactEmail())
        || !Strings.isNullOrEmpty(iDigBioRecord.getContactRole());
  }
}
