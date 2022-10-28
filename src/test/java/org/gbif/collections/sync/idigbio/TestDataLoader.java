package org.gbif.collections.sync.idigbio;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.common.DataLoader;
import org.gbif.collections.sync.idigbio.IDigBioDataLoader.IDigBioData;
import org.gbif.collections.sync.idigbio.model.IDigBioRecord;

import java.util.List;

import lombok.Builder;
import lombok.Singular;

@Builder
public class TestDataLoader implements DataLoader<IDigBioData> {

  @Singular(value = "institution")
  private final List<Institution> institutions;

  @Singular(value = "collection")
  private final List<Collection> collections;

  @Singular(value = "record")
  private final List<IDigBioRecord> iDigBioRecords;

  @Override
  public IDigBioData loadData() {
    return new IDigBioData(institutions, collections, iDigBioRecords);
  }
}
