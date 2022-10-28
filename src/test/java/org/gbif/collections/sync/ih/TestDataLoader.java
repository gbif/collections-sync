package org.gbif.collections.sync.ih;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Contact;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.common.DataLoader;
import org.gbif.collections.sync.ih.IHDataLoader.IHData;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;

import java.util.List;

import lombok.Builder;
import lombok.Singular;

@Builder
public class TestDataLoader implements DataLoader<IHData> {

  @Singular(value = "institution")
  private final List<Institution> institutions;

  @Singular(value = "collection")
  private final List<Collection> collections;

  @Singular(value = "contact")
  private final List<Contact> contacts;

  @Singular(value = "ihInstitution")
  private final List<IHInstitution> ihInstitutions;

  @Singular(value = "ihStaff")
  private final List<IHStaff> ihStaff;

  @Singular(value = "country")
  private final List<String> countries;

  @Override
  public IHData loadData() {
    return new IHData(institutions, collections, ihInstitutions, ihStaff, countries);
  }
}
