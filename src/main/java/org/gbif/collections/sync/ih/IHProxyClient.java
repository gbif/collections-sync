package org.gbif.collections.sync.ih;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.common.BaseProxyClient;
import org.gbif.collections.sync.common.DataLoader;
import org.gbif.collections.sync.common.DataLoader.GrSciCollAndIHData;
import org.gbif.collections.sync.config.IHConfig;
import org.gbif.collections.sync.ih.model.IHStaff;
import org.gbif.collections.sync.parsers.CountryParser;

import lombok.Builder;
import lombok.Getter;

import static org.gbif.collections.sync.common.Utils.mapByIrn;

@Getter
public class IHProxyClient extends BaseProxyClient {

  private final DataLoader dataLoader;
  private Map<String, Set<Institution>> institutionsMapByIrn;
  private Map<String, Set<Collection>> collectionsMapByIrn;
  private Map<String, Set<Person>> grSciCollPersonsByIrn;
  private Set<Person> allGrSciCollPersons;
  private Map<String, List<IHStaff>> ihStaffMapByCode;

  @Builder
  private IHProxyClient(IHConfig ihConfig, CountryParser countryParser, DataLoader dataLoader) {
    super(ihConfig.getSyncConfig());
    if (dataLoader != null) {
      this.dataLoader = dataLoader;
    } else {
      this.dataLoader = DataLoader.create(ihConfig.getSyncConfig());
    }
    loadData();
  }

  private void loadData() {
    GrSciCollAndIHData data = dataLoader.fetchGrSciCollAndIHData();
    institutionsMapByIrn = mapByIrn(data.getInstitutions());
    collectionsMapByIrn = mapByIrn(data.getCollections());
    grSciCollPersonsByIrn = mapByIrn(data.getPersons());
    this.allGrSciCollPersons = new HashSet<>(data.getPersons());
    ihStaffMapByCode = data.getIhStaff().stream().collect(Collectors.groupingBy(IHStaff::getCode));
  }
}