package org.gbif.collections.sync.clients.proxy;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.common.DataLoader;
import org.gbif.collections.sync.config.IHConfig;
import org.gbif.collections.sync.ih.IHDataLoader.IHData;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.Builder;
import lombok.Getter;

import static org.gbif.collections.sync.common.Utils.mapByIrn;

@Getter
public class IHProxyClient extends BaseProxyClient {

  private final DataLoader<IHData> dataLoader;
  private final IHConfig ihConfig;
  private List<IHInstitution> ihInstitutions;
  private Map<String, Set<Institution>> institutionsMapByIrn;
  private Map<String, Set<Collection>> collectionsMapByIrn;
  private Map<String, Set<Person>> grSciCollPersonsByIrn;
  private Set<Person> allGrSciCollPersons;
  private Map<String, List<IHStaff>> ihStaffMapByCode;
  private List<String> countries;

  @Builder
  private IHProxyClient(IHConfig ihConfig, DataLoader<IHData> dataLoader) {
    super(ihConfig.getSyncConfig());
    this.ihConfig = ihConfig;
    this.dataLoader = dataLoader;
    loadData();
  }

  private void loadData() {
    IHData data = dataLoader.loadData();
    ihInstitutions = data.getIhInstitutions();
    institutionsMapByIrn = mapByIrn(data.getInstitutions());
    collectionsMapByIrn = mapByIrn(data.getCollections());
    this.allGrSciCollPersons = new HashSet<>(data.getPersons());
    ihStaffMapByCode =
        data.getIhStaff().stream()
            .filter(s -> "Yes".equalsIgnoreCase(s.getCorrespondent()))
            .collect(Collectors.groupingBy(IHStaff::getCode));
    countries = data.getCountries();
  }
}
