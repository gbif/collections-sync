package org.gbif.collections.sync.ih;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.clients.http.GrSciCollHttpClient;
import org.gbif.collections.sync.clients.http.IHHttpClient;
import org.gbif.collections.sync.common.DataLoader;
import org.gbif.collections.sync.config.IHConfig;
import org.gbif.collections.sync.ih.IHDataLoader.IHData;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IHDataLoader implements DataLoader<IHData> {

  private final GrSciCollHttpClient grSciCollHttpClient;
  private final IHHttpClient ihHttpClient;

  private IHDataLoader(IHConfig ihConfig) {
    grSciCollHttpClient = GrSciCollHttpClient.getInstance(ihConfig.getSyncConfig().getRegistry());
    ihHttpClient = IHHttpClient.getInstance(ihConfig.getIhWsUrl());
  }

  public static IHDataLoader create(IHConfig ihConfig) {
    return new IHDataLoader(ihConfig);
  }

  public IHData loadData() {
    Objects.requireNonNull(grSciCollHttpClient);
    Objects.requireNonNull(ihHttpClient);

    CompletableFuture<List<IHInstitution>> ihInstitutionsFuture =
        CompletableFuture.supplyAsync(ihHttpClient::getInstitutions);
    CompletableFuture<List<IHStaff>> ihStaffFuture =
        CompletableFuture.supplyAsync(ihHttpClient::getStaff);
    CompletableFuture<List<String>> countriesFuture =
        CompletableFuture.supplyAsync(ihHttpClient::getCountries);
    CompletableFuture<List<Institution>> institutionsFuture =
        CompletableFuture.supplyAsync(grSciCollHttpClient::getIhInstitutions);
    CompletableFuture<List<Collection>> collectionsFuture =
        CompletableFuture.supplyAsync(grSciCollHttpClient::getIhCollections);
    CompletableFuture<List<Person>> personsFuture =
      CompletableFuture.supplyAsync(grSciCollHttpClient::getPersons);

    log.info("Loading data from WSs");
    CompletableFuture.allOf(
            ihInstitutionsFuture,
            ihStaffFuture,
            institutionsFuture,
            collectionsFuture,
            countriesFuture)
        .join();

    return new IHData(
        institutionsFuture.join(),
        collectionsFuture.join(),
        personsFuture.join(),
        ihInstitutionsFuture.join(),
        ihStaffFuture.join(),
        countriesFuture.join());
  }

  @AllArgsConstructor
  @Getter
  public static class IHData {
    List<Institution> institutions;
    List<Collection> collections;
    List<Person> persons;
    List<IHInstitution> ihInstitutions;
    List<IHStaff> ihStaff;
    List<String> countries;
  }
}
