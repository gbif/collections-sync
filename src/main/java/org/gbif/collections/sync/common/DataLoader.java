package org.gbif.collections.sync.common;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.config.IHConfig;
import org.gbif.collections.sync.config.SyncConfig;
import org.gbif.collections.sync.http.clients.GrSciCollHttpClient;
import org.gbif.collections.sync.http.clients.IHHttpClient;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataLoader {

  private final GrSciCollHttpClient grSciCollHttpClient;
  private final IHHttpClient ihHttpClient;

  private DataLoader(SyncConfig syncConfig) {
    grSciCollHttpClient = GrSciCollHttpClient.getInstance(syncConfig);
    this.ihHttpClient = null;
  }

  private DataLoader(IHConfig ihConfig) {
    grSciCollHttpClient = GrSciCollHttpClient.getInstance(ihConfig.getSyncConfig());
    ihHttpClient = IHHttpClient.getInstance(ihConfig.getIhWsUrl());
  }

  public static DataLoader create(SyncConfig syncConfig) {
    return new DataLoader(syncConfig);
  }

  public static DataLoader create(IHConfig ihConfig) {
    return new DataLoader(ihConfig);
  }

  public GrSciCollData fetchGrSciCollData() {
    Objects.requireNonNull(grSciCollHttpClient);

    CompletableFuture<List<Institution>> institutionsFuture =
        CompletableFuture.supplyAsync(grSciCollHttpClient::getInstitutions);
    CompletableFuture<List<Collection>> collectionsFuture =
        CompletableFuture.supplyAsync(grSciCollHttpClient::getCollections);
    CompletableFuture<List<Person>> personsFuture =
        CompletableFuture.supplyAsync(grSciCollHttpClient::getPersons);

    log.info("Loading data from WSs");
    CompletableFuture.allOf(institutionsFuture, collectionsFuture, personsFuture).join();

    return new GrSciCollData(
        institutionsFuture.join(), collectionsFuture.join(), personsFuture.join());
  }

  public GrSciCollAndIHData fetchGrSciCollAndIHData() {
    CompletableFuture<List<IHInstitution>> ihInstitutionsFuture =
        CompletableFuture.supplyAsync(ihHttpClient::getInstitutions);
    CompletableFuture<List<IHStaff>> ihStaffFuture =
        CompletableFuture.supplyAsync(ihHttpClient::getStaff);
    CompletableFuture<List<Institution>> institutionsFuture =
        CompletableFuture.supplyAsync(grSciCollHttpClient::getInstitutions);
    CompletableFuture<List<Collection>> collectionsFuture =
        CompletableFuture.supplyAsync(grSciCollHttpClient::getCollections);
    CompletableFuture<List<Person>> personsFuture =
        CompletableFuture.supplyAsync(grSciCollHttpClient::getPersons);

    log.info("Loading data from WSs");
    CompletableFuture.allOf(
            ihInstitutionsFuture,
            ihStaffFuture,
            institutionsFuture,
            collectionsFuture,
            personsFuture)
        .join();

    return new GrSciCollAndIHData(
        institutionsFuture.join(),
        collectionsFuture.join(),
        personsFuture.join(),
        ihInstitutionsFuture.join(),
        ihStaffFuture.join());
  }

  @AllArgsConstructor
  @Getter
  public static class GrSciCollAndIHData {
    List<Institution> institutions;
    List<Collection> collections;
    List<Person> persons;
    List<IHInstitution> ihInstitutions;
    List<IHStaff> ihStaff;
  }

  @AllArgsConstructor
  @Getter
  public static class GrSciCollData {
    List<Institution> institutions;
    List<Collection> collections;
    List<Person> persons;
  }
}
