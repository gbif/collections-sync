package org.gbif.collections.sync.ih.match;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.collections.sync.clients.proxy.IHProxyClient;
import org.gbif.collections.sync.common.parsers.CountryParser;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHStaff;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import lombok.AllArgsConstructor;

/** Matches IH entities to GrSciColl ones. */
public class Matcher {

  private final IHProxyClient proxyClient;
  private final CountryParser countryParser;

  private Matcher(IHProxyClient proxyClient) {
    this.countryParser = CountryParser.from(proxyClient.getCountries());
    this.proxyClient = proxyClient;
  }

  public static Matcher create(IHProxyClient proxyClient) {
    return new Matcher(proxyClient);
  }

  public IHMatchResult match(IHInstitution ihInstitution) {
    // find matches
    Set<Institution> institutionsMatched =
        proxyClient
            .getInstitutionsMapByIrn()
            .getOrDefault(ihInstitution.getIrn(), Collections.emptySet());
    Set<Collection> collectionsMatched =
        proxyClient
            .getCollectionsMapByIrn()
            .getOrDefault(ihInstitution.getIrn(), Collections.emptySet());

    return IHMatchResult.builder()
        .ihInstitution(ihInstitution)
        .ihStaff(
            proxyClient
                .getIhStaffMapByCode()
                .getOrDefault(ihInstitution.getCode(), Collections.emptyList()))
        .institutions(institutionsMatched)
        .collections(collectionsMatched)
        .build();
  }

  @AllArgsConstructor
  static class IHStaffToMatch {
    IHStaff ihStaff;
    UUID institutionMatched;
    UUID collectionMatched;

    IHStaffToMatch(IHStaff ihStaff) {
      this.ihStaff = ihStaff;
    }
  }
}
