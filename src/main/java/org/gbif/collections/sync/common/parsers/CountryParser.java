package org.gbif.collections.sync.common.parsers;

import org.gbif.api.vocabulary.Country;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CountryParser {

  private static final Map<String, Country> COUNTRY_MANUAL_MAPPINGS = new HashMap<>();
  private final Map<String, Country> countryLookup;

  static {
    // manual mapping of countries
    COUNTRY_MANUAL_MAPPINGS.put("U.K.", Country.UNITED_KINGDOM);
    COUNTRY_MANUAL_MAPPINGS.put("UK", Country.UNITED_KINGDOM);
    COUNTRY_MANUAL_MAPPINGS.put("Scotland", Country.UNITED_KINGDOM);
    COUNTRY_MANUAL_MAPPINGS.put("Alderney", Country.UNITED_KINGDOM);
    COUNTRY_MANUAL_MAPPINGS.put("England", Country.UNITED_KINGDOM);
    COUNTRY_MANUAL_MAPPINGS.put("Congo Republic (Congo-Brazzaville)", Country.CONGO);
    COUNTRY_MANUAL_MAPPINGS.put("Republic of Congo-Brazzaville", Country.CONGO);
    COUNTRY_MANUAL_MAPPINGS.put(
        "Democratic Republic of the Congo", Country.CONGO_DEMOCRATIC_REPUBLIC);
    COUNTRY_MANUAL_MAPPINGS.put("Democratic Republic of Congo", Country.CONGO_DEMOCRATIC_REPUBLIC);
    COUNTRY_MANUAL_MAPPINGS.put("Zaire", Country.CONGO_DEMOCRATIC_REPUBLIC);
    COUNTRY_MANUAL_MAPPINGS.put("Italia", Country.ITALY);
    COUNTRY_MANUAL_MAPPINGS.put("Ivory Coast", Country.CÔTE_DIVOIRE);
    COUNTRY_MANUAL_MAPPINGS.put("Laos", Country.LAO);
    COUNTRY_MANUAL_MAPPINGS.put("Republic of Korea", Country.KOREA_SOUTH);
    COUNTRY_MANUAL_MAPPINGS.put("Republic of South Korea", Country.KOREA_SOUTH);
    COUNTRY_MANUAL_MAPPINGS.put("Korea, South", Country.KOREA_SOUTH);
    COUNTRY_MANUAL_MAPPINGS.put("Korea (South)", Country.KOREA_SOUTH);
    COUNTRY_MANUAL_MAPPINGS.put("South Korea", Country.KOREA_SOUTH);
    COUNTRY_MANUAL_MAPPINGS.put("São Tomé e Príncipe", Country.SAO_TOME_PRINCIPE);
    COUNTRY_MANUAL_MAPPINGS.put("Slovak Republic", Country.SLOVAKIA);
    COUNTRY_MANUAL_MAPPINGS.put("México", Country.MEXICO);
    COUNTRY_MANUAL_MAPPINGS.put("French Guiana (France)", Country.FRENCH_GUIANA);
    COUNTRY_MANUAL_MAPPINGS.put("Reunion", Country.RÉUNION);
    COUNTRY_MANUAL_MAPPINGS.put("Palestinian Territories", Country.PALESTINIAN_TERRITORY);
    COUNTRY_MANUAL_MAPPINGS.put("Espanya", Country.SPAIN);
    COUNTRY_MANUAL_MAPPINGS.put("Virgin Islands, U.S.A.", Country.VIRGIN_ISLANDS);
    COUNTRY_MANUAL_MAPPINGS.put("Brasil", Country.BRAZIL);
    COUNTRY_MANUAL_MAPPINGS.put("Türkiye", Country.TURKEY);
    COUNTRY_MANUAL_MAPPINGS.put("Panamá", Country.PANAMA);
  }

  private CountryParser(List<String> countries) {
    countryLookup = mapCountries(countries);

    if (countryLookup.size() < countries.size()) {
      log.warn("We couldn't match all the countries to our enum");
    }
  }

  public static CountryParser from(List<String> countries) {
    return new CountryParser(countries);
  }

  @VisibleForTesting
  static Map<String, Country> mapCountries(List<String> countries) {
    // build map with the titles of the Country enum
    Map<String, Country> titleLookup =
        Maps.uniqueIndex(
            Lists.newArrayList(Country.values()), c -> c != null ? c.getTitle() : null);

    Map<String, Country> mappings = new HashMap<>();

    countries.forEach(
        c -> {
          Country country = titleLookup.get(c);

          if (country == null) {
            country = Country.fromIsoCode(c);
          }
          if (country == null) {
            country = Country.fromIsoCode(c.replaceAll("\\.", ""));
          }
          if (country == null && c.contains(",")) {
            country = titleLookup.get(c.split(",")[0]);
          }
          if (country == null) {
            country =
                Arrays.stream(Country.values())
                    .filter(v -> c.contains(v.getTitle()))
                    .findFirst()
                    .orElse(null);
          }
          if (country == null) {
            country =
                Arrays.stream(Country.values())
                    .filter(v -> v.getTitle().contains(c))
                    .findFirst()
                    .orElse(null);
          }
          if (country == null) {
            country =
                Arrays.stream(Country.values())
                    .filter(v -> v.name().replaceAll("[_]", " ").equalsIgnoreCase(c))
                    .findFirst()
                    .orElse(null);
          }

          if (country != null) {
            mappings.put(c.toLowerCase(), country);
          }
        });

    COUNTRY_MANUAL_MAPPINGS.forEach((k, v) -> mappings.put(k.toLowerCase(), v));

    return mappings;
  }

  public Country parse(String country) {
    return countryLookup.get(country.toLowerCase().trim());
  }
}
