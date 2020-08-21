package org.gbif.collections.sync.parsers;

import java.util.List;
import java.util.Map;

import org.gbif.api.vocabulary.Country;
import org.gbif.collections.sync.http.clients.IHHttpClient;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class CountryParserTest {

  @Ignore("Manual test")
  @Test
  public void ihCountryMappingTest() {
    IHHttpClient ihHttpClient =
        IHHttpClient.getInstance("http://sweetgum.nybg.org/science/api/v1/");
    List<String> countries = ihHttpClient.getCountries();

    Map<String, Country> mappings = CountryParser.mapCountries(countries);

    assertTrue(mappings.size() >= countries.size());
  }
}
