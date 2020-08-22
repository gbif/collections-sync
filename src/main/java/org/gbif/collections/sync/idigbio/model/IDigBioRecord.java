package org.gbif.collections.sync.idigbio.model;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Data;

import static com.fasterxml.jackson.annotation.JsonInclude.Include;

@Data
public class IDigBioRecord {

  private String institution;
  private String collection;

  @JsonProperty("recordsets")
  private String recordSets;

  private String recordsetQuery;

  @JsonProperty("institution_code")
  private String institutionCode;

  @JsonProperty("collection_code")
  private String collectionCode;

  @JsonProperty("collection_uuid")
  private String collectionUuid;

  @JsonProperty("collection_lsid")
  private String collectionLsid;

  @JsonProperty("collection_url")
  private String collectionUrl;

  @JsonProperty("collection_catalog_url")
  private String collectionCatalogUrl;

  private String description;
  private String descriptionForSpecialists;
  private Integer cataloguedSpecimens;
  private String knownToContainTypes;
  private String taxonCoverage;

  @JsonProperty("geographic_range")
  private String geographicRange;

  private String collectionExtent;

  private String contact;

  @JsonProperty("contact_role")
  private String contactRole;

  @JsonProperty("contact_email")
  private String contactEmail;

  @JsonUnwrapped(prefix = "mailing_")
  private Address mailingAddress;

  @JsonUnwrapped(prefix = "physical_")
  private Address physicalAddress;

  @JsonProperty("UniqueNameUUID")
  private String uniqueNameUuid;

  private String attributionLogoURL;
  private String providerManagedID;
  private String derivedFrom;
  private String sameAs;
  private String flags;
  private String portalDisplay;
  private Double lat;
  private Double lon;

  @JsonInclude(Include.NON_NULL)
  @JsonProperty("_grbioInstMatch")
  @JsonDeserialize(using = InstMatchDeserializer.class)
  private UUID grbioInstMatch;

  @JsonInclude(Include.NON_NULL)
  @JsonProperty("_matchReason")
  private String matchReason;

  private LocalDateTime modifiedDate;

  @Data
  public static class Address {
    private String address;
    private String city;
    private String state;
    private String zip;
  }

  public static class InstMatchDeserializer extends JsonDeserializer<UUID> {

    @Override
    public UUID deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      if (p.getText().startsWith("[")) {
        return UUID.fromString(p.nextTextValue());
      } else {
        return UUID.fromString(p.getText());
      }
    }
  }
}
