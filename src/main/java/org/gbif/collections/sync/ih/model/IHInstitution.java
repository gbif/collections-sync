package org.gbif.collections.sync.ih.model;

import java.util.List;

import lombok.Data;

/** Models an Index Herbariorum institution. */
@Data
public class IHInstitution implements IHEntity {

  private String irn;
  private String organization;
  private String code;
  private String division;
  private String department;
  private Integer specimenTotal;
  private Address address;
  private Contact contact;
  private Location location;
  private String dateModified;
  private String currentStatus;
  private String taxonomicCoverage;
  private String geography;
  private String notes;
  private String dateFounded;
  private List<String> incorporatedHerbaria;
  private List<String> importantCollectors;
  private CollectionSummary collectionsSummary;
  private String cites;

  @Data
  public static class Address {
    private String physicalStreet;
    private String physicalCity;
    private String physicalState;
    private String physicalZipCode;
    private String physicalCountry;
    private String postalStreet;
    private String postalCity;
    private String postalState;
    private String postalZipCode;
    private String postalCountry;
  }

  @Data
  public static class Contact {
    private String phone;
    private String email;
    private String webUrl;
  }

  @Data
  public static class Location {
    private Double lat;
    private Double lon;
  }

  @Data
  public static class CollectionSummary {
    private int numAlgae;
    private int numAlgaeDatabased;
    private int numAlgaeImaged;
    private int numBryos;
    private int numBryosDatabased;
    private int numBryosImaged;
    private int numFungi;
    private int numFungiDatabased;
    private int numFungiImaged;
    private int numPteridos;
    private int numPteridosDatabased;
    private int numPteridosImaged;
    private int numSeedPl;
    private int numSeedPlDatabased;
    private int numSeedPlImaged;
  }

}
