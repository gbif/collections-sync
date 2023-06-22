package org.gbif.collections.sync.ih.model;

import java.util.Comparator;

import lombok.Data;

import static org.gbif.collections.sync.common.Utils.countNonNullValues;

/** Models an Index Herbariorum staff. */
@Data
public class IHStaff implements IHEntity {

  private String irn;
  private String code;
  private String lastName;
  private String middleName;
  private String firstName;
  private String birthDate;
  private String correspondent;
  private String position;
  private String specialities;
  private Address address;
  private Contact contact;
  private String dateModified;
  private String currentStatus;

  @Data
  public static class Address {
    private String street;
    private String city;
    private String state;
    private String zipCode;
    private String country;
  }

  @Data
  public static class Contact {
    private String phone;
    private String email;
    private String fax;
  }

  public static final Comparator<IHStaff> COMPARATOR_BY_COMPLETENESS =
      Comparator.comparingLong(IHStaff::countNonNullValuesIHStaff);

  private static long countNonNullValuesIHStaff(IHStaff ihStaff) {
    long countStaff = countNonNullValues(IHStaff.class, ihStaff);
    long countAddress = countNonNullValues(IHStaff.Address.class, ihStaff.getAddress());
    long countContact = countNonNullValues(IHStaff.Contact.class, ihStaff.getContact());

    return countStaff + countAddress + countContact;
  }
}
