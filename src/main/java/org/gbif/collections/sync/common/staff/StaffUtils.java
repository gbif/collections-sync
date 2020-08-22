package org.gbif.collections.sync.common.staff;

import java.util.List;

import org.gbif.api.model.collections.Person;
import org.gbif.collections.sync.ih.model.IHStaff;

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import static org.gbif.collections.sync.common.parsers.DataParser.normalizeString;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class StaffUtils {

  public static boolean compareLists(List<String> l1, List<String> l2) {
    if (l1 != null && !l1.isEmpty() && l2 != null && !l2.isEmpty()) {
      for (String v1 : l1) {
        for (String v2 : l2) {
          if (v1.equals(v2)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  public static boolean compareStrings(String s1, String s2) {
    return !Strings.isNullOrEmpty(s1) && !Strings.isNullOrEmpty(s2) && s1.equals(s2);
  }

  public static boolean compareStringsPartially(String s1, String s2) {
    return !Strings.isNullOrEmpty(s1)
        && !Strings.isNullOrEmpty(s2)
        && ((s1.startsWith(s2) || s2.startsWith(s1)) || (s1.endsWith(s2) || s2.endsWith(s1)));
  }

  public static boolean compareFullNamePartially(String s1, String s2) {
    if (!Strings.isNullOrEmpty(s1) && !Strings.isNullOrEmpty(s2)) {
      String[] parts1 = s1.split(" ");
      String[] parts2 = s2.split(" ");

      for (String p1 : parts1) {
        for (String p2 : parts2) {
          if (p1.length() >= 5 && p1.equals(p2)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  public static final String concatIHFirstName(IHStaff s) {
    StringBuilder firstNameBuilder = new StringBuilder();
    if (!Strings.isNullOrEmpty(s.getFirstName())) {
      firstNameBuilder.append(s.getFirstName().trim());
      firstNameBuilder.append(" ");
    }
    if (!Strings.isNullOrEmpty(s.getMiddleName())) {
      firstNameBuilder.append(s.getMiddleName().trim());
    }

    String firstName = firstNameBuilder.toString();
    if (Strings.isNullOrEmpty(firstName)) {
      return null;
    }

    return normalizeName(firstName);
  }

  public static final String concatIHName(IHStaff s) {
    String firstName = concatIHFirstName(s);
    StringBuilder fullNameBuilder = new StringBuilder(firstName);
    if (!Strings.isNullOrEmpty(s.getLastName())) {
      fullNameBuilder.append(" ");
      fullNameBuilder.append(s.getLastName().trim());
    }

    String fullName = fullNameBuilder.toString();
    if (Strings.isNullOrEmpty(fullName)) {
      return null;
    }

    return normalizeName(fullName);
  }

  public static final String concatPersonName(Person p) {
    StringBuilder fullNameBuilder = new StringBuilder();
    if (!Strings.isNullOrEmpty(p.getFirstName())) {
      // persons in our registry usually have the full name in this field and can contain
      // multiple whitespaces, so we need to normalize them
      fullNameBuilder.append(p.getFirstName());
      fullNameBuilder.append(" ");
    }
    if (!Strings.isNullOrEmpty(p.getLastName())) {
      fullNameBuilder.append(p.getLastName().trim());
    }

    String fullName = fullNameBuilder.toString();
    if (Strings.isNullOrEmpty(fullName)) {
      return null;
    }

    return normalizeName(fullName);
  }

  public static String normalizeName(String name) {
    if (Strings.isNullOrEmpty(name)) {
      return name;
    }
    return normalizeString(name.replaceAll("[,.]", ""));
  }
}
