package org.gbif.collections.sync.common.parsers;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.DoubleFunction;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class DataParser {

  public static final DoubleFunction<BigDecimal> TO_BIGDECIMAL =
      v -> BigDecimal.valueOf(v).setScale(6, RoundingMode.CEILING);

  public static final Function<Date, LocalDateTime> TO_LOCAL_DATE_TIME_UTC =
      d -> d.toInstant().atZone(ZoneOffset.UTC).toLocalDateTime();

  private static final Pattern WHITESPACE_PATTERN = Pattern.compile("[\\h\\s+]");
  private static final Pattern CONTAINS_NUMBER = Pattern.compile(".*[0-9].*");
  private static final List<SimpleDateFormat> DATE_FORMATS = new ArrayList<>();

  static {
    // date formats supported
    DATE_FORMATS.add(new SimpleDateFormat("yyyy-MM-dd"));
    DATE_FORMATS.add(new SimpleDateFormat("yyyy-MM"));
    DATE_FORMATS.add(new SimpleDateFormat("dd/MM/yyyy"));
    DATE_FORMATS.add(new SimpleDateFormat("dd MMMM yyyy", Locale.US));
    DATE_FORMATS.add(new SimpleDateFormat("MMMM yyyy", Locale.US));
    DATE_FORMATS.add(new SimpleDateFormat("MMMM yyyy", new Locale("ES")));
    DATE_FORMATS.add(new SimpleDateFormat("yyyy"));
  }

  public static List<String> parseStringList(String stringList) {
    if (Strings.isNullOrEmpty(stringList)) {
      return Collections.emptyList();
    }

    String listNormalized = stringList.replaceAll("[\n;]", ",");
    return Arrays.stream(listNormalized.split(","))
        .filter(DataParser::hasValue)
        .map(DataParser::normalizeString)
        .collect(Collectors.toList());
  }

  public static List<String> getStringList(String stringList) {
    if (Strings.isNullOrEmpty(stringList)) {
      return Collections.emptyList();
    }

    String listNormalized = stringList.replaceAll("[\n;]", ",");
    return Arrays.stream(listNormalized.split(","))
        .filter(DataParser::hasValue)
        .collect(Collectors.toList());
  }

  public static boolean hasValue(String value) {
    return !Strings.isNullOrEmpty(value) && !value.equalsIgnoreCase("null");
  }

  public static String normalizeString(String value) {
    return Strings.isNullOrEmpty(value)
        ? null
        : StringUtils.normalizeSpace(value.toLowerCase()).trim();
  }

  public static boolean isValidEmail(String email) {
    return !Strings.isNullOrEmpty(email) && email.length() >= 5 && email.contains("@");
  }

  public static boolean isValidFax(String fax) {
    return !Strings.isNullOrEmpty(fax)
        && CONTAINS_NUMBER.matcher(fax).matches()
        && fax.length() >= 5;
  }

  public static boolean isValidPhone(String phone) {
    return !Strings.isNullOrEmpty(phone)
        && CONTAINS_NUMBER.matcher(phone).matches()
        && phone.length() >= 5;
  }

  public static Optional<URI> parseUri(String uri) {
    return parseUri(uri, "Invalid URI");
  }

  public static Optional<URI> parseUri(String uri, String errorMsg) {
    // we try to clean the URL first
    String webUrl = WHITESPACE_PATTERN.matcher(uri.trim()).replaceAll("");

    if (webUrl.startsWith("http//:")) {
      webUrl = webUrl.replace("http//:", "http://");
    }

    if (!webUrl.startsWith("http") && !webUrl.startsWith("Http") && !webUrl.startsWith("HTTP")) {
      webUrl = "http://" + webUrl;
    }

    try {
      return Optional.of(URI.create(webUrl));
    } catch (Exception ex) {
      log.warn("{}: {}", errorMsg, webUrl, ex);
      return Optional.empty();
    }
  }

  public static Date parseDate(String dateAsString) {
    return parseDate(dateAsString, "Invalid date");
  }

  public static Date parseDate(String dateAsString, String errorMsg) {
    if (!hasValue(dateAsString)) {
      return null;
    }

    // some dates came with a dot at the end
    if (dateAsString.endsWith(".")) {
      dateAsString = dateAsString.substring(0, dateAsString.length() - 1);
    }

    for (SimpleDateFormat dateFormat : DATE_FORMATS) {
      try {
        return dateFormat.parse(dateAsString);
      } catch (Exception e) {
        log.debug("Failed parsing date {}: ", dateAsString, e);
      }
    }

    log.warn("{}: {}", errorMsg, dateAsString);
    return null;
  }

  public static Optional<String> getFirstString(String stringList) {
    if (!hasValue(stringList)) {
      return Optional.empty();
    }

    String firstValue = null;
    if (stringList.contains(",")) {
      firstValue = stringList.split(",")[0];
    } else if (stringList.contains(";")) {
      firstValue = stringList.split(";")[0];
    } else if (stringList.contains("\n")) {
      firstValue = stringList.split("\n")[0];
    }

    if (hasValue(firstValue)) {
      return Optional.of(firstValue.trim());
    }

    return Optional.of(stringList.trim());
  }

  public static String getStringValue(String value) {
    return hasValue(value) ? value.trim() : null;
  }

  public static Optional<String> getStringValueOpt(String value) {
    return hasValue(value) ? Optional.of(value.trim()) : Optional.empty();
  }

  public static List<String> getListValue(List<String> list) {
    return list != null ? list : Collections.emptyList();
  }

  public static String cleanString(String string) {
    return Strings.isNullOrEmpty(string) ? null : StringUtils.normalizeSpace(string).trim();
  }
}