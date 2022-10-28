package org.gbif.collections.sync.idigbio.match;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.registry.Identifier;
import org.gbif.collections.sync.clients.proxy.IDigBioProxyClient;
import org.gbif.collections.sync.common.Utils;
import org.gbif.collections.sync.idigbio.model.IDigBioRecord;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import static org.gbif.collections.sync.idigbio.IDigBioUtils.IDIGBIO_NO_CODE;
import static org.gbif.collections.sync.idigbio.IDigBioUtils.IS_IDIGBIO_COLLECTION_UUID_MT;
import static org.gbif.collections.sync.idigbio.IDigBioUtils.getIdigbioCodes;

@Slf4j
public class Matcher {

  private final IDigBioProxyClient proxyClient;

  public Matcher(IDigBioProxyClient proxyClient) {
    this.proxyClient = proxyClient;
  }

  public IDigBioMatchResult match(IDigBioRecord iDigBioRecord) {
    IDigBioMatchResult.IDigBioMatchResultBuilder result =
        IDigBioMatchResult.builder().iDigBioRecord(iDigBioRecord).proxyClient(proxyClient);

    Institution institutionMatch =
        proxyClient.getInstitutionsByKey().get(iDigBioRecord.getGrbioInstMatch());
    if (institutionMatch == null) {
      fixNullCodes(iDigBioRecord);
      institutionMatch = matchWithNewInstitutions(iDigBioRecord);
    }

    if (institutionMatch != null) {
      result.institutionMatched(institutionMatch);
      // we try to find a match among the institution collections
      matchCollection(institutionMatch.getKey(), iDigBioRecord)
          .ifPresent(result::collectionMatched);
    }

    return result.build();
  }

  private Institution matchWithNewInstitutions(IDigBioRecord iDigBioRecord) {
    // we try with the newly created institutions
    List<String> iDigBioCodes = getIdigbioCodes(iDigBioRecord.getInstitutionCode());
    String instUniqueNameUuid = iDigBioRecord.getUniqueNameUuid();
    Predicate<List<Identifier>> containsIdentifier =
        ids ->
            Strings.isNullOrEmpty(iDigBioRecord.getUniqueNameUuid())
                || ids.stream()
                    .anyMatch(identifier -> identifier.getIdentifier().equals(instUniqueNameUuid));

    List<Institution> institutionsMatched =
        proxyClient.getNewlyCreatedIDigBioInstitutions().stream()
            .filter(
                i ->
                    iDigBioCodes.contains(i.getCode())
                        && i.getName().equals(iDigBioRecord.getInstitution())
                        && containsIdentifier.test(i.getIdentifiers()))
            .collect(Collectors.toList());

    if (institutionsMatched.size() > 1) {
      log.warn("Multiple candidates for record {}: {}", iDigBioRecord, institutionsMatched);
    } else if (institutionsMatched.size() == 1) {
      return institutionsMatched.get(0);
    }
    return null;
  }

  private Optional<Collection> matchCollection(UUID institutionKey, IDigBioRecord iDigBioRecord) {
    // try first with machine tags
    String iDigBioCollectionUuid = iDigBioRecord.getCollectionUuid();
    if (!Strings.isNullOrEmpty(iDigBioCollectionUuid)) {
      Collection collection = proxyClient.getCollectionsByIDigBioUuid().get(iDigBioCollectionUuid);
      if (collection != null) {
        return Optional.of(collection);
      }
    }

    // if no machine tags found, we try with the collections of the institution matched
    Set<Collection> collections = proxyClient.getCollectionsByInstitution().get(institutionKey);
    if (collections == null) {
      return Optional.empty();
    }

    // filter the ones created by this import, we only want to match with existing ones
    collections =
        collections.stream()
            .filter(
                c ->
                    c.getCreatedBy() != null // needed for dryRun
                        && !c.getCreatedBy()
                            .equals(
                                proxyClient
                                    .getIDigBioConfig()
                                    .getSyncConfig()
                                    .getRegistry()
                                    .getWsUser()))
            .collect(Collectors.toSet());

    List<String> iDigBioCodes = getIdigbioCodes(iDigBioRecord.getCollectionCode());

    List<Collection> matches = null;
    if (!Strings.isNullOrEmpty(iDigBioRecord.getSameAs())
        && iDigBioRecord.getSameAs().contains("irn=")) {

      if (!Strings.isNullOrEmpty(iDigBioRecord.getCollection())
          && !iDigBioRecord.getCollection().toLowerCase().contains("herbari")
          && !iDigBioRecord.getCollection().toLowerCase().startsWith("botany")) {
        return Optional.empty();
      }

      if (iDigBioCodes.isEmpty()) {
        iDigBioCodes.addAll(getIdigbioCodes(iDigBioRecord.getInstitutionCode()));
      }

      String irn = iDigBioRecord.getSameAs().split("irn=")[1];
      matches =
          collections.stream()
              .filter(c -> countIdentifierMatches(Utils.encodeIRN(irn), c) > 0)
              .filter(c -> iDigBioCodes.isEmpty() || iDigBioCodes.contains(c.getCode()))
              .filter(c -> c.getMachineTags().stream().noneMatch(IS_IDIGBIO_COLLECTION_UUID_MT))
              .collect(Collectors.toList());
    } else {
      if (iDigBioCodes.isEmpty()) {
        return Optional.empty();
      }

      iDigBioCodes.removeAll(getIdigbioCodes(iDigBioRecord.getInstitutionCode()));

      Predicate<Collection> hasSomeSimilarity =
          c -> {
            long score = stringSimilarity(iDigBioRecord.getCollection(), c.getName());
            score += countIdentifierMatches(iDigBioRecord.getCollectionLsid(), c);
            return score > 0;
          };

      matches =
          collections.stream()
              .filter(c -> iDigBioCodes.contains(c.getCode()))
              .filter(hasSomeSimilarity)
              .collect(Collectors.toList());
    }

    if (matches.size() > 1) {
      log.warn("Idigbio record {} matches with more than 1 collection: {}", iDigBioRecord, matches);
      // we count multiple matches as no match since it's not clear which one to take and we prefer
      // to be cautious and duplicate collections
      return Optional.empty();
    }

    return matches.isEmpty() ? Optional.empty() : Optional.of(matches.get(0));
  }

  private void fixNullCodes(IDigBioRecord iDigBioRecord) {
    if (Strings.isNullOrEmpty(iDigBioRecord.getInstitutionCode())
        && Strings.isNullOrEmpty(iDigBioRecord.getCollectionCode())) {
      iDigBioRecord.setInstitutionCode(IDIGBIO_NO_CODE);
      iDigBioRecord.setCollectionCode(IDIGBIO_NO_CODE);
    }
  }

  @VisibleForTesting
  static long stringSimilarity(String n1, String n2) {
    String n1Normalized = StringUtils.normalizeSpace(n1.toLowerCase());
    String n2Normalized = StringUtils.normalizeSpace(n2.toLowerCase());

    n1Normalized =
        Arrays.stream(n1Normalized.split(" "))
            .filter(v -> v.length() > 3)
            .sorted()
            .collect(Collectors.joining(" "));
    n2Normalized =
        Arrays.stream(n2Normalized.split(" "))
            .filter(v -> v.length() > 3)
            .sorted()
            .collect(Collectors.joining(" "));

    return n1Normalized.contains(n2Normalized) || n2Normalized.contains(n1Normalized) ? 1 : 0;
  }

  @VisibleForTesting
  static long countIdentifierMatches(String identifier, Collection collection) {
    if (Strings.isNullOrEmpty(identifier) || collection.getIdentifiers() == null) {
      return 0;
    }

    return collection.getIdentifiers().stream()
        .filter(
            i -> i.getIdentifier().contains(identifier) || identifier.contains(i.getIdentifier()))
        .count();
  }
}
