package org.gbif.collections.sync.ih;

import org.gbif.api.model.collections.CollectionEntity;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IHSyncResultExporter {

  private static final String SECTION_SEPARATOR =
      "##########################################################################";
  private static final String SUBSECTION_SEPARATOR =
      "--------------------------------------------------------------------------";
  private static final String LINE_STARTER = ">";
  private static final String SIMPLE_INDENT = "\t";
  private static final String DOUBLE_INDENT = "\t\t";

  public static void exportResultsToFile(IHSyncResult result, Path filePath) {

    try (BufferedWriter writer = Files.newBufferedWriter(filePath)) {

      // Summary
      printWithNewLineAfter(writer, "IH Sync " + LocalDateTime.now());
      printWithNewLineAfter(writer, "Summary:");
      printWithNewLineAfter(writer, SUBSECTION_SEPARATOR);
      printWithNewLineAfter(
          writer, "Collection Only Matches: " + result.getCollectionOnlyMatches().size());
      printWithNewLineAfter(
          writer, "Institution Only Matches: " + result.getInstitutionOnlyMatches().size());
      printWithNewLineAfter(
          writer, "Institution & Collection Matches: " + result.getInstAndCollMatches().size());
      printWithNewLineAfter(writer, "No Matches: " + result.getNoMatches().size());
      printWithNewLineAfter(writer, "Conflicts: " + result.getConflicts().size());
      printWithNewLineAfter(writer, "Failed Actions: " + result.getFailedActions().size());
      writer.newLine();
      writer.newLine();

      // Collection Only Matches
      printSectionTitle(
          writer, "Collection Only Matches: " + result.getCollectionOnlyMatches().size());
      result
          .getCollectionOnlyMatches()
          .forEach(
              m -> {
                printEntityMatch(writer, m.getMatchedCollection());
                printStaffMatch(writer, m.getStaffMatch());
              });

      writer.newLine();
      printSectionTitle(
          writer, "Institution Only Matches: " + result.getInstitutionOnlyMatches().size());
      result
          .getInstitutionOnlyMatches()
          .forEach(
              m -> {
                printEntityMatch(writer, m.getMatchedInstitution());
                printStaffMatch(writer, m.getStaffMatch());
                printEntity(writer, "New Collection", m.getNewCollection());
              });

      writer.newLine();
      printSectionTitle(
          writer, "Institution & Collection Matches: " + result.getInstAndCollMatches().size());
      result
          .getInstAndCollMatches()
          .forEach(
              m -> {
                printEntityMatch(writer, m.getMatchedInstitution());
                printStaffMatch(writer, m.getStaffMatchInstitution());
                printEntityMatch(writer, m.getMatchedCollection());
                printStaffMatch(writer, m.getStaffMatchCollection());
              });

      writer.newLine();
      printSectionTitle(writer, "No Matches: " + result.getNoMatches().size());
      result
          .getNoMatches()
          .forEach(
              m -> {
                printEntity(writer, "New Institution", m.getNewInstitution());
                printEntity(writer, "New Collection", m.getNewCollection());
                printStaffMatch(writer, m.getStaffMatch());
              });

      // Conflicts
      printSection(writer, "General Conflicts", result.getConflicts());

      // fails
      printSection(writer, "Failed Actions", result.getFailedActions());

    } catch (Exception e) {
      log.warn("Couldn't save diff results", e);
    }
  }

  private static void printSectionTitle(BufferedWriter writer, String title) throws IOException {
    writer.write(title);
    writer.newLine();
    writer.write(SECTION_SEPARATOR);
    writer.newLine();
    writer.write(LINE_STARTER);
  }

  private static void printSubsectionTitle(BufferedWriter writer, String title) throws IOException {
    writer.write(DOUBLE_INDENT + title);
    writer.newLine();
    writer.write(DOUBLE_INDENT + SUBSECTION_SEPARATOR);
    writer.newLine();
    writer.write(DOUBLE_INDENT + LINE_STARTER);
  }

  private static <T> void printSection(BufferedWriter writer, String title, List<T> collection)
      throws IOException {
    writer.newLine();
    printSectionTitle(writer, title + ": " + collection.size());
    printCollection(writer, collection);
    writer.newLine();
  }

  private static <T> void printSubsection(BufferedWriter writer, String title, List<T> collection)
      throws IOException {
    writer.newLine();
    printSubsectionTitle(writer, title + ": " + collection.size());
    printCollectionSubsection(writer, collection);
    writer.newLine();
  }

  private static <T> void printCollection(BufferedWriter writer, List<T> collection)
      throws IOException {
    for (T e : collection) {
      writer.write(LINE_STARTER);
      printWithNewLineAfter(writer, e.toString());
    }
  }

  private static <T> void printCollectionSubsection(BufferedWriter writer, List<T> collection)
      throws IOException {
    for (T e : collection) {
      writer.write(DOUBLE_INDENT + LINE_STARTER);
      printWithNewLineAfter(writer, DOUBLE_INDENT + e.toString());
    }
  }

  private static void printStaffMatch(BufferedWriter writer, IHSyncResult.StaffMatch staffMatch) {
    try {
      printWithNewLineAfter(writer, ">>> Staff");
      printSubsection(writer, "New Persons", staffMatch.getNewPersons());
      printSubsection(writer, "Matched Persons", staffMatch.getMatchedPersons());
      staffMatch.getMatchedPersons().stream()
          .sorted(Comparator.comparing(IHSyncResult.EntityMatch::isUpdate))
          .forEach(m -> printEntityMatch(writer, m));
      printSubsection(writer, "Removed Persons", staffMatch.getRemovedPersons());
      printSubsection(writer, "Staff Conflicts", staffMatch.getConflicts());
    } catch (IOException e) {
      log.warn("Couldn't print staff match {}", staffMatch, e);
    }
  }

  private static <T extends CollectionEntity> void printEntityMatch(
      BufferedWriter writer, IHSyncResult.EntityMatch<T> entityMatch) {
    try {
      if (entityMatch.isUpdate()) {
        printWithNewLineAfter(writer, "Updated");
        printWithNewLineAfter(writer, SIMPLE_INDENT + "OLD: " + entityMatch.getMatched());
        printWithNewLineAfter(writer, SIMPLE_INDENT + "NEW: " + entityMatch.getMerged());
      } else {
        printWithNewLineAfter(writer, "No Change: " + entityMatch.getMatched());
      }
    } catch (IOException e) {
      log.warn("Couldn't print entity match {}", entityMatch, e);
    }
  }

  private static <T extends CollectionEntity> void printEntity(
      BufferedWriter writer, String text, T entity) {
    try {
      printWithNewLineAfter(writer, text);
      printWithNewLineAfter(writer, entity.toString());
    } catch (IOException e) {
      log.warn("Couldn't print entity {}", entity, e);
    }
  }

  private static void printWithNewLineAfter(BufferedWriter writer, String text) throws IOException {
    writer.write(text);
    writer.newLine();
  }
}
