package org.gbif.collections.sync.common;

@FunctionalInterface
public interface MatchResultStrategy<T, R> {

  R handleAndReturn(T matchResult);

  default void handle(T matchResult) {
    handleAndReturn(matchResult);
  }
}
