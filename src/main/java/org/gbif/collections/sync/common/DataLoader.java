package org.gbif.collections.sync.common;

@FunctionalInterface
public interface DataLoader<T> {

  T loadData();

}
