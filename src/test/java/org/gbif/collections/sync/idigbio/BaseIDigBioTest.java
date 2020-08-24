package org.gbif.collections.sync.idigbio;

import org.gbif.collections.sync.config.IDigBioConfig;

import static org.gbif.collections.sync.TestUtils.createTestSyncConfig;

public class BaseIDigBioTest {

  protected static final IDigBioConfig iDigBioConfig = createConfig();

  private static IDigBioConfig createConfig() {
    IDigBioConfig iDigBioConfig = new IDigBioConfig();
    iDigBioConfig.setSyncConfig(createTestSyncConfig());
    return iDigBioConfig;
  }
}
