/*
 * (C) ActiveViam 2026
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.cfg.impl;

import com.activeviam.activepivot.core.api.foundry.configurator.ICubeManagerConfigurator;
import com.activeviam.activepivot.core.datastore.api.foundry.config.IModelConfigurer;
import com.activeviam.database.api.schema.IDatabaseSchema;
import com.activeviam.database.datastore.api.foundry.core.configurator.IDatabaseConfigurator;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig;
import com.activeviam.tech.core.internal.features.FeatureFlagUtils.Features;

/**
 * Defines the data model of the Memory Analysis Cube with the Foundry API: the database storing the
 * memory statistics and the monitoring cube on top of it.
 *
 * @author ActiveViam
 */
public class MacModelConfigurer implements IModelConfigurer {

  /** Enables the experimental Foundry API, required to build the data model of the application. */
  public static void enableFoundry() {
    System.setProperty(Features.FOUNDRY.toProperty(), "true");
  }

  @Override
  public void configureDatabase(final IDatabaseConfigurator database) {
    new MemoryAnalysisDatastoreDescriptionConfig().configureDatabase(database);
  }

  @Override
  public void configureCubes(
      final ICubeManagerConfigurator cubeManager, final IDatabaseSchema schema) {
    new ManagerDescriptionConfig().configureCubes(cubeManager);
  }
}
