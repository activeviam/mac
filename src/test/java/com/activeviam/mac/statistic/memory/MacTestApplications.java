/*
 * (C) ActiveViam 2026
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.activeviam.activepivot.core.api.foundry.configurator.ICubeManagerConfigurator;
import com.activeviam.activepivot.core.datastore.api.builder.ApplicationWithDatastore;
import com.activeviam.activepivot.core.datastore.api.foundry.config.IModelConfigurer;
import com.activeviam.activepivot.core.datastore.avinternal.foundry.core.builder.FoundryApplicationDefinition;
import com.activeviam.activepivot.core.datastore.avinternal.foundry.core.builder.FoundryApplicationFactory;
import com.activeviam.activepivot.core.impl.internal.utils.ApplicationInTests;
import com.activeviam.activepivot.core.internal.foundry.naming.GlobalKeyResolver;
import com.activeviam.database.api.schema.IDatabaseSchema;
import com.activeviam.database.datastore.api.foundry.core.configurator.IDatabaseConfigurator;
import com.activeviam.database.datastore.internal.IInternalDatastore;
import com.activeviam.mac.cfg.impl.MacModelConfigurer;
import com.activeviam.tech.core.internal.features.FeatureFlagUtils.Features;
import com.activeviam.tech.mvcc.api.security.impl.BranchPermissionsManager;
import java.util.List;

/** Builds the MAC monitoring application and database used by the tests. */
public final class MacTestApplications {

  private MacTestApplications() {}

  /**
   * Creates the MAC monitoring application, with its database and its cube.
   *
   * <p>The application is not started.
   *
   * @return the monitoring application
   */
  public static ApplicationInTests<IInternalDatastore> createMonitoringApplication() {
    final ApplicationWithDatastore application = createApplication(new MacModelConfigurer());
    return new ApplicationInTests<>(
        (IInternalDatastore) application.getDatastore(), application.getManager());
  }

  /**
   * Creates the MAC monitoring database, without any cube.
   *
   * @return the monitoring database
   */
  public static IInternalDatastore createMonitoringDatastore() {
    final MacModelConfigurer macModel = new MacModelConfigurer();
    final IModelConfigurer databaseOnly =
        new IModelConfigurer() {
          @Override
          public void configureDatabase(final IDatabaseConfigurator database) {
            macModel.configureDatabase(database);
          }

          @Override
          public void configureCubes(
              final ICubeManagerConfigurator cubeManager, final IDatabaseSchema schema) {
            // No cube
          }
        };
    return (IInternalDatastore) createApplication(databaseOnly).getDatastore();
  }

  /**
   * Creates an application with the Foundry API.
   *
   * <p>The Foundry API is only enabled while building the application: the monitored applications
   * of the tests are built with the legacy API, which cannot be used when the Foundry API is
   * enabled.
   */
  private static ApplicationWithDatastore createApplication(final IModelConfigurer model) {
    final String property = Features.FOUNDRY.toProperty();
    final String previousValue = System.getProperty(property);
    MacModelConfigurer.enableFoundry();
    try {
      return FoundryApplicationFactory.createInMemory(
          new FoundryApplicationDefinition(
              model, List.of(), List.of(), List.of(), List.of(), List.of(), List.of()),
          new GlobalKeyResolver(),
          null,
          BranchPermissionsManager.forTests());
    } finally {
      if (previousValue == null) {
        System.clearProperty(property);
      } else {
        System.setProperty(property, previousValue);
      }
    }
  }
}
