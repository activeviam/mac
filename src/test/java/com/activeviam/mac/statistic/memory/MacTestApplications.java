/*
 * (C) ActiveViam 2026
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.activeviam.activepivot.core.impl.internal.utils.ApplicationInTests;
import com.activeviam.database.datastore.internal.IInternalDatastore;
import com.activeviam.database.datastore.internal.builder.impl.UnitTestDatastoreBuilder;
import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig;

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
    return ApplicationInTests.builder()
        .withDatastore(new MemoryAnalysisDatastoreDescriptionConfig().datastoreSchemaDescription())
        .withManager(new ManagerDescriptionConfig().managerDescription())
        .build();
  }

  /**
   * Creates the MAC monitoring database, without any cube.
   *
   * @return the monitoring database
   */
  public static IInternalDatastore createMonitoringDatastore() {
    return new UnitTestDatastoreBuilder()
        .setSchemaDescription(
            new MemoryAnalysisDatastoreDescriptionConfig().datastoreSchemaDescription())
        .build();
  }
}
