/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.store.IDatastore;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.fwk.AgentException;
import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;

public abstract class ASingleAppMonitoringTest extends AMonitoringTest {

  @BeforeEach
  public void setup() throws AgentException, IOException {
    final IDatastoreSchemaDescription datastoreSchemaDescription = datastoreSchema();
    final IActivePivotManagerDescription managerDescription =
        managerDescription(datastoreSchemaDescription);

    setupAndExportMonitoredApplication(datastoreSchemaDescription, managerDescription,
        this::beforeExport);
    setupMac();
    loadAndImportStatistics();
  }

  protected abstract IDatastoreSchemaDescription datastoreSchema();

  protected abstract IActivePivotManagerDescription managerDescription(
      final IDatastoreSchemaDescription datastoreSchema);

  protected abstract void beforeExport(
      final IDatastore datastore, final IActivePivotManager manager);
}
