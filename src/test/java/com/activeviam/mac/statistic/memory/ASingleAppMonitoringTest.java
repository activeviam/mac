/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.store.IDatastore;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.fwk.AgentException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import org.junit.jupiter.api.BeforeEach;

public abstract class ASingleAppMonitoringTest extends AMonitoringTest {

  protected IDatastore monitoredDatastore;
  protected IActivePivotManager monitoredManager;
  protected Path statisticsPath;
  protected Collection<IMemoryStatistic> statistics;

  @BeforeEach
  public void setup() throws AgentException, IOException {
    final IDatastoreSchemaDescription datastoreSchemaDescription = datastoreSchema();
    final IActivePivotManagerDescription managerDescription =
        managerDescription(datastoreSchemaDescription);

    final ExportedApplication monitoredApplication = setupAndExportMonitoredApplication(datastoreSchemaDescription, managerDescription,
        this::beforeExport);
    monitoredDatastore = monitoredApplication.monitoredDatastore;
    monitoredManager = monitoredApplication.monitoredManager;
    statisticsPath = monitoredApplication.statisticsPath;

    setupMac();
    statistics = loadAndImportStatistics(statisticsPath);
  }

  protected abstract IDatastoreSchemaDescription datastoreSchema();

  protected abstract IActivePivotManagerDescription managerDescription(
      final IDatastoreSchemaDescription datastoreSchema);

  protected abstract void beforeExport(
      final IDatastore datastore, final IActivePivotManager manager);
}
