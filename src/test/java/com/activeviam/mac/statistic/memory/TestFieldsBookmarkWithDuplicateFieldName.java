/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.activeviam.copper.testing.CubeTester;
import com.activeviam.mac.statistic.memory.descriptions.MicroApplicationDescriptionWithReferenceAndSameFieldName;
import com.activeviam.mac.statistic.memory.descriptions.MonitoringApplicationDescription;
import com.activeviam.mac.statistic.memory.junit.RegistrySetupExtension;
import com.activeviam.properties.impl.ActiveViamProperty;
import com.activeviam.properties.impl.ActiveViamPropertyExtension;
import com.activeviam.properties.impl.ActiveViamPropertyExtension.ActiveViamPropertyExtensionBuilder;
import com.qfs.junit.LocalResourcesExtension;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils.StatisticsSummary;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.qfs.util.impl.QfsFileTestUtils;
import com.quartetfs.biz.pivot.IMultiVersionActivePivot;
import com.quartetfs.biz.pivot.dto.CellSetDTO;
import com.quartetfs.biz.pivot.query.impl.MDXQuery;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.query.QueryException;
import java.nio.file.Path;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(RegistrySetupExtension.class)
public class TestFieldsBookmarkWithDuplicateFieldName {

  @RegisterExtension
  protected static ActiveViamPropertyExtension propertyExtension =
      new ActiveViamPropertyExtensionBuilder()
          .withProperty(ActiveViamProperty.ACTIVEVIAM_TEST_PROPERTY, true)
          .build();

  @RegisterExtension
  protected final LocalResourcesExtension resources = new LocalResourcesExtension();

  protected static Path tempDir = QfsFileTestUtils.createTempDirectory(TestMACMeasures.class);

  protected Application monitoredApplication;
  protected Application monitoringApplication;
  protected StatisticsSummary statisticsSummary;
  protected CubeTester tester;

  @BeforeEach
  public void setup() throws AgentException, DatastoreTransactionException {
    monitoredApplication = MonitoringTestUtils.setupApplication(
        new MicroApplicationDescriptionWithReferenceAndSameFieldName(),
        resources);

    final Path exportPath = MonitoringTestUtils.exportMostRecentVersion(
        monitoredApplication.getDatastore(),
        monitoredApplication.getManager(),
        tempDir,
        this.getClass().getSimpleName());

    final IMemoryStatistic stats = MonitoringTestUtils.loadMemoryStatFromFolder(exportPath);
    statisticsSummary = MemoryStatisticsTestUtils.getStatisticsSummary(stats);

    monitoringApplication = MonitoringTestUtils
        .setupApplication(new MonitoringApplicationDescription(stats), resources);

    tester = MonitoringTestUtils.createMonitoringCubeTester(monitoringApplication.getManager());
  }

  @Test
  public void testDifferentMemoryUsagesForBothFields() throws QueryException {
    final IMultiVersionActivePivot pivot = tester.pivot();

    final MDXQuery usageQuery =
        new MDXQuery("SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS, "
            + "{"
            + "  ([Owners].[Owner].[Owner].[Store A], [Fields].[Field].[Field].[val]),"
            + "  ([Owners].[Owner].[Owner].[Store B], [Fields].[Field].[Field].[val])"
            + "} ON ROWS "
            + "FROM [MemoryCube]");

    final CellSetDTO result = pivot.execute(usageQuery);

    Assertions.assertThat((long) result.getCells().get(0).getValue())
        .isNotEqualTo((long) result.getCells().get(1).getValue());
  }
}
