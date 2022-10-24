/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig;
import com.activeviam.pivot.utils.ApplicationInTests;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils.StatisticsSummary;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.pivot.monitoring.impl.MemoryAnalysisService;
import com.qfs.server.cfg.IDatastoreSchemaDescriptionConfig;
import com.qfs.store.IDatastore;
import com.quartetfs.biz.pivot.IMultiVersionActivePivot;
import com.quartetfs.biz.pivot.dto.CellSetDTO;
import com.quartetfs.biz.pivot.query.impl.MDXQuery;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.contributions.impl.ClasspathContributionProvider;
import com.quartetfs.fwk.query.QueryException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestFieldsBookmarkWithDuplicateFieldName extends ATestMemoryStatistic {

  public static final int ADDED_DATA_SIZE = 20;
  private ApplicationInTests<IDatastore> monitoredApp;
  private ApplicationInTests<IDatastore> monitoringApp;
  StatisticsSummary summary;

  @BeforeAll
  public static void setupRegistry() {
    Registry.setContributionProvider(new ClasspathContributionProvider());
  }

  @BeforeEach
  public void setup() throws AgentException {
    this.monitoredApp = createMicroApplicationWithReferenceAndSameFieldName();

    this.monitoredApp
        .getDatabase()
        .edit(
            tm -> {
              IntStream.range(0, ADDED_DATA_SIZE).forEach(i -> tm.add("A", i, i * i));
              IntStream.range(0, 3 * ADDED_DATA_SIZE).forEach(i -> tm.add("B", i, -i * i));
            });

    // Force to discard all versions
    this.monitoredApp.getDatabase().getEpochManager().forceDiscardEpochs(__ -> true);

    // perform GCs before exporting the store data
    performGC();

    final MemoryAnalysisService analysisService =
        (MemoryAnalysisService)
            createService(this.monitoredApp.getDatabase(), this.monitoredApp.getManager());
    final Path exportPath = analysisService.exportMostRecentVersion("testOverview");

    final IMemoryStatistic stats = loadMemoryStatFromFolder(exportPath);
    this.summary = MemoryStatisticsTestUtils.getStatisticsSummary(stats);

    // Start a monitoring datastore with the exported data
    ManagerDescriptionConfig config = new ManagerDescriptionConfig();
    IDatastoreSchemaDescriptionConfig schemaConfig = new MemoryAnalysisDatastoreDescriptionConfig();
    this.monitoringApp =
        ApplicationInTests.builder()
            .withDatastore(schemaConfig.datastoreSchemaDescription())
            .withManager(config.managerDescription())
            .build();

    resources.register(this.monitoringApp).start();

    // Fill the monitoring datastore
    ATestMemoryStatistic.feedMonitoringApplication(
        this.monitoringApp.getDatabase(), List.of(stats), "storeA");

    IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);
    Assertions.assertThat(pivot).isNotNull();
  }

  @Test
  public void testDifferentMemoryUsagesForBothFields() throws QueryException {
    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery usageQuery =
        new MDXQuery(
            "SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS, "
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
