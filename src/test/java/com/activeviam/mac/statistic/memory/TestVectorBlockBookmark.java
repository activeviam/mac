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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestVectorBlockBookmark extends ATestMemoryStatistic {

  public static final int ADDED_DATA_SIZE = 20;
  public static final int FIELD_SHARING_COUNT = 2;
  private ApplicationInTests<IDatastore> monitoredApp;
  private ApplicationInTests<IDatastore> monitoringApp;
  StatisticsSummary summary;

  @BeforeAll
  public static void setupRegistry() {
    Registry.setContributionProvider(new ClasspathContributionProvider());
  }

  @BeforeEach
  public void setup() throws AgentException {
    this.monitoredApp = createMicroApplicationWithSharedVectorField();

    this.monitoredApp
        .getDatabase()
        .edit(
            tm ->
                IntStream.range(0, ADDED_DATA_SIZE)
                    .forEach(i -> tm.add("A", i * i, new double[] {i}, new double[] {-i, -i * i})));

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
    final IDatastoreSchemaDescriptionConfig schemaConfig =
        new MemoryAnalysisDatastoreDescriptionConfig();

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

  @AfterEach
  public void tearDown() throws AgentException {
    this.monitoringApp.getDatabase().close();
    this.monitoringApp.getManager().stop();
  }

  @Test
  public void testVectorBlockRecordConsumptionIsZero() throws QueryException {
    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery recordQuery =
        new MDXQuery(
            "SELECT [Components].[Component].[Component].[RECORDS] ON ROWS,"
                + " [Measures].[DirectMemory.SUM] ON COLUMNS"
                + " FROM [MemoryCube]"
                + " WHERE ("
                + "   [Owners].[Owner].[Owner].[Store A],"
                + "   [Fields].[Field].[Field].[vector1]"
                + " )");

    final CellSetDTO result = pivot.execute(recordQuery);

    Assertions.assertThat((long) result.getCells().get(0).getValue()).isZero();
  }

  @Test
  public void testVectorBlockConsumption() throws QueryException {
    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery vectorBlockQuery =
        new MDXQuery(
            "SELECT {"
                + "   [Components].[Component].[ALL].[AllMember],"
                + "   [Components].[Component].[Component].[VECTOR_BLOCK]"
                + " } ON ROWS,"
                + " [Measures].[DirectMemory.SUM] ON COLUMNS"
                + " FROM [MemoryCube]"
                + " WHERE ("
                + "   [Owners].[Owner].[Owner].[Store A],"
                + "   [Fields].[Field].[Field].[vector1]"
                + " )");

    final CellSetDTO result = pivot.execute(vectorBlockQuery);

    Assertions.assertThat((long) result.getCells().get(1).getValue())
        .isEqualTo((long) result.getCells().get(0).getValue());
  }

  @Test
  public void testVectorBlockLength() throws QueryException {
    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery lengthQuery =
        new MDXQuery(
            "SELECT  [Measures].[VectorBlock.Length] ON COLUMNS"
                + " FROM [MemoryCube]"
                + " WHERE ("
                + "   [Owners].[Owner].[Owner].[Store A],"
                + "   [Fields].[Field].[Field].[vector1]"
                + " )");

    final CellSetDTO result = pivot.execute(lengthQuery);

    Assertions.assertThat(CellSetUtils.extractValueFromSingleCellDTO(result))
        .isEqualTo(MICROAPP_VECTOR_BLOCK_SIZE);
  }

  @Test
  public void testVectorBlockRefCount() throws QueryException {
    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery refCountQuery =
        new MDXQuery(
            "SELECT [Measures].[VectorBlock.RefCount] ON COLUMNS"
                + " FROM [MemoryCube]"
                + " WHERE ("
                + "   [Owners].[Owner].[Owner].[Store A],"
                + "   [Fields].[Field].[Field].[vector1]"
                + " )");

    final CellSetDTO refCountResult = pivot.execute(refCountQuery);

    Assertions.assertThat((long) CellSetUtils.extractValueFromSingleCellDTO(refCountResult))
        .isEqualTo(FIELD_SHARING_COUNT * ADDED_DATA_SIZE);
  }
}
