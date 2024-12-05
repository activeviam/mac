/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import static org.assertj.core.api.Assertions.assertThat;

import com.activeviam.activepivot.core.impl.internal.utils.ApplicationInTests;
import com.activeviam.activepivot.core.intf.api.cube.IMultiVersionActivePivot;
import com.activeviam.activepivot.server.impl.api.query.MDXQuery;
import com.activeviam.activepivot.server.impl.api.query.MdxQueryUtil;
import com.activeviam.activepivot.server.impl.private_.observability.memory.MemoryAnalysisService;
import com.activeviam.activepivot.server.intf.api.dto.CellSetDTO;
import com.activeviam.activepivot.server.spring.api.config.IDatastoreSchemaDescriptionConfig;
import com.activeviam.database.datastore.api.IDatastore;
import com.activeviam.database.datastore.internal.IInternalDatastore;
import com.activeviam.database.datastore.internal.monitoring.MemoryStatisticsTestUtils;
import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.cfg.impl.RegistryInitializationConfig;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig;
import com.activeviam.tech.core.api.agent.AgentException;
import com.activeviam.tech.core.api.query.QueryException;
import com.activeviam.tech.observability.internal.memory.AMemoryStatistic;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestVectorBlockBookmark extends ATestMemoryStatistic {

  public static final int ADDED_DATA_SIZE = 40;
  public static final int FIELD_SHARING_COUNT = 2;
  private ApplicationInTests<IDatastore> monitoredApp;
  private ApplicationInTests<IInternalDatastore> monitoringApp;
  MemoryStatisticsTestUtils.StatisticsSummary summary;

  @BeforeAll
  public static void setupRegistry() {
    RegistryInitializationConfig.setupRegistry();
  }

  @BeforeEach
  public void setup() {
    this.monitoredApp = createMicroApplicationWithSharedVectorField();

    this.monitoredApp
        .getDatabase()
        .edit(
            tm ->
                IntStream.range(0, ADDED_DATA_SIZE)
                    .forEach(i -> tm.add("A", i * i, new double[] {i}, new double[] {-i, -i * i})));

    // Force to discard all versions
    this.monitoredApp.getDatabase().getEpochManager().forceDiscardEpochs(epoch -> true);

    // perform GCs before exporting the store data
    performGC();
    final MemoryAnalysisService analysisService =
        (MemoryAnalysisService)
            createService(this.monitoredApp.getDatabase(), this.monitoredApp.getManager());
    final Path exportPath = analysisService.exportMostRecentVersion("testOverview");

    final AMemoryStatistic stats = loadMemoryStatFromFolder(exportPath);
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
    assertThat(pivot).isNotNull();
  }

  @AfterEach
  public void tearDown() throws AgentException {
    this.monitoringApp.getDatabase().close();
    this.monitoringApp.getManager().stop();
  }

  @Test
  void testVectorBlockRecordConsumptionIsZero() throws QueryException {
    final MDXQuery recordQuery =
        new MDXQuery(
            "SELECT [Components].[Component].[Component].[RECORDS] ON ROWS,"
                + " [Measures].[DirectMemory.SUM] ON COLUMNS"
                + " FROM [MemoryCube]"
                + " WHERE ("
                + "   [Owners].[Owner].[Owner].[Store A],"
                + "   [Fields].[Field].[Field].[vector1]"
                + " )");

    final CellSetDTO result = MdxQueryUtil.execute(this.monitoringApp.getManager(), recordQuery);

    assertThat((long) result.getCells().get(0).getValue()).isZero();
  }

  @Test
  void testVectorBlockConsumption() throws QueryException {
    final MDXQuery vectorBlockQueryField1 =
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

    final CellSetDTO result1 =
        MdxQueryUtil.execute(this.monitoringApp.getManager(), vectorBlockQueryField1);

    // Vector1 and Vector2 data are intertwined in the same blocks, so the direct memory is only
    // partially used
    // by the filtered fields, but at least one of the selected blocks belong to the filtered field
    final var blockCountUsed =
        ((ADDED_DATA_SIZE * 2 + ADDED_DATA_SIZE) / MICROAPP_VECTOR_BLOCK_SIZE) + 1;
    final var directMemoryUsedOnVector = blockCountUsed * MICROAPP_VECTOR_BLOCK_SIZE * Double.BYTES;

    assertThat((long) result1.getCells().get(1).getValue())
        .isEqualTo(directMemoryUsedOnVector)
        .isEqualTo((long) result1.getCells().get(0).getValue());

    final MDXQuery vectorBlockQueryField2 =
        new MDXQuery(
            "SELECT {"
                + "   [Components].[Component].[ALL].[AllMember],"
                + "   [Components].[Component].[Component].[VECTOR_BLOCK]"
                + " } ON ROWS,"
                + " [Measures].[DirectMemory.SUM] ON COLUMNS"
                + " FROM [MemoryCube]"
                + " WHERE ("
                + "   [Owners].[Owner].[Owner].[Store A],"
                + "   [Fields].[Field].[Field].[vector2]"
                + " )");

    final CellSetDTO result2 =
        MdxQueryUtil.execute(this.monitoringApp.getManager(), vectorBlockQueryField2);

    assertThat((long) result2.getCells().get(1).getValue())
        .isEqualTo(directMemoryUsedOnVector)
        .isEqualTo((long) result2.getCells().get(0).getValue());
  }

  @Test
  void testVectorBlockLength() throws QueryException {
    final MDXQuery lengthQuery =
        new MDXQuery(
            "SELECT  [Measures].[VectorBlock.Length] ON COLUMNS"
                + " FROM [MemoryCube]"
                + " WHERE ("
                + "   [Owners].[Owner].[Owner].[Store A],"
                + "   [Fields].[Field].[Field].[vector1]"
                + " )");

    final CellSetDTO result = MdxQueryUtil.execute(this.monitoringApp.getManager(), lengthQuery);

    assertThat(CellSetUtils.extractValueFromSingleCellDTO(result))
        .isEqualTo(MICROAPP_VECTOR_BLOCK_SIZE);
  }

  @Test
  void testVectorBlockRefCount() throws QueryException {
    final MDXQuery refCountQuery =
        new MDXQuery(
            "SELECT [Measures].[VectorBlock.RefCount] ON COLUMNS"
                + " FROM [MemoryCube]"
                + " WHERE ("
                + "   [Owners].[Owner].[Owner].[Store A],"
                + "   [Fields].[Field].[Field].[vector1]"
                + " )");

    final CellSetDTO refCountResult =
        MdxQueryUtil.execute(this.monitoringApp.getManager(), refCountQuery);

    assertThat(CellSetUtils.extractValueFromSingleCellDTO(refCountResult))
        .isEqualTo(FIELD_SHARING_COUNT * ADDED_DATA_SIZE);
  }
}
