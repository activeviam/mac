package com.activeviam.mac.statistic.memory;

import com.activeviam.activepivot.core.impl.internal.utils.ApplicationInTests;
import com.activeviam.activepivot.core.intf.api.cube.IMultiVersionActivePivot;
import com.activeviam.activepivot.server.impl.api.query.MDXQuery;
import com.activeviam.activepivot.server.impl.private_.observability.memory.MemoryAnalysisService;
import com.activeviam.activepivot.server.intf.api.dto.CellSetDTO;
import com.activeviam.activepivot.server.spring.api.config.IDatastoreSchemaDescriptionConfig;
import com.activeviam.database.datastore.internal.IInternalDatastore;
import com.activeviam.database.datastore.internal.monitoring.MemoryStatisticsTestUtils;
import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig;
import com.activeviam.tech.core.api.agent.AgentException;
import com.activeviam.tech.core.api.query.QueryException;
import com.activeviam.tech.core.api.registry.Registry;
import com.activeviam.tech.observability.internal.memory.AMemoryStatistic;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestOverviewBookmark extends ATestMemoryStatistic {

  public static final int ADDED_DATA_SIZE = 20;
  private ApplicationInTests<IInternalDatastore> monitoredApp;
  private ApplicationInTests<IInternalDatastore> monitoringApp;
  private MemoryStatisticsTestUtils.StatisticsSummary summary;

  @BeforeAll
  public static void setupRegistry() {
    Registry.initialize(Registry.RegistryContributions.builder().build());
  }

  @BeforeEach
  public void setup() throws AgentException {
    initializeApplication();

    final Path exportPath = generateMemoryStatistics();

    final AMemoryStatistic stats = loadMemoryStatFromFolder(exportPath);
    this.summary = MemoryStatisticsTestUtils.getStatisticsSummary(stats);

    initializeMonitoringApplication(stats);

    IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);
    Assertions.assertThat(pivot).isNotNull();
  }

  private void initializeApplication() {
    this.monitoredApp = createMicroApplication();

    this.monitoredApp
        .getDatabase()
        .edit(tm -> IntStream.range(0, ADDED_DATA_SIZE).forEach(i -> tm.add("A", i * i)));
  }

  private Path generateMemoryStatistics() {
    this.monitoredApp.getDatabase().getEpochManager().forceDiscardEpochs(__ -> true);

    performGC();

    final MemoryAnalysisService analysisService =
        (MemoryAnalysisService)
            createService(this.monitoredApp.getDatabase(), this.monitoredApp.getManager());
    return analysisService.exportMostRecentVersion("testOverview");
  }

  private void initializeMonitoringApplication(final AMemoryStatistic data) throws AgentException {
    ManagerDescriptionConfig config = new ManagerDescriptionConfig();
    final IDatastoreSchemaDescriptionConfig schemaConfig =
        new MemoryAnalysisDatastoreDescriptionConfig();
    this.monitoringApp =
        ApplicationInTests.builder()
            .withDatastore(schemaConfig.datastoreSchemaDescription())
            .withManager(config.managerDescription())
            .build();

    resources.register(this.monitoringApp).start();

    ATestMemoryStatistic.feedMonitoringApplication(
        this.monitoringApp.getDatabase(), List.of(data), "storeA");
  }

  @AfterEach
  public void tearDown() throws AgentException {
    this.monitoringApp.getDatabase().close();
    this.monitoringApp.getManager().stop();
  }

  @Test
  public void testOverviewGrandTotal() throws QueryException {
    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery totalQuery =
        new MDXQuery("SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS FROM [MemoryCube]");

    final CellSetDTO totalResult = pivot.execute(totalQuery);

    Assertions.assertThat(CellSetUtils.extractValueFromSingleCellDTO(totalResult))
        .isEqualTo(this.summary.offHeapMemory);
  }

  @Test
  public void testOwnerTotal() throws QueryException {
    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery totalQuery =
        new MDXQuery("SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS FROM [MemoryCube]");

    final MDXQuery perOwnerQuery =
        new MDXQuery(
            "SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS, "
                + "NON EMPTY [Owners].[Owner].[ALL].[AllMember].Children ON ROWS "
                + "FROM [MemoryCube]");

    final MDXQuery excessMemoryQuery =
        new MDXQuery(
            "WITH MEMBER [Measures].[Owner.COUNT] AS "
                + ownershipCountMdxExpression("[Owners].[Owner]")
                + " MEMBER [Measures].[ExcessDirectMemory] AS"
                + " Sum("
                + "   [Chunks].[ChunkId].[ALL].[AllMember].Children,"
                + "   ([Measures].[Owner.COUNT] - 1) * [Measures].[DirectMemory.SUM]"
                + " )"
                + " SELECT [Measures].[ExcessDirectMemory] ON COLUMNS"
                + " FROM [MemoryCube]");

    final CellSetDTO totalResult = pivot.execute(totalQuery);
    final CellSetDTO perOwnerResult = pivot.execute(perOwnerQuery);
    final CellSetDTO excessMemoryResult = pivot.execute(excessMemoryQuery);

    Assertions.assertThat(
            CellSetUtils.sumValuesFromCellSetDTO(perOwnerResult)
                - CellSetUtils.extractDoubleValueFromSingleCellDTO(excessMemoryResult).longValue())
        .isEqualTo(CellSetUtils.extractValueFromSingleCellDTO(totalResult));
  }

  @Test
  public void testStoreTotal() throws QueryException {
    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery storeTotalQuery =
        new MDXQuery(
            "SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS "
                + "FROM [MemoryCube] "
                + "WHERE [Owners].[Owner].[ALL].[AllMember].[Store A]");

    final MDXQuery perComponentsStoreQuery =
        new MDXQuery(
            "SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS, "
                + "[Components].[Component].[ALL].[AllMember].Children ON ROWS "
                + "FROM [MemoryCube] "
                + "WHERE [Owners].[Owner].[ALL].[AllMember].[Store A]");

    final MDXQuery excessMemoryQuery =
        new MDXQuery(
            "WITH MEMBER [Measures].[Component.COUNT] AS "
                + ownershipCountMdxExpression("[Components].[Component]")
                + " MEMBER [Measures].[ExcessDirectMemory] AS"
                + " Sum("
                + "   [Chunks].[ChunkId].[ALL].[AllMember].Children,"
                + "   ([Measures].[Component.COUNT] - 1) * [Measures].[DirectMemory.SUM]"
                + " )"
                + " SELECT [Measures].[ExcessDirectMemory] ON COLUMNS"
                + " FROM [MemoryCube]"
                + " WHERE [Owners].[Owner].[ALL].[AllMember].[Store A]");

    final CellSetDTO storeTotalResult = pivot.execute(storeTotalQuery);
    final CellSetDTO perComponentStoreResult = pivot.execute(perComponentsStoreQuery);
    final CellSetDTO excessMemoryResult = pivot.execute(excessMemoryQuery);

    Assertions.assertThat(
            CellSetUtils.sumValuesFromCellSetDTO(perComponentStoreResult)
                - CellSetUtils.extractDoubleValueFromSingleCellDTO(excessMemoryResult).longValue())
        .isEqualTo(CellSetUtils.extractValueFromSingleCellDTO(storeTotalResult));
  }

  @Test
  public void testCubeTotal() throws QueryException {
    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery cubeTotalQuery =
        new MDXQuery(
            "SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS "
                + "FROM [MemoryCube] "
                + "WHERE [Owners].[Owner].[ALL].[AllMember].[Cube Cube]");

    final MDXQuery perComponentCubeQuery =
        new MDXQuery(
            "SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS,"
                + "[Components].[Component].[ALL].[AllMember].Children ON ROWS "
                + "FROM [MemoryCube] "
                + "WHERE [Owners].[Owner].[ALL].[AllMember].[Cube Cube]");

    final MDXQuery excessMemoryQuery =
        new MDXQuery(
            "WITH MEMBER [Measures].[Component.COUNT] AS "
                + ownershipCountMdxExpression("[Components].[Component]")
                + " MEMBER [Measures].[ExcessDirectMemory] AS"
                + " Sum("
                + "   [Chunks].[ChunkId].[ALL].[AllMember].Children,"
                + "   ([Measures].[Component.COUNT] - 1) * [Measures].[DirectMemory.SUM]"
                + " )"
                + " SELECT [Measures].[ExcessDirectMemory] ON COLUMNS"
                + " FROM [MemoryCube]"
                + " WHERE [Owners].[Owner].[ALL].[AllMember].[Cube Cube]");

    final CellSetDTO cubeTotalResult = pivot.execute(cubeTotalQuery);
    final CellSetDTO perComponentCubeResult = pivot.execute(perComponentCubeQuery);
    final CellSetDTO excessMemoryResult = pivot.execute(excessMemoryQuery);

    Assertions.assertThat(
            CellSetUtils.sumValuesFromCellSetDTO(perComponentCubeResult)
                - CellSetUtils.extractDoubleValueFromSingleCellDTO(excessMemoryResult).longValue())
        .isEqualTo(CellSetUtils.extractValueFromSingleCellDTO(cubeTotalResult));
  }

  protected String ownershipCountMdxExpression(final String hierarchyUniqueName) {
    return "DistinctCount("
        + "  Generate("
        + "    NonEmpty("
        + "      [Chunks].[ChunkId].[ALL].[AllMember].Children,"
        + "      {[Measures].[contributors.COUNT]}"
        + "    ),"
        + "    NonEmpty("
        + "      "
        + hierarchyUniqueName
        + ".[ALL].[AllMember].Children,"
        + "      {[Measures].[contributors.COUNT]}"
        + "    )"
        + "  )"
        + ")";
  }
}
