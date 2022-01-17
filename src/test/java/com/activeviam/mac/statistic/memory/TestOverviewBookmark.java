package com.activeviam.mac.statistic.memory;

import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.pivot.builders.StartBuilding;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils.StatisticsSummary;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.pivot.monitoring.impl.MemoryAnalysisService;
import com.qfs.store.IDatastore;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.IMultiVersionActivePivot;
import com.quartetfs.biz.pivot.dto.CellSetDTO;
import com.quartetfs.biz.pivot.query.impl.MDXQuery;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.contributions.impl.ClasspathContributionProvider;
import com.quartetfs.fwk.impl.Pair;
import com.quartetfs.fwk.query.QueryException;
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
  private Pair<IDatastore, IActivePivotManager> monitoredApp;
  private Pair<IDatastore, IActivePivotManager> monitoringApp;
  private StatisticsSummary summary;

  @BeforeAll
  public static void setupRegistry() {
    Registry.setContributionProvider(new ClasspathContributionProvider());
  }

  @BeforeEach
  public void setup() throws AgentException {
    initializeApplication();

    final Path exportPath = generateMemoryStatistics();

    final IMemoryStatistic stats = loadMemoryStatFromFolder(exportPath);
    this.summary = MemoryStatisticsTestUtils.getStatisticsSummary(stats);

    initializeMonitoringApplication(stats);

    IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getRight()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);
    Assertions.assertThat(pivot).isNotNull();
  }

  private void initializeApplication() {
    this.monitoredApp = createMicroApplication();

    this.monitoredApp
        .getLeft()
        .edit(tm -> IntStream.range(0, ADDED_DATA_SIZE).forEach(i -> tm.add("A", i * i)));
  }

  private Path generateMemoryStatistics() {
    this.monitoredApp.getLeft().getEpochManager().forceDiscardEpochs(__ -> true);

    performGC();

    final MemoryAnalysisService analysisService =
        (MemoryAnalysisService)
            createService(this.monitoredApp.getLeft(), this.monitoredApp.getRight());
    return analysisService.exportMostRecentVersion("testOverview");
  }

  private void initializeMonitoringApplication(final IMemoryStatistic data) throws AgentException {
    ManagerDescriptionConfig config = new ManagerDescriptionConfig();
    final IDatastore monitoringDatastore =
        StartBuilding.datastore().setSchemaDescription(config.schemaDescription()).build();

    IActivePivotManager manager =
        StartBuilding.manager()
            .setDescription(config.managerDescription())
            .setDatastoreAndPermissions(monitoringDatastore)
            .buildAndStart();
    this.monitoringApp = new Pair<>(monitoringDatastore, manager);

    ATestMemoryStatistic.feedMonitoringApplication(monitoringDatastore, List.of(data), "storeA");
  }

  @AfterEach
  public void tearDown() throws AgentException {
    this.monitoringApp.getLeft().close();
    this.monitoringApp.getRight().stop();
  }

  @Test
  public void testOverviewGrandTotal() throws QueryException {
    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getRight()
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
            .getRight()
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
            .getRight()
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
            .getRight()
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
