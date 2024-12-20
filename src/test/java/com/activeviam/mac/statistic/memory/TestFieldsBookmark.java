package com.activeviam.mac.statistic.memory;

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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestFieldsBookmark extends ATestMemoryStatistic {

  public static final int ADDED_DATA_SIZE = 20;
  private ApplicationInTests<IDatastore> monitoredApp;
  private ApplicationInTests<IInternalDatastore> monitoringApp;
  MemoryStatisticsTestUtils.StatisticsSummary summary;

  @BeforeAll
  public static void setupRegistry() {
    RegistryInitializationConfig.setupRegistry();
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

    final AMemoryStatistic stats = loadMemoryStatFromFolder(exportPath);
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
        monitoringApp.getDatabase(), List.of(stats), "storeA");

    IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);
    Assertions.assertThat(pivot).isNotNull();
  }

  @Test
  public void testStoreTotal() throws QueryException {
    final MDXQuery storeTotal =
        new MDXQuery(
            "SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS "
                + "FROM [MemoryCube]"
                + "WHERE [Owners].[Owner].[ALL].[AllMember].[Store A]");

    final MDXQuery perFieldQuery =
        new MDXQuery(
            "SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS, "
                + "NON EMPTY [Fields].[Field].[ALL].[AllMember].Children ON ROWS "
                + "FROM [MemoryCube]"
                + "WHERE [Owners].[Owner].[ALL].[AllMember].[Store A]");

    final MDXQuery excessMemoryQuery =
        new MDXQuery(
            "WITH MEMBER [Measures].[Field.COUNT] AS "
                + ownershipCountMdxExpression("[Fields].[Field]")
                + " MEMBER [Measures].[ExcessDirectMemory] AS"
                + " Sum("
                + "   [Chunks].[ChunkId].[ALL].[AllMember].Children,"
                + "   IIF([Measures].[Field.COUNT] > 1,"
                + "     ([Measures].[Field.COUNT] - 1) * [Measures].[DirectMemory.SUM],"
                + "     0)"
                + " )"
                + " SELECT [Measures].[ExcessDirectMemory] ON COLUMNS"
                + " FROM [MemoryCube]"
                + " WHERE [Owners].[Owner].[ALL].[AllMember].[Store A]");

    final CellSetDTO totalResult =
        MdxQueryUtil.execute(this.monitoringApp.getManager(), storeTotal);
    final CellSetDTO fieldResult =
        MdxQueryUtil.execute(this.monitoringApp.getManager(), perFieldQuery);
    final CellSetDTO excessResult =
        MdxQueryUtil.execute(this.monitoringApp.getManager(), excessMemoryQuery);

    Assertions.assertThat(CellSetUtils.extractValueFromSingleCellDTO(totalResult))
        .isEqualTo(
            CellSetUtils.sumValuesFromCellSetDTO(fieldResult)
                - CellSetUtils.extractDoubleValueFromSingleCellDTO(excessResult).longValue());
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
