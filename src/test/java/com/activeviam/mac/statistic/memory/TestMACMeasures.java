package com.activeviam.mac.statistic.memory;

import static com.activeviam.tech.test.internal.util.ThrowingLambda.cast;
import static java.util.stream.Collectors.toMap;

import com.activeviam.activepivot.core.impl.internal.utils.ApplicationInTests;
import com.activeviam.activepivot.core.intf.api.cube.IMultiVersionActivePivot;
import com.activeviam.activepivot.server.impl.api.query.MDXQuery;
import com.activeviam.activepivot.server.impl.api.query.MdxQueryUtil;
import com.activeviam.activepivot.server.impl.private_.observability.memory.MemoryAnalysisService;
import com.activeviam.activepivot.server.intf.api.dto.CellDTO;
import com.activeviam.activepivot.server.intf.api.dto.CellSetDTO;
import com.activeviam.activepivot.server.spring.api.config.IDatastoreSchemaDescriptionConfig;
import com.activeviam.database.datastore.api.transaction.DatastoreTransactionException;
import com.activeviam.database.datastore.internal.IInternalDatastore;
import com.activeviam.database.datastore.internal.NoTransactionException;
import com.activeviam.database.datastore.internal.monitoring.MemoryStatisticsTestUtils;
import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.cfg.impl.RegistryInitializationConfig;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig;
import com.activeviam.tech.core.api.agent.AgentException;
import com.activeviam.tech.core.api.exceptions.ActiveViamRuntimeException;
import com.activeviam.tech.core.api.properties.ActiveViamProperty;
import com.activeviam.tech.core.api.query.QueryException;
import com.activeviam.tech.core.internal.properties.ActiveViamPropertyExtension;
import com.activeviam.tech.core.internal.util.ArrayUtil;
import com.activeviam.tech.observability.api.memory.IMemoryStatistic;
import com.activeviam.tech.observability.internal.memory.AMemoryStatistic;
import com.activeviam.tech.observability.internal.memory.MemoryStatisticConstants;
import com.activeviam.tech.test.internal.junit.resources.ResourcesExtension;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Test class verifying the results obtained by the Measures provided in the MemoryAnalysisCube
 *
 * <p>Export tool usage:
 *
 * <p>Tools.extractSnappyFile(path to file);
 */
@ExtendWith({ResourcesExtension.class})
public class TestMACMeasures extends ATestMemoryStatistic {

  public static final int ADDED_DATA_SIZE = 100;
  public static final int REMOVED_DATA_SIZE = 10;

  @RegisterExtension
  @SuppressWarnings("unused")
  public static ActiveViamPropertyExtension propertyRule =
      new ActiveViamPropertyExtension.ActiveViamPropertyExtensionBuilder()
          .withProperty(ActiveViamProperty.ACTIVEVIAM_TEST_PROPERTY, true)
          .build();

  private ApplicationInTests<IInternalDatastore> monitoredApp;
  private ApplicationInTests<IInternalDatastore> monitoringApp;
  AMemoryStatistic stats;
  Map<String, Long> appStats;
  MemoryStatisticsTestUtils.StatisticsSummary statsSumm;

  @BeforeAll
  public static void init() {
    RegistryInitializationConfig.setupRegistry();
  }

  private static Map<String, Long> extractApplicationStats(final IMemoryStatistic export) {
    final IMemoryStatistic firstChild = export.getChildren().iterator().next();
    return ArrayUtil.<String, String>mutableMap(
            ManagerDescriptionConfig.USED_HEAP,
            MemoryStatisticConstants.STAT_NAME_GLOBAL_USED_HEAP_MEMORY,
            ManagerDescriptionConfig.COMMITTED_HEAP,
            MemoryStatisticConstants.STAT_NAME_GLOBAL_MAX_HEAP_MEMORY,
            ManagerDescriptionConfig.USED_DIRECT,
            MemoryStatisticConstants.STAT_NAME_GLOBAL_USED_DIRECT_MEMORY,
            ManagerDescriptionConfig.MAX_DIRECT,
            MemoryStatisticConstants.STAT_NAME_GLOBAL_MAX_DIRECT_MEMORY)
        .entrySet()
        .stream()
        .collect(
            toMap(
                Map.Entry::getKey,
                entry -> firstChild.getAttributes().get(entry.getValue()).asLong()));
  }

  @BeforeEach
  public void setup() throws AgentException {
    this.monitoredApp = createMicroApplication();
    // Add 100 records
    this.monitoredApp
        .getDatabase()
        .edit(tm -> IntStream.range(0, ADDED_DATA_SIZE).forEach(i -> tm.add("A", i * i)));
    // Delete 10 records
    this.monitoredApp
        .getDatabase()
        .edit(
            tm ->
                IntStream.range(50, 50 + REMOVED_DATA_SIZE)
                    .forEach(
                        i -> {
                          try {
                            tm.remove("A", i * i);
                          } catch (NoTransactionException
                              | DatastoreTransactionException
                              | IllegalArgumentException
                              | NullPointerException e) {
                            throw new ActiveViamRuntimeException(e);
                          }
                        }));

    // Force to discard all versions
    this.monitoredApp.getDatabase().getEpochManager().forceDiscardEpochs(__ -> true);
    // perform GCs before exporting the store data
    performGC();
    final MemoryAnalysisService analysisService =
        (MemoryAnalysisService)
            createService(this.monitoredApp.getDatabase(), this.monitoredApp.getManager());
    final Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");

    this.stats = loadMemoryStatFromFolder(exportPath);
    this.appStats = extractApplicationStats(this.stats);
    this.statsSumm = MemoryStatisticsTestUtils.getStatisticsSummary(this.stats);

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
        this.monitoringApp.getDatabase(), List.of(this.stats), "storeA");

    IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);
    Assertions.assertThat(pivot).isNotNull();
  }

  @Test
  public void testDirectMemorySum() throws QueryException {
    final MDXQuery query =
        new MDXQuery(
            "SELECT"
                + "  NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res = MdxQueryUtil.execute(this.monitoringApp.getManager(), query);

    Long value = CellSetUtils.extractValueFromSingleCellDTO(res);

    final MDXQuery query2 =
        new MDXQuery(
            "SELECT" + "  NON EMPTY [Measures].[Chunks.COUNT] ON COLUMNS" + "  FROM [MemoryCube]");
    CellSetDTO res2 = MdxQueryUtil.execute(this.monitoringApp.getManager(), query2);
    Long nbC = CellSetUtils.extractValueFromSingleCellDTO(res2);

    final MDXQuery query3 =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + "  NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res3 = MdxQueryUtil.execute(this.monitoringApp.getManager(), query3);

    // Check that the cell size is the expected one (the amount of chunks)
    Assertions.assertThat(res3.getCells().size()).isEqualTo(nbC.intValue());
    // Check that the summed value corresponds to the sum on each chunk of the Chunk
    // Level
    Assertions.assertThat(CellSetUtils.sumValuesFromCellSetDTO(res3)).isEqualTo(value);
    // Check that the summed value corresponds to the Exported sum
    Assertions.assertThat(this.statsSumm.offHeapMemory).isEqualTo(value);
  }

  @Test
  public void testOnHeapMemorySum() throws QueryException {
    final MDXQuery query =
        new MDXQuery(
            "SELECT"
                + "  NON EMPTY [Measures].[HeapMemory.SUM] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res = MdxQueryUtil.execute(this.monitoringApp.getManager(), query);

    Long value = CellSetUtils.extractValueFromSingleCellDTO(res);

    final MDXQuery query2 =
        new MDXQuery(
            "SELECT" + "  NON EMPTY [Measures].[Chunks.COUNT] ON COLUMNS" + "  FROM [MemoryCube]");
    CellSetDTO res2 = MdxQueryUtil.execute(this.monitoringApp.getManager(), query2);
    Long nbC = CellSetUtils.extractValueFromSingleCellDTO(res2);

    final MDXQuery query3 =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + "  NON EMPTY [Measures].[HeapMemory.SUM] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res3 = MdxQueryUtil.execute(this.monitoringApp.getManager(), query3);

    // Check that the cell size is the expected one (the amount of chunks)
    Assertions.assertThat(res3.getCells().size()).isEqualTo(nbC.intValue());
    // Check that the summed value corresponds to the sum on each chunk of the Chunk
    // Level
    Assertions.assertThat(CellSetUtils.sumValuesFromCellSetDTO(res3)).isEqualTo(value);

    /*
     * On-heap memory usage by chunks is not consistent with application on-heap usage since on-heap data is
     * not necessarily held by chunks
     */
    // Assertions.assertThat(statsSumm.onHeapMemory).isEqualTo(value);
  }

  @Test
  public void testChunkSize() throws QueryException {

    final MDXQuery query =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + "  NON EMPTY [Measures].[ChunkSize.SUM] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res = MdxQueryUtil.execute(this.monitoringApp.getManager(), query);

    Assertions.assertThat(CellSetUtils.extractValuesFromCellSetDTO(res))
        .contains((double) ATestMemoryStatistic.MICROAPP_CHUNK_SIZE);
  }

  @Test
  public void testNonWrittenCount() throws QueryException {

    final MDXQuery query =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + "  NON EMPTY [Measures].[Unused rows] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res = MdxQueryUtil.execute(this.monitoringApp.getManager(), query);

    Assertions.assertThat(CellSetUtils.extractValuesFromCellSetDTO(res))
        .contains((double) ATestMemoryStatistic.MICROAPP_CHUNK_SIZE - ADDED_DATA_SIZE);
  }

  @Test
  public void testApplicationMeasures() {

    SoftAssertions.assertSoftly(
        assertions ->
            this.appStats.forEach(
                cast(
                    (measure, value) -> {
                      final MDXQuery query =
                          new MDXQuery(
                              "SELECT"
                                  + "  NON EMPTY [Measures].["
                                  + measure
                                  + "] ON COLUMNS"
                                  + "  FROM [MemoryCube]");
                      final CellSetDTO result =
                          MdxQueryUtil.execute(this.monitoringApp.getManager(), query);
                      final Long resultValue = CellSetUtils.extractValueFromSingleCellDTO(result);
                      assertions.assertThat(resultValue).as("Value of " + measure).isEqualTo(value);
                    })));
  }

  /**
   * The measures of the application memory must be constant throughout the application. This checks
   * that whatever the level and depth, those values are the same.
   */
  @Test
  public void testApplicationMeasuresAtAnyPointOfTheCube() {

    SoftAssertions.assertSoftly(
        assertions ->
            this.appStats.forEach(
                cast(
                    (measure, value) -> {
                      final MDXQuery query =
                          new MDXQuery(
                              "SELECT"
                                  + " NON EMPTY [Measures].["
                                  + measure
                                  + "] ON COLUMNS"
                                  + " FROM [MemoryCube]"
                                  + " WHERE ([Owners].[Owner].[ALL].[AllMember].FirstChild)");
                      final CellSetDTO result =
                          MdxQueryUtil.execute(this.monitoringApp.getManager(), query);
                      final Long resultValue = CellSetUtils.extractValueFromSingleCellDTO(result);
                      assertions.assertThat(resultValue).as("Value of " + measure).isEqualTo(value);
                    })));

    SoftAssertions.assertSoftly(
        assertions ->
            this.appStats.forEach(
                cast(
                    (measure, value) -> {
                      final MDXQuery query =
                          new MDXQuery(
                              "SELECT"
                                  + " NON EMPTY [Measures].["
                                  + measure
                                  + "] ON COLUMNS"
                                  + " FROM [MemoryCube]"
                                  + " WHERE (["
                                  + ManagerDescriptionConfig.CHUNK_DIMENSION
                                  + "].[ChunkId].[ALL].[AllMember].FirstChild)");
                      final CellSetDTO result =
                          MdxQueryUtil.execute(this.monitoringApp.getManager(), query);
                      final Long resultValue = CellSetUtils.extractValueFromSingleCellDTO(result);
                      assertions.assertThat(resultValue).as("Value of " + measure).isEqualTo(value);
                    })));
  }

  @Test
  public void testFreedCount() throws QueryException {

    final MDXQuery query =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + "  [Measures].[Deleted rows] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res = MdxQueryUtil.execute(this.monitoringApp.getManager(), query);

    Assertions.assertThat(CellSetUtils.extractValuesFromCellSetDTO(res))
        .contains((double) REMOVED_DATA_SIZE);
  }

  @Test
  public void testNonWrittenRatio() throws QueryException {

    final MDXQuery query =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + "  NON EMPTY [Measures].[ChunkSize.SUM] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res = MdxQueryUtil.execute(this.monitoringApp.getManager(), query);

    final MDXQuery query2 =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + "  NON EMPTY [Measures].[Unused rows] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res2 = MdxQueryUtil.execute(this.monitoringApp.getManager(), query2);

    final MDXQuery query3 =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + "  NON EMPTY [Measures].[Unused rows ratio] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res3 = MdxQueryUtil.execute(this.monitoringApp.getManager(), query3);

    final Double[] chunkSizes = CellSetUtils.extractValuesFromCellSetDTO(res);
    final Double[] nonWrittenRows = CellSetUtils.extractValuesFromCellSetDTO(res2);
    final Double[] nonWrittenRatio = CellSetUtils.extractValuesFromCellSetDTO(res3);

    for (int i = 0; i < chunkSizes.length; i++) {
      Assertions.assertThat(nonWrittenRatio[i]).isEqualTo(nonWrittenRows[i] / chunkSizes[i]);
    }
  }

  @Test
  public void testDeletedRatio() throws QueryException {

    final MDXQuery query =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + "  NON EMPTY [Measures].[ChunkSize.SUM] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res = MdxQueryUtil.execute(this.monitoringApp.getManager(), query);

    final MDXQuery query2 =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + "  NON EMPTY [Measures].[Deleted rows] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res2 = MdxQueryUtil.execute(this.monitoringApp.getManager(), query2);

    final MDXQuery query3 =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + "  NON EMPTY [Measures].[Deleted rows ratio] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res3 = MdxQueryUtil.execute(this.monitoringApp.getManager(), query3);

    final Double[] chunkSizes = CellSetUtils.extractValuesFromCellSetDTO(res);
    final Double[] DeletedRows = CellSetUtils.extractValuesFromCellSetDTO(res2);
    final Double[] DeletedRatio = CellSetUtils.extractValuesFromCellSetDTO(res3);

    for (int i = 0; i < chunkSizes.length; i++) {
      Assertions.assertThat(DeletedRatio[i]).isEqualTo(DeletedRows[i] / chunkSizes[i]);
    }
  }

  @Test
  public void testDictionarySize() throws QueryException {
    final MDXQuery query =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + " NON EMPTY [Measures].[DictionarySize.SUM] ON COLUMNS"
                + " FROM [MemoryCube]"
                + " WHERE ("
                + "  [Owners].[Owner].[Owner].[Store A],"
                + "  [Fields].[Field].[Field].[id]"
                + " )");
    CellSetDTO res = MdxQueryUtil.execute(this.monitoringApp.getManager(), query);

    final long expectedDictionarySize =
        this.monitoredApp
            .getDatabase()
            .getQueryMetadata()
            .getDictionaries()
            .getDictionary("A", "id")
            .size();

    Assertions.assertThat(res.getCells())
        .isNotEmpty()
        .extracting(CellDTO::getValue)
        .containsOnly(expectedDictionarySize);
  }
}
