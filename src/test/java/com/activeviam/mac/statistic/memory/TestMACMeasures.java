package com.activeviam.mac.statistic.memory;

import static com.qfs.util.impl.ThrowingLambda.cast;
import static java.util.stream.Collectors.toMap;

import com.activeviam.fwk.ActiveViamRuntimeException;
import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig;
import com.activeviam.pivot.utils.ApplicationInTests;
import com.activeviam.properties.impl.ActiveViamProperty;
import com.activeviam.properties.impl.ActiveViamPropertyExtension;
import com.activeviam.properties.impl.ActiveViamPropertyExtension.ActiveViamPropertyExtensionBuilder;
import com.qfs.junit.LocalResourcesExtension;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils.StatisticsSummary;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;
import com.qfs.pivot.monitoring.impl.MemoryAnalysisService;
import com.qfs.server.cfg.IDatastoreSchemaDescriptionConfig;
import com.qfs.store.IDatastore;
import com.qfs.store.NoTransactionException;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.qfs.util.impl.QfsArrays;
import com.quartetfs.biz.pivot.IMultiVersionActivePivot;
import com.quartetfs.biz.pivot.dto.CellDTO;
import com.quartetfs.biz.pivot.dto.CellSetDTO;
import com.quartetfs.biz.pivot.query.impl.MDXQuery;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.contributions.impl.ClasspathContributionProvider;
import com.quartetfs.fwk.query.QueryException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Test class verifying the results obtained by the Measures provided in the MemoryAnalysisCube
 *
 * <p>Export tool usage:
 *
 * <p>Tools.extractSnappyFile(path to file);
 */
public class TestMACMeasures extends ATestMemoryStatistic {

  public static final int ADDED_DATA_SIZE = 100;
  public static final int REMOVED_DATA_SIZE = 10;

  @RegisterExtension
  @SuppressWarnings("unused")
  public static ActiveViamPropertyExtension propertyRule =
      new ActiveViamPropertyExtensionBuilder()
          .withProperty(ActiveViamProperty.ACTIVEVIAM_TEST_PROPERTY, true)
          .build();

  @RegisterExtension
  public final LocalResourcesExtension methodResources = new LocalResourcesExtension();

  private ApplicationInTests<IDatastore> monitoredApp;
  private ApplicationInTests<IDatastore> monitoringApp;
  IMemoryStatistic stats;
  Map<String, Long> appStats;
  StatisticsSummary statsSumm;

  @BeforeAll
  public static void init() {
    Registry.setContributionProvider(new ClasspathContributionProvider());
  }

  private static Map<String, Long> extractApplicationStats(final IMemoryStatistic export) {
    final IMemoryStatistic firstChild = export.getChildren().iterator().next();
    return QfsArrays.<String, String>mutableMap(
            ManagerDescriptionConfig.USED_HEAP,
            MemoryStatisticConstants.STAT_NAME_GLOBAL_USED_HEAP_MEMORY,
            ManagerDescriptionConfig.COMMITTED_HEAP,
            MemoryStatisticConstants.STAT_NAME_GLOBAL_MAX_HEAP_MEMORY,
            ManagerDescriptionConfig.USED_DIRECT,
            MemoryStatisticConstants.STAT_NAME_GLOBAL_USED_DIRECT_MEMORY,
            ManagerDescriptionConfig.MAX_DIRECT,
            MemoryStatisticConstants.STAT_NAME_GLOBAL_MAX_DIRECT_MEMORY)
        .entrySet().stream()
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
    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);
    final MDXQuery query =
        new MDXQuery(
            "SELECT"
                + "  NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res = pivot.execute(query);

    Long value = CellSetUtils.extractValueFromSingleCellDTO(res);

    final MDXQuery query2 =
        new MDXQuery(
            "SELECT" + "  NON EMPTY [Measures].[Chunks.COUNT] ON COLUMNS" + "  FROM [MemoryCube]");
    CellSetDTO res2 = pivot.execute(query2);
    Long nbC = CellSetUtils.extractValueFromSingleCellDTO(res2);

    final MDXQuery query3 =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + "  NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res3 = pivot.execute(query3);

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
    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);
    final MDXQuery query =
        new MDXQuery(
            "SELECT"
                + "  NON EMPTY [Measures].[HeapMemory.SUM] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res = pivot.execute(query);

    Long value = CellSetUtils.extractValueFromSingleCellDTO(res);

    final MDXQuery query2 =
        new MDXQuery(
            "SELECT" + "  NON EMPTY [Measures].[Chunks.COUNT] ON COLUMNS" + "  FROM [MemoryCube]");
    CellSetDTO res2 = pivot.execute(query2);
    Long nbC = CellSetUtils.extractValueFromSingleCellDTO(res2);

    final MDXQuery query3 =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + "  NON EMPTY [Measures].[HeapMemory.SUM] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res3 = pivot.execute(query3);

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

    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery query =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + "  NON EMPTY [Measures].[ChunkSize.SUM] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res = pivot.execute(query);

    Assertions.assertThat(CellSetUtils.extractValuesFromCellSetDTO(res))
        .contains((double) ATestMemoryStatistic.MICROAPP_CHUNK_SIZE);
  }

  @Test
  public void testNonWrittenCount() throws QueryException {

    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery query =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + "  NON EMPTY [Measures].[Unused rows] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res = pivot.execute(query);

    Assertions.assertThat(CellSetUtils.extractValuesFromCellSetDTO(res))
        .contains((double) ATestMemoryStatistic.MICROAPP_CHUNK_SIZE - ADDED_DATA_SIZE);
  }

  @Test
  public void testApplicationMeasures() {
    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);

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
                      final CellSetDTO result = pivot.execute(query);
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
    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);

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
                      final CellSetDTO result = pivot.execute(query);
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
                      final CellSetDTO result = pivot.execute(query);
                      final Long resultValue = CellSetUtils.extractValueFromSingleCellDTO(result);
                      assertions.assertThat(resultValue).as("Value of " + measure).isEqualTo(value);
                    })));
  }

  @Test
  public void testFreedCount() throws QueryException {

    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery query =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + "  [Measures].[Deleted rows] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res = pivot.execute(query);

    Assertions.assertThat(CellSetUtils.extractValuesFromCellSetDTO(res))
        .contains((double) REMOVED_DATA_SIZE);
  }

  @Test
  public void testNonWrittenRatio() throws QueryException {

    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);
    final MDXQuery query =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + "  NON EMPTY [Measures].[ChunkSize.SUM] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res = pivot.execute(query);

    final MDXQuery query2 =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + "  NON EMPTY [Measures].[Unused rows] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res2 = pivot.execute(query2);

    final MDXQuery query3 =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + "  NON EMPTY [Measures].[Unused rows ratio] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res3 = pivot.execute(query3);

    final Double[] chunkSizes = CellSetUtils.extractValuesFromCellSetDTO(res);
    final Double[] nonWrittenRows = CellSetUtils.extractValuesFromCellSetDTO(res2);
    final Double[] nonWrittenRatio = CellSetUtils.extractValuesFromCellSetDTO(res3);

    for (int i = 0; i < chunkSizes.length; i++) {
      Assertions.assertThat(nonWrittenRatio[i]).isEqualTo(nonWrittenRows[i] / chunkSizes[i]);
    }
  }

  @Test
  public void testDeletedRatio() throws QueryException {

    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);
    final MDXQuery query =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + "  NON EMPTY [Measures].[ChunkSize.SUM] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res = pivot.execute(query);

    final MDXQuery query2 =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + "  NON EMPTY [Measures].[Deleted rows] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res2 = pivot.execute(query2);

    final MDXQuery query3 =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + "  NON EMPTY [Measures].[Deleted rows ratio] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res3 = pivot.execute(query3);

    final Double[] chunkSizes = CellSetUtils.extractValuesFromCellSetDTO(res);
    final Double[] DeletedRows = CellSetUtils.extractValuesFromCellSetDTO(res2);
    final Double[] DeletedRatio = CellSetUtils.extractValuesFromCellSetDTO(res3);

    for (int i = 0; i < chunkSizes.length; i++) {
      Assertions.assertThat(DeletedRatio[i]).isEqualTo(DeletedRows[i] / chunkSizes[i]);
    }
  }

  @Test
  public void testDictionarySize() throws QueryException {
    final IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);
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
    CellSetDTO res = pivot.execute(query);

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
