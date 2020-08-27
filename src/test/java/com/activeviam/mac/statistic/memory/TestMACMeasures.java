package com.activeviam.mac.statistic.memory;

import static com.qfs.util.impl.ThrowingLambda.cast;
import static java.util.stream.Collectors.toMap;

import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.statistic.memory.descriptions.MicroApplication;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils.StatisticsSummary;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;
import com.qfs.store.IDatastore;
import com.qfs.store.NoTransactionException;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.qfs.util.impl.QfsArrays;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.IMultiVersionActivePivot;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.dto.CellSetDTO;
import com.quartetfs.biz.pivot.query.impl.MDXQuery;
import com.quartetfs.fwk.QuartetRuntimeException;
import com.quartetfs.fwk.query.QueryException;
import java.util.Collection;
import java.util.Map;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test class verifying the results obtained by the Measures provided in the MemoryAnalysisCube
 *
 * <p>Export tool usage:
 *
 * <p>Tools.extractSnappyFile(path to file);
 */
public class TestMACMeasures extends ATestOnMonitoringApp {

  public static final int ADDED_DATA_SIZE = 100;
  public static final int REMOVED_DATA_SIZE = 10;

  Map<String, Long> appStats;

  @Override
  protected IDatastoreSchemaDescription datastoreSchema() {
    return MicroApplication.datastoreDescription();
  }

  @Override
  protected IActivePivotManagerDescription managerDescription(
      IDatastoreSchemaDescription datastoreSchema) {
    return MicroApplication.managerDescription(datastoreSchema);
  }

  @Override
  protected void beforeExport(
      IDatastore datastore, IActivePivotManager manager) {
    datastore.edit(
        tm -> {
          IntStream.range(0, ADDED_DATA_SIZE)
              .forEach(
                  i -> {
                    tm.add("A", i * i);
                  });
        });

    datastore.edit(
        tm -> {
          IntStream.range(50, 50 + REMOVED_DATA_SIZE)
              .forEach(
                  i -> {
                    try {
                      tm.remove("A", i * i);
                    } catch (NoTransactionException
                        | DatastoreTransactionException
                        | IllegalArgumentException
                        | NullPointerException e) {
                      throw new QuartetRuntimeException(e);
                    }
                  });
        });

    // Force to discard all versions
    datastore.getEpochManager().forceDiscardEpochs(__ -> true);
  }

  @BeforeEach
  public void computeAppStats() {
    appStats = extractApplicationStats(statistics);
  }

  @Test
  public void testDirectMemorySum() throws QueryException {
    final IMultiVersionActivePivot pivot = monitoringManager.getActivePivots()
        .get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery query =
        new MDXQuery(
            "SELECT"
                + "  NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS"
                + "  FROM [MemoryCube]");
    final CellSetDTO res = pivot.execute(query);

    final Long value = CellSetUtils.extractValueFromSingleCellDTO(res);

    final MDXQuery query2 =
        new MDXQuery(
            "SELECT"
                + "  NON EMPTY [Measures].[contributors.COUNT] ON COLUMNS"
                + "  FROM [MemoryCube]");
    final CellSetDTO res2 = pivot.execute(query2);
    final Long nbC = CellSetUtils.extractValueFromSingleCellDTO(res2);

    final MDXQuery query3 =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + "  NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS"
                + "  FROM [MemoryCube]");
    final CellSetDTO res3 = pivot.execute(query3);

    // Check that the cell size is the expected one (the amount of chunks)
    Assertions.assertThat(res3.getCells().size()).isEqualTo(nbC.intValue());
    // Check that the summed value corresponds to the sum on each chunk of the Chunk
    // Level
    Assertions.assertThat(CellSetUtils.sumValuesFromCellSetDTO(res3)).isEqualTo(value);
    // Check that the summed value corresponds to the Exported sum
    final StatisticsSummary statsSumm = computeStatisticsSummary(statistics);
    Assertions.assertThat(statsSumm.offHeapMemory).isEqualTo(value);
  }

  @Test
  public void testOnHeapMemorySum() throws QueryException {

    final IMultiVersionActivePivot pivot =
        monitoringManager.getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);
    final MDXQuery query =
        new MDXQuery(
            "SELECT"
                + "  NON EMPTY [Measures].[HeapMemory.SUM] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res = pivot.execute(query);

    Long value = CellSetUtils.extractValueFromSingleCellDTO(res);

    final MDXQuery query2 =
        new MDXQuery(
            "SELECT"
                + "  NON EMPTY [Measures].[contributors.COUNT] ON COLUMNS"
                + "  FROM [MemoryCube]");
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
     * On-heap memory usage by chunks is not consistent with application on-heap
     * usage since on-heap data is not necessarily held by chunks
     */
    // Assertions.assertThat(statsSumm.onHeapMemory).isEqualTo(value);
  }

  @Test
  public void testChunkSize() throws QueryException {

    final IMultiVersionActivePivot pivot =
        monitoringManager.getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);

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
        monitoringManager.getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery query =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + "  NON EMPTY [Measures].[NonWrittenRows.COUNT] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res = pivot.execute(query);

    Assertions.assertThat(CellSetUtils.extractValuesFromCellSetDTO(res))
        .contains((double) ATestMemoryStatistic.MICROAPP_CHUNK_SIZE - ADDED_DATA_SIZE);
  }

  @Test
  public void testApplicationMeasures() {
    final IMultiVersionActivePivot pivot =
        monitoringManager.getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);

    SoftAssertions.assertSoftly(
        assertions -> {
          appStats.forEach(
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
                  }));
        });
  }

  /**
   * The measures of the application memory must be constant throughout the application. This checks
   * that whatever the level and depth, those values are the same.
   */
  @Test
  public void testApplicationMeasuresAtAnyPointOfTheCube() {
    final IMultiVersionActivePivot pivot =
        monitoringManager.getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);

    SoftAssertions.assertSoftly(
        assertions -> {
          appStats.forEach(
              cast(
                  (measure, value) -> {
                    final MDXQuery query =
                        new MDXQuery(
                            "SELECT"
                                + " NON EMPTY [Measures].["
                                + measure
                                + "] ON COLUMNS"
                                + " FROM [MemoryCube]"
                                + " WHERE ([Chunk Owners].[Owner].[ALL].[AllMember].FirstChild)");
                    final CellSetDTO result = pivot.execute(query);
                    final Long resultValue = CellSetUtils.extractValueFromSingleCellDTO(result);
                    assertions.assertThat(resultValue).as("Value of " + measure).isEqualTo(value);
                  }));
        });

    SoftAssertions.assertSoftly(
        assertions -> {
          appStats.forEach(
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
                  }));
        });
  }

  @Test
  public void testFreedCount() throws QueryException {

    final IMultiVersionActivePivot pivot =
        monitoringManager.getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery query =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + "  [Measures].[DeletedRows.COUNT] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res = pivot.execute(query);

    Assertions.assertThat(CellSetUtils.extractValuesFromCellSetDTO(res))
        .contains((double) REMOVED_DATA_SIZE);
  }

  @Test
  public void testNonWrittenRatio() throws QueryException {

    final IMultiVersionActivePivot pivot =
        monitoringManager.getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);
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
                + "  NON EMPTY [Measures].[NonWrittenRows.COUNT] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res2 = pivot.execute(query2);

    final MDXQuery query3 =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + "  NON EMPTY [Measures].[NonWrittenRows.Ratio] ON COLUMNS"
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
        monitoringManager.getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);
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
                + "  NON EMPTY [Measures].[DeletedRows.COUNT] ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO res2 = pivot.execute(query2);

    final MDXQuery query3 =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
                + "  NON EMPTY [Measures].[DeletedRows.Ratio] ON COLUMNS"
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
  public void testOwnerCountOnChunks() throws QueryException {
    performCountTest(
        "[Measures].[Owner.COUNT]", "[Owners].[Owner]", "[Chunks].[ChunkId].[ChunkId].Members");
  }

  @Test
  public void testOwnerCountOnOwnerAndComponents() throws QueryException {
    performCountTest(
        "[Measures].[Owner.COUNT]",
        "[Owners].[Owner]",
        " Crossjoin("
            + "    Hierarchize("
            + "      DrilldownLevel("
            + "        [Owners].[Owner].[ALL].[AllMember]"
            + "      )"
            + "    ),"
            + "    Hierarchize("
            + "      DrilldownLevel("
            + "        [Components].[Component].[ALL].[AllMember]"
            + "      )"
            + "    )"
            + "  )");
  }

  @Test
  public void testComponentCountOnChunks() throws QueryException {
    performCountTest(
        "[Measures].[Component.COUNT]",
        "[Components].[Component]",
        "[Chunks].[ChunkId].[ChunkId].Members");
  }

  @Test
  public void testComponentCountOnOwnerAndComponents() throws QueryException {
    performCountTest(
        "[Measures].[Component.COUNT]",
        "[Components].[Component]",
        " Crossjoin("
            + "    Hierarchize("
            + "      DrilldownLevel("
            + "        [Owners].[Owner].[ALL].[AllMember]"
            + "      )"
            + "    ),"
            + "    Hierarchize("
            + "      DrilldownLevel("
            + "        [Components].[Component].[ALL].[AllMember]"
            + "      )"
            + "    )"
            + "  )");
  }

  protected void performCountTest(
      String measureName, String countedHierarchy, String rowMdxExpression) throws QueryException {

    final IMultiVersionActivePivot pivot =
        monitoringManager.getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);

    final MDXQuery countQuery =
        new MDXQuery(
            "SELECT"
                + " NON EMPTY "
                + rowMdxExpression
                + " ON ROWS,"
                + "  NON EMPTY "
                + measureName
                + " ON COLUMNS"
                + "  FROM [MemoryCube]");
    CellSetDTO countResult = pivot.execute(countQuery);

    final MDXQuery verificationQuery =
        new MDXQuery(
            "WITH Member [Measures].[Expected.COUNT] AS"
                + " DistinctCount("
                + "  Generate("
                + "    NonEmpty("
                + "      Crossjoin("
                + "        [Chunks].[ChunkId].[ALL].[AllMember].Children,"
                + "        "
                + countedHierarchy
                + ".CurrentMember"
                + "      )"
                + "    ),"
                + "    Generate("
                + "      NonEmpty("
                + "        Crossjoin("
                + "          [Chunks].[ChunkId].CurrentMember,"
                + "          "
                + countedHierarchy
                + ".[ALL].[AllMember].Children"
                + "        )"
                + "      ),"
                + "      {"
                + "        "
                + countedHierarchy
                + ".CurrentMember"
                + "      }"
                + "    )"
                + "  )"
                + " ) "
                + " SELECT"
                + " NON EMPTY "
                + rowMdxExpression
                + " ON ROWS,"
                + " NON EMPTY [Measures].[Expected.COUNT] ON COLUMNS"
                + " FROM [MemoryCube]");

    CellSetDTO expectedResult = pivot.execute(verificationQuery);

    final Double[] counts = CellSetUtils.extractValuesFromCellSetDTO(countResult);
    final Double[] expectedCounts = CellSetUtils.extractValuesFromCellSetDTO(expectedResult);

    Assertions.assertThat(counts).containsExactly(expectedCounts);
  }

  private static Map<String, Long> extractApplicationStats(
      final Collection<IMemoryStatistic> export) {
    final IMemoryStatistic firstChild = export.iterator().next();
    return QfsArrays.<String, String>mutableMap(
        ManagerDescriptionConfig.USED_HEAP,
        MemoryStatisticConstants.STAT_NAME_GLOBAL_USED_HEAP_MEMORY,
        ManagerDescriptionConfig.COMMITTED_HEAP,
        MemoryStatisticConstants.ST$AT_NAME_GLOBAL_MAX_HEAP_MEMORY,
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
}
