package com.activeviam.mac.statistic.memory;

import static com.qfs.util.impl.ThrowingLambda.cast;

import com.activeviam.copper.testing.CubeTester;
import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.statistic.memory.descriptions.MicroApplicationDescription;
import com.activeviam.mac.statistic.memory.junit.RegistrySetupExtension;
import com.activeviam.properties.impl.ActiveViamProperty;
import com.activeviam.properties.impl.ActiveViamPropertyExtension;
import com.activeviam.properties.impl.ActiveViamPropertyExtension.ActiveViamPropertyExtensionBuilder;
import com.qfs.junit.LocalResourcesExtension;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils.StatisticsSummary;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;
import com.qfs.util.impl.QfsArrays;
import com.qfs.util.impl.QfsFileTestUtils;
import com.quartetfs.biz.pivot.IMultiVersionActivePivot;
import com.quartetfs.biz.pivot.dto.CellDTO;
import com.quartetfs.biz.pivot.dto.CellSetDTO;
import com.quartetfs.biz.pivot.query.impl.MDXQuery;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.query.QueryException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
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
@ExtendWith(RegistrySetupExtension.class)
public class TestMACMeasures {

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

  protected Map<String, Long> applicationStatistics;
  protected StatisticsSummary statisticsSummary;
  protected CubeTester tester;

  @BeforeEach
  public void setup() throws AgentException {
    monitoredApplication = MonitoringTestUtils
        .setupApplication(new MicroApplicationDescription(), resources,
            MicroApplicationDescription::fillWithGenericData);

    final Path exportPath =
        MonitoringTestUtils.exportMostRecentVersion(monitoredApplication.getDatastore(),
            monitoredApplication.getManager(),
            tempDir,
            this.getClass().getSimpleName());

    final IMemoryStatistic stats = MonitoringTestUtils.loadMemoryStatFromFolder(exportPath);
    applicationStatistics = extractApplicationStats(stats);
    statisticsSummary = MemoryStatisticsTestUtils.getStatisticsSummary(stats);

    monitoringApplication = MonitoringTestUtils.setupMonitoringApplication(stats, resources);

    tester = MonitoringTestUtils.createMonitoringCubeTester(monitoringApplication.getManager());
  }

  @Test
  public void testDirectMemorySum() throws QueryException {
    final IMultiVersionActivePivot pivot = tester.pivot();

    final MDXQuery query = new MDXQuery("SELECT"
        + "  NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS"
        + "  FROM [MemoryCube]");
    CellSetDTO res = pivot.execute(query);

    long value = CellSetUtils.extractValueFromSingleCellDTO(res);

    final MDXQuery query2 = new MDXQuery("SELECT"
        + "  NON EMPTY [Measures].[Chunks.COUNT] ON COLUMNS"
        + "  FROM [MemoryCube]");
    CellSetDTO res2 = pivot.execute(query2);
    long nbC = CellSetUtils.extractValueFromSingleCellDTO(res2);

    final MDXQuery query3 = new MDXQuery("SELECT"
        + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
        + "  NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS"
        + "  FROM [MemoryCube]");
    CellSetDTO res3 = pivot.execute(query3);

    // Check that the cell size is the expected one (the amount of chunks)
    Assertions.assertThat(res3.getCells().size())
        .isEqualTo(nbC);
    // Check that the summed value corresponds to the sum on each chunk of the Chunk
    // Level
    Assertions.assertThat(CellSetUtils.sumValuesFromCellSetDTO(res3, 0L, Long::sum))
        .isEqualTo(value);
    // Check that the summed value corresponds to the Exported sum
    Assertions.assertThat(statisticsSummary.offHeapMemory)
        .isEqualTo(value);
  }

  @Test
  public void testOnHeapMemorySum() throws QueryException {
    final IMultiVersionActivePivot pivot = tester.pivot();

    final MDXQuery query = new MDXQuery("SELECT"
        + "  NON EMPTY [Measures].[HeapMemory.SUM] ON COLUMNS"
        + "  FROM [MemoryCube]");
    CellSetDTO res = pivot.execute(query);

    long value = CellSetUtils.extractValueFromSingleCellDTO(res);

    final MDXQuery query2 = new MDXQuery("SELECT"
        + "  NON EMPTY [Measures].[Chunks.COUNT] ON COLUMNS"
        + "  FROM [MemoryCube]");
    CellSetDTO res2 = pivot.execute(query2);
    long nbC = CellSetUtils.extractValueFromSingleCellDTO(res2);

    final MDXQuery query3 = new MDXQuery("SELECT"
        + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
        + "  NON EMPTY [Measures].[HeapMemory.SUM] ON COLUMNS"
        + "  FROM [MemoryCube]");
    CellSetDTO res3 = pivot.execute(query3);

    // Check that the cell size is the expected one (the amount of chunks)
    Assertions.assertThat(res3.getCells().size())
        .isEqualTo(nbC);
    // Check that the summed value corresponds to the sum on each chunk of the Chunk
    // Level
    Assertions.assertThat(CellSetUtils.sumValuesFromCellSetDTO(res3, 0L, Long::sum))
        .isEqualTo(value);

    /*
     * On-heap memory usage by chunks is not consistent with application on-heap
     * usage since on-heap data is not necessarily held by chunks
     */
    // Assertions.assertThat(statsSumm.onHeapMemory).isEqualTo(value);
  }

  @Test
  public void testChunkSize() throws QueryException {
    final IMultiVersionActivePivot pivot = tester.pivot();

    final MDXQuery query = new MDXQuery("SELECT"
        + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
        + "  NON EMPTY [Measures].[ChunkSize.SUM] ON COLUMNS"
        + "  FROM [MemoryCube]");
    CellSetDTO res = pivot.execute(query);

    Assertions.assertThat(CellSetUtils.extractValuesFromCellSetDTO(res))
        .contains((long) MicroApplicationDescription.CHUNK_SIZE);
  }

  @Test
  public void testNonWrittenCount() throws QueryException {
    final IMultiVersionActivePivot pivot = tester.pivot();

    final MDXQuery query = new MDXQuery("SELECT"
        + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
        + "  NON EMPTY [Measures].[NonWrittenRows.COUNT] ON COLUMNS"
        + "  FROM [MemoryCube]");
    CellSetDTO res = pivot.execute(query);

    Assertions.assertThat(CellSetUtils.extractValuesFromCellSetDTO(res))
        .contains((long) MicroApplicationDescription.CHUNK_SIZE
            - MicroApplicationDescription.ADDED_DATA_SIZE);
  }

  @Test
  public void testApplicationMeasures() {
    SoftAssertions.assertSoftly(assertions -> {
      applicationStatistics.forEach(
          cast((measure, value) -> {
            tester.mdxQuery("SELECT"
                + "  NON EMPTY [Measures].["
                + measure
                + "] ON COLUMNS"
                + "  FROM [MemoryCube]")
                .getTester()
                .hasOnlyOneCell()
                .containing(value);
          }));
    });
  }

  /**
   * The measures of the application memory must be constant throughout the application. This checks
   * that whatever the level and depth, those values are the same.
   */
  @Test
  public void testApplicationMeasuresAtAnyPointOfTheCube() {
    SoftAssertions.assertSoftly(
        assertions -> {
          applicationStatistics.forEach(
              cast((measure, value) -> {
                tester.mdxQuery("SELECT"
                    + " NON EMPTY [Measures].["
                    + measure
                    + "] ON COLUMNS"
                    + " FROM [MemoryCube]"
                    + " WHERE ([Owners].[Owner].[ALL].[AllMember].FirstChild)")
                    .getTester()
                    .hasOnlyOneCell()
                    .containing(value);
              }));
        });

    SoftAssertions.assertSoftly(
        assertions -> {
          applicationStatistics.forEach(
              cast((measure, value) -> {
                tester.mdxQuery("SELECT"
                    + " NON EMPTY [Measures].["
                    + measure
                    + "] ON COLUMNS"
                    + " FROM [MemoryCube]"
                    + " WHERE (["
                    + ManagerDescriptionConfig.CHUNK_DIMENSION
                    + "].[ChunkId].[ALL].[AllMember].FirstChild)")
                    .getTester()
                    .hasOnlyOneCell()
                    .containing(value);
              }));
        });
  }

  @Test
  public void testFreedCount() throws QueryException {
    final IMultiVersionActivePivot pivot = tester.pivot();

    final MDXQuery query = new MDXQuery("SELECT"
        + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
        + "  [Measures].[DeletedRows.COUNT] ON COLUMNS"
        + "  FROM [MemoryCube]");
    CellSetDTO res = pivot.execute(query);

    Assertions.assertThat(CellSetUtils.extractValuesFromCellSetDTO(res))
        .contains((long) MicroApplicationDescription.REMOVED_DATA_SIZE);
  }

  @Test
  public void testNonWrittenRatio() throws QueryException {
    final IMultiVersionActivePivot pivot = tester.pivot();

    final MDXQuery query = new MDXQuery("SELECT"
        + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
        + "  NON EMPTY [Measures].[ChunkSize.SUM] ON COLUMNS"
        + "  FROM [MemoryCube]");
    CellSetDTO res = pivot.execute(query);

    final MDXQuery query2 = new MDXQuery("SELECT"
        + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
        + "  NON EMPTY [Measures].[NonWrittenRows.COUNT] ON COLUMNS"
        + "  FROM [MemoryCube]");
    CellSetDTO res2 = pivot.execute(query2);

    final MDXQuery query3 = new MDXQuery("SELECT"
        + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
        + "  NON EMPTY [Measures].[NonWrittenRows.Ratio] ON COLUMNS"
        + "  FROM [MemoryCube]");
    CellSetDTO res3 = pivot.execute(query3);

    final List<Long> chunkSizes = CellSetUtils.extractValuesFromCellSetDTO(res);
    final List<Long> nonWrittenRows = CellSetUtils.extractValuesFromCellSetDTO(res2);
    final List<Double> nonWrittenRatio = CellSetUtils.extractValuesFromCellSetDTO(res3);

    for (int i = 0; i < chunkSizes.size(); i++) {
      Assertions.assertThat(nonWrittenRatio.get(i))
          .isEqualTo((double) nonWrittenRows.get(i) / chunkSizes.get(i));
    }
  }

  @Test
  public void testDeletedRatio() throws QueryException {
    final IMultiVersionActivePivot pivot = tester.pivot();

    final MDXQuery query = new MDXQuery("SELECT"
        + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
        + "  NON EMPTY [Measures].[ChunkSize.SUM] ON COLUMNS"
        + "  FROM [MemoryCube]");
    CellSetDTO res = pivot.execute(query);

    final MDXQuery query2 = new MDXQuery("SELECT"
        + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
        + "  NON EMPTY [Measures].[DeletedRows.COUNT] ON COLUMNS"
        + "  FROM [MemoryCube]");
    CellSetDTO res2 = pivot.execute(query2);

    final MDXQuery query3 = new MDXQuery("SELECT"
        + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
        + "  NON EMPTY [Measures].[DeletedRows.Ratio] ON COLUMNS"
        + "  FROM [MemoryCube]");
    CellSetDTO res3 = pivot.execute(query3);

    final List<Long> chunkSizes = CellSetUtils.extractValuesFromCellSetDTO(res);
    final List<Long> deletedRows = CellSetUtils.extractValuesFromCellSetDTO(res2);
    final List<Double> deletedRatio = CellSetUtils.extractValuesFromCellSetDTO(res3);

    for (int i = 0; i < chunkSizes.size(); i++) {
      Assertions.assertThat(deletedRatio.get(i))
          .isEqualTo((double) deletedRows.get(i) / chunkSizes.get(i));
    }
  }

  @Test
  public void testDictionarySize() throws QueryException {
    final IMultiVersionActivePivot pivot = tester.pivot();

    final MDXQuery query = new MDXQuery("SELECT"
        + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
        + " NON EMPTY [Measures].[DictionarySize.SUM] ON COLUMNS"
        + " FROM [MemoryCube]"
        + " WHERE ("
        + "  [Owners].[Owner].[Owner].[Store A],"
        + "  [Fields].[Field].[Field].[id]"
        + " )");
    CellSetDTO res = pivot.execute(query);

    final long expectedDictionarySize = monitoredApplication.getDatastore().getDictionaries()
        .getDictionary("A", "id")
        .size();

    Assertions.assertThat(res.getCells())
        .isNotEmpty()
        .extracting(CellDTO::getValue)
        .containsOnly(expectedDictionarySize);
  }

  private static Map<String, Long> extractApplicationStats(final IMemoryStatistic export) {
    final IMemoryStatistic firstChild = export.getChildren().iterator().next();
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
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> firstChild.getAttributes().get(entry.getValue()).asLong()));
  }
}
