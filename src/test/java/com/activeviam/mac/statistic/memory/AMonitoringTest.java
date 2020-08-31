/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import static com.activeviam.mac.memory.DatastoreConstants.CHUNK_ID;
import static com.activeviam.mac.memory.DatastoreConstants.CHUNK_STORE;
import static com.activeviam.mac.memory.DatastoreConstants.CHUNK__CLASS;
import static com.activeviam.mac.memory.DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE;
import static com.activeviam.mac.memory.DatastoreConstants.CHUNK__COMPONENT;
import static com.activeviam.mac.memory.DatastoreConstants.CHUNK__OFF_HEAP_SIZE;
import static com.activeviam.mac.memory.DatastoreConstants.CHUNK__OWNER;
import static com.activeviam.mac.memory.DatastoreConstants.CHUNK__PARENT_ID;

import com.activeviam.fwk.ActiveViamRuntimeException;
import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.entities.NoOwner;
import com.activeviam.mac.statistic.memory.visitor.impl.FeedVisitor;
import com.activeviam.pivot.builders.StartBuilding;
import com.activeviam.properties.impl.ActiveViamProperty;
import com.activeviam.properties.impl.ActiveViamPropertyExtension;
import com.qfs.chunk.direct.impl.SlabDirectChunkAllocator;
import com.qfs.chunk.impl.Chunks;
import com.qfs.condition.impl.BaseConditions;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.junit.LocalResourcesExtension;
import com.qfs.monitoring.memory.impl.OnHeapPivotMemoryQuantifierPlugin;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils.StatisticsSummary;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.impl.MemoryStatisticBuilder;
import com.qfs.pivot.monitoring.impl.MemoryAnalysisService;
import com.qfs.pivot.monitoring.impl.MemoryStatisticSerializerUtil;
import com.qfs.service.monitoring.IMemoryAnalysisService;
import com.qfs.store.IDatastore;
import com.qfs.store.query.IDictionaryCursor;
import com.qfs.store.record.IRecordFormat;
import com.qfs.store.record.IRecordReader;
import com.qfs.util.impl.QfsFileTestUtils;
import com.qfs.util.impl.ThrowingLambda.ThrowingBiConsumer;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.definitions.impl.ActivePivotDatastorePostProcessor;
import com.quartetfs.biz.pivot.test.util.PivotTestUtils;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.contributions.impl.ClasspathContributionProvider;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import test.util.impl.DatastoreTestUtils;

public abstract class AMonitoringTest {

  public static final int MAX_GC_STEPS = 10;

  @RegisterExtension
  protected LocalResourcesExtension resources = new LocalResourcesExtension();

  @RegisterExtension
  protected ActiveViamPropertyExtension properties = ActiveViamPropertyExtension.builder()
      .withProperty(ActiveViamProperty.ACTIVEVIAM_TEST_PROPERTY, true)
      .build();

  protected Path statisticsPath;
  protected IDatastore monitoredDatastore;
  protected IActivePivotManager monitoredManager;
  protected IDatastore monitoringDatastore;
  protected IActivePivotManager monitoringManager;
  protected Collection<IMemoryStatistic> statistics;

  @BeforeAll
  public static void setUpRegistry() {
    PivotTestUtils.setUpRegistry(OnHeapPivotMemoryQuantifierPlugin.class);
    Registry.setContributionProvider(new ClasspathContributionProvider());
  }

  protected void setupAndExportMonitoredApplication(
      final IDatastoreSchemaDescription datastoreSchemaDescription,
      final IActivePivotManagerDescription managerDescription,
      final ThrowingBiConsumer<IDatastore, IActivePivotManager> beforeExport)
      throws AgentException {
    monitoredDatastore = StartBuilding.datastore()
        .setSchemaDescription(datastoreSchemaDescription)
        .addSchemaDescriptionPostProcessors(
            ActivePivotDatastorePostProcessor.createFrom(managerDescription))
        .build();
    resources.register(monitoredDatastore);

    monitoredManager = StartBuilding.manager()
        .setDescription(managerDescription)
        .setDatastoreAndPermissions(monitoredDatastore)
        .buildAndStart();
    resources.register(monitoredManager::stop);

    beforeExport.accept(monitoredDatastore, monitoredManager);
    performGC();
    exportApplicationMemoryStatistics(monitoredDatastore, monitoredManager, getTempDirectory());
  }

  protected void setupMac() throws AgentException {
    final ManagerDescriptionConfig config = new ManagerDescriptionConfig();
    monitoringDatastore = createAnalysisDatastore(config);
    resources.register(monitoringDatastore);
    monitoringManager = createAndStartAnalysisActivePivotManager(config, monitoringDatastore);
    resources.register(monitoringManager::stop);
  }

  protected static void performGC() {
    // Make sure that no thread holds stale blocks.
    DatastoreTestUtils.resetAllThreadsVectorAllocator();

    /*
     * Note. We can't rely on calling MemUtils.runGC()
     * because on some servers (alto), it seems not enough.
     * Plus, MemUtils relies on on heap memory....
     */
    final SlabDirectChunkAllocator allocator = (SlabDirectChunkAllocator) Chunks.allocator();
    for (int i = 0; i < MAX_GC_STEPS; i++) {
      try {
        System.gc();

        Thread.sleep(1 << i); // give gc some times.

        // create a soft assertion that allows getting the assertions results of all assertions
        // even if the first assertion is already false.
        break;
      } catch (Throwable e) {
        if (i == MAX_GC_STEPS - 1) {
          // MAX_GC was not enough, throw !
          throw new RuntimeException(
              "Incorrect direct memory count or reserved memory after " + MAX_GC_STEPS + " gcs.",
              e);
        }
      }
    }
  }

  protected void exportApplicationMemoryStatistics(
      final IDatastore datastore, final IActivePivotManager manager, final Path exportPath) {
    final IMemoryAnalysisService analysisService = new MemoryAnalysisService(
        datastore, manager, datastore.getEpochManager(), exportPath);
    statisticsPath = analysisService.exportMostRecentVersion("memoryStatistics");
  }

  protected Path getTempDirectory() {
    return QfsFileTestUtils.createTempDirectory(this.getClass());
  }

  protected IDatastore createAnalysisDatastore(final ManagerDescriptionConfig config) {
    return StartBuilding.datastore().setSchemaDescription(config.schemaDescription()).build();
  }

  protected IActivePivotManager createAndStartAnalysisActivePivotManager(
      final ManagerDescriptionConfig config, final IDatastore datastore)
      throws AgentException {
    return StartBuilding.manager()
        .setDescription(config.managerDescription())
        .setDatastoreAndPermissions(datastore)
        .buildAndStart();
  }

  protected void loadAndImportStatistics() throws IOException {
    statistics = loadMemoryStatistic(statisticsPath);
    feedStatisticsIntoDatastore(statistics, monitoringDatastore);
  }

  protected Collection<IMemoryStatistic> loadMemoryStatistic(final Path path) throws IOException {
    return loadMemoryStatistic(path, p -> true);
  }

  protected Collection<IMemoryStatistic> loadMemoryStatistic(
      final Path path, final Predicate<Path> filter) throws IOException {
    return Files.list(path)
        .filter(filter)
        .map(file -> {
          try {
            return MemoryStatisticSerializerUtil.readStatisticFile(file.toFile());
          } catch (IOException exception) {
            throw new ActiveViamRuntimeException(exception);
          }
        })
        .collect(Collectors.toList());
  }

  protected StatisticsSummary computeStatisticsSummary(
      final Collection<IMemoryStatistic> statistics) {
    return MemoryStatisticsTestUtils.getStatisticsSummary(aggregateMemoryStatistics(statistics));
  }

  protected IMemoryStatistic aggregateMemoryStatistics(
      final Collection<IMemoryStatistic> statistics) {
    return new MemoryStatisticBuilder()
        .withCreatorClasses(this.getClass())
        .withChildren(statistics)
        .build();
  }

  protected void feedStatisticsIntoDatastore(
      final Collection<? extends IMemoryStatistic> statistics, final IDatastore analysisDatastore) {
    analysisDatastore.edit(transactionManager ->
        statistics.forEach(statistic ->
            statistic.accept(
                new FeedVisitor(analysisDatastore.getSchemaMetadata(), transactionManager,
                    "dump"))));
  }

  protected void assertStatisticsConsistency() {
    final StatisticsSummary statisticsSummary = computeStatisticsSummary(statistics);

    assertConsistencyWithSummary(monitoringDatastore, statisticsSummary);
    checkForUnclassifiedChunks(monitoringDatastore);
    checkForUnrootedChunks(monitoringDatastore);
  }

  /**
   * Asserts the monitoring datastore contains chunks consistent with what the statistics summary
   * says.
   *
   * @param monitoringDatastore The monitoring datastore in which the statistics was loaded.
   * @param statisticsSummary The statistics summary we want to compare the datastore with.
   */
  protected static void assertConsistencyWithSummary(
      final IDatastore monitoringDatastore, final StatisticsSummary statisticsSummary) {
    IDictionaryCursor cursor =
        monitoringDatastore
            .getHead()
            .getQueryRunner()
            .forStore(CHUNK_STORE)
            .withoutCondition()
            .selecting(CHUNK__OFF_HEAP_SIZE, CHUNK_ID, CHUNK__CLASS)
            .onCurrentThread()
            .run();

    // Count all chunks plus the total allocated amount of bytes
    final LongAdder sum = new LongAdder();
    final LongAdder countFromStore = new LongAdder();
    Map<String, Collection<Long>> chunkIdsByClass = new HashMap<>();
    while (cursor.hasNext()) {
      cursor.next();
      IRecordReader reader = cursor.getRecord();
      sum.add((long) reader.read(CHUNK__OFF_HEAP_SIZE));
      countFromStore.increment();
      final String chunkClass = (String) reader.read(CHUNK__CLASS);
      final long chunkId = (long) reader.read(CHUNK_ID);
      final Collection<Long> v = chunkIdsByClass.computeIfAbsent(chunkClass, __ -> new HashSet<>());
      if (!v.add(chunkId)) {
        System.out.println("Chunk ID " + chunkId + " already existed.");
        final IDictionaryCursor chunkIdCursor =
            monitoringDatastore
                .getHead()
                .getQueryRunner()
                .forStore(CHUNK_STORE)
                .withCondition(BaseConditions.Equal(CHUNK_ID, chunkId))
                .selectingAllStoreFields()
                .onCurrentThread()
                .run();
        while (chunkIdCursor.hasNext()) {
          chunkIdCursor.next();
          System.out.println(Arrays.toString(chunkIdCursor.getRecord().toTuple()));
        }
      }
    }

    SoftAssertions.assertSoftly(
        assertions -> {
          assertions
              .assertThat(sum.longValue())
              .as("off-heap memory computed on monitoring datastore")
              .isEqualTo(statisticsSummary.offHeapMemory);
          assertions
              .assertThat(countFromStore.longValue())
              .as("total number of chunks loaded in monitoring store")
              .isEqualTo(statisticsSummary.numberDistinctChunks);
          assertions
              .assertThat(chunkIdsByClass)
              .as("Classes of the loaded chunks")
              .containsAllEntriesOf(statisticsSummary.chunkIdsByClass);
        });
  }

  protected static void checkForUnclassifiedChunks(IDatastore monitoringDatastore) {
    // Check that all chunks have a parent type/id
    final IDictionaryCursor cursor =
        monitoringDatastore
            .getHead()
            .getQueryRunner()
            .forStore(CHUNK_STORE)
            .withCondition(
                BaseConditions.Or(
                    BaseConditions.Equal(
                        CHUNK__CLOSEST_PARENT_TYPE, IRecordFormat.GLOBAL_DEFAULT_STRING),
                    BaseConditions.Equal(CHUNK__PARENT_ID, IRecordFormat.GLOBAL_DEFAULT_STRING)))
            .selecting(CHUNK_ID, CHUNK__CLASS, CHUNK__CLOSEST_PARENT_TYPE, CHUNK__PARENT_ID)
            .onCurrentThread()
            .run();
    if (cursor.hasNext()) {
      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count += 1;
        System.out.println("Error for " + cursor.getRawRecord());
      }
      throw new AssertionError(count + " chunks without parent type/id");
    }
  }

  protected static void checkForUnrootedChunks(IDatastore monitoringDatastore) {
    // Check that all chunks have an owner
    final IDictionaryCursor cursor =
        monitoringDatastore
            .getHead()
            .getQueryRunner()
            .forStore(CHUNK_STORE)
            .withCondition(
                BaseConditions.Or(
                    BaseConditions.Equal(CHUNK__OWNER, NoOwner.getInstance()),
                    BaseConditions.Equal(CHUNK__COMPONENT, IRecordFormat.GLOBAL_DEFAULT_STRING)))
            .selecting(CHUNK_ID, CHUNK__CLASS, CHUNK__OWNER, CHUNK__COMPONENT)
            .onCurrentThread()
            .run();
    if (cursor.hasNext()) {
      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count += 1;
        System.out.println("Error for " + cursor.getRawRecord());
      }
      throw new AssertionError(count + " chunks without owner or component");
    }
  }
}
