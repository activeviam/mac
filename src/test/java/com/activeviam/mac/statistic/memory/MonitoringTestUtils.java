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
import static com.activeviam.mac.memory.DatastoreConstants.CHUNK__OFF_HEAP_SIZE;
import static com.activeviam.mac.memory.DatastoreConstants.CHUNK__PARENT_ID;
import static com.activeviam.mac.memory.DatastoreConstants.VERSION__EPOCH_ID;

import com.activeviam.copper.testing.CubeTester;
import com.activeviam.copper.testing.query.CubeTesterImpl;
import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.entities.NoOwner;
import com.activeviam.mac.memory.AnalysisDatastoreFeeder;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription.ParentType;
import com.activeviam.mac.statistic.memory.descriptions.ITestApplicationDescription;
import com.activeviam.pivot.builders.StartBuilding;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.qfs.condition.ICondition;
import com.qfs.condition.impl.BaseConditions;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.junit.AResourcesExtension;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils.StatisticsSummary;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.impl.MemoryStatisticBuilder;
import com.qfs.multiversion.IEpochManagementPolicy;
import com.qfs.pivot.monitoring.impl.MemoryAnalysisService;
import com.qfs.pivot.monitoring.impl.MemoryStatisticSerializerUtil;
import com.qfs.service.monitoring.IMemoryAnalysisService;
import com.qfs.store.IDatastore;
import com.qfs.store.query.IDictionaryCursor;
import com.qfs.store.record.IRecordFormat;
import com.qfs.store.record.IRecordReader;
import com.qfs.util.impl.ThrowingLambda.ThrowingBiConsumer;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.definitions.impl.ActivePivotDatastorePostProcessor;
import com.quartetfs.fwk.AgentException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.Data;
import lombok.experimental.UtilityClass;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import test.util.impl.DatastoreTestUtils;

@UtilityClass
public class MonitoringTestUtils {

  public static final int MAX_GC_STEPS = 10;
  public static final String DEFAULT_DUMP_NAME = "testDump";

  public static Application setupApplication(
      ITestApplicationDescription applicationDescription,
      AResourcesExtension resourcesExtension,
      ThrowingBiConsumer<IDatastore, IActivePivotManager> operations)
      throws AgentException {

    final IDatastoreSchemaDescription datastoreSchemaDescription =
        applicationDescription.datastoreDescription();
    final IActivePivotManagerDescription managerDescription =
        applicationDescription.managerDescription(datastoreSchemaDescription);
    final IEpochManagementPolicy epochManagementPolicy =
        applicationDescription.epochManagementPolicy();

    final IDatastore datastore = resourcesExtension.create(StartBuilding.datastore()
        .setSchemaDescription(datastoreSchemaDescription)
        .addSchemaDescriptionPostProcessors(
            ActivePivotDatastorePostProcessor.createFrom(managerDescription))
        .setEpochManagementPolicy(epochManagementPolicy)
        ::build);

    final IActivePivotManager manager = StartBuilding.manager()
        .setDescription(managerDescription)
        .setDatastoreAndPermissions(datastore)
        .buildAndStart();
    resourcesExtension.register(manager::stop);

    operations.accept(datastore, manager);

    return new Application(datastore, manager);
  }

  public static Application setupMonitoringApplication(
      IMemoryStatistic statistics,
      AResourcesExtension resourcesExtension)
      throws AgentException {

    final Application monitoringApplication = setupMonitoringApplication(resourcesExtension);
    feedStatisticsIntoDatastore(statistics, monitoringApplication.getDatastore());
    return monitoringApplication;
  }

  public static Application setupMonitoringApplication(
      AResourcesExtension resourcesExtension)
      throws AgentException {

    final ManagerDescriptionConfig config = new ManagerDescriptionConfig();
    final IDatastoreSchemaDescription datastoreSchemaDescription = config.schemaDescription();
    final IActivePivotManagerDescription managerDescription = config.managerDescription();

    final IDatastore datastore = resourcesExtension.create(StartBuilding.datastore()
        .setSchemaDescription(datastoreSchemaDescription)
        .addSchemaDescriptionPostProcessors(
            ActivePivotDatastorePostProcessor.createFrom(managerDescription))
        ::build);

    final IActivePivotManager manager = StartBuilding.manager()
        .setDescription(managerDescription)
        .setDatastoreAndPermissions(datastore)
        .buildAndStart();
    resourcesExtension.register(manager::stop);

    return new Application(datastore, manager);
  }

  protected static void feedStatisticsIntoDatastore(
      IMemoryStatistic statistics, IDatastore datastore) {
    feedStatisticsIntoDatastore(statistics, datastore, DEFAULT_DUMP_NAME);
  }

  protected static void feedStatisticsIntoDatastore(
      IMemoryStatistic statistics, IDatastore datastore, String dumpName) {
    final AnalysisDatastoreFeeder feeder = new AnalysisDatastoreFeeder(dumpName);
    feeder.loadInto(datastore, Stream.of(statistics));
  }

  public static Path exportApplication(
      final IDatastore datastore,
      final IActivePivotManager manager,
      final Path directory,
      final String folderSuffix) {
    return setupForExport(datastore, manager, directory)
        .exportApplication(folderSuffix);
  }

  public static Path exportMostRecentVersion(
      final IDatastore datastore,
      final IActivePivotManager manager,
      final Path directory,
      final String folderSuffix) {
    return setupForExport(datastore, manager, directory)
        .exportMostRecentVersion(folderSuffix);
  }

  public static Path exportVersions(
      final IDatastore datastore,
      final IActivePivotManager manager,
      final Path directory,
      final String folderSuffix,
      final long[] selectedEpochs) {
    return setupForExport(datastore, manager, directory)
        .exportVersions(folderSuffix, selectedEpochs);
  }

  public static Path exportBranches(
      final IDatastore datastore,
      final IActivePivotManager manager,
      final Path directory,
      final String folderSuffix,
      final Set<String> selectedBranches) {
    return setupForExport(datastore, manager, directory)
        .exportBranches(folderSuffix, selectedBranches);
  }

  protected static IMemoryAnalysisService setupForExport(
      final IDatastore datastore, final IActivePivotManager manager, final Path directory) {
    performGC();
    datastore.getEpochManager().forceDiscardEpochs(x -> true);

    return createAnalysisService(datastore, manager, directory);
  }

  public static void performGC() {
    // Make sure that no thread holds stale blocks.
    DatastoreTestUtils.resetAllThreadsVectorAllocator();

    /*
     * Note. We can't rely on calling MemUtils.runGC()
     * because on some servers (alto), it seems not enough.
     * Plus, MemUtils relies on on heap memory....
     */
    for (int i = 0; i < MAX_GC_STEPS; i++) {
      try {
        System.gc();
        Thread.sleep(1 << i);
        break;
      } catch (Throwable e) {
        if (i == MAX_GC_STEPS - 1) {
          throw new RuntimeException(
              "Incorrect direct memory count or reserved memory after " + MAX_GC_STEPS + " gcs.",
              e);
        }
      }
    }
  }

  public static IMemoryStatistic loadMemoryStatFromFolder(final Path folderPath) {
    return loadMemoryStatFromFolder(folderPath, x -> true);
  }

  public static IMemoryStatistic loadDatastoreMemoryStatFromFolder(
      final Path folderPath) {
    return loadMemoryStatFromFolder(
        folderPath,
        path -> path.getFileName().toString()
            .startsWith(MemoryAnalysisService.STORE_FILE_PREFIX));
  }

  public static IMemoryStatistic loadPivotMemoryStatFromFolder(
      final Path folderPath) {
    return loadMemoryStatFromFolder(
        folderPath,
        path -> path.getFileName().toString()
            .startsWith(MemoryAnalysisService.PIVOT_FILE_PREFIX));
  }

  public static IMemoryStatistic loadMemoryStatFromFolder(
      final Path folderPath, final Predicate<Path> filter) {
    final List<IMemoryStatistic> childStats;
    try (final var fileList = Files.list(folderPath)) {
      childStats = fileList.filter(filter)
          .map(file -> {
            try {
              return MemoryStatisticSerializerUtil.readStatisticFile(file.toFile());
            } catch (IOException e) {
              throw new RuntimeException("Cannot read " + file, e);
            }
          }).collect(Collectors.toList());
    } catch (IOException e) {
      throw new IllegalArgumentException("Cannot list files under " + folderPath, e);
    }

    return new MemoryStatisticBuilder()
        .withCreatorClasses(MonitoringTestUtils.class)
        .withChildren(childStats)
        .build();
  }

  public static CubeTester createMonitoringCubeTester(final IActivePivotManager manager) {
    return new CubeTesterImpl(ManagerDescriptionConfig.MONITORING_CUBE, manager);
  }

  public static IMemoryAnalysisService createAnalysisService(
      final IDatastore datastore, final IActivePivotManager manager, final Path dumpFolder) {
    return new MemoryAnalysisService(datastore, manager, datastore.getEpochManager(), dumpFolder);
  }

  public static IDatastore assertLoadsCorrectly(
      IMemoryStatistic statistic,
      AResourcesExtension resourcesExtension) {
    final IDatastore monitoringDatastore = createAnalysisDatastore(statistic, resourcesExtension);

    final StatisticsSummary statisticsSummary =
        MemoryStatisticsTestUtils.getStatisticsSummary(statistic);

    assertDatastoreConsistentWithSummary(monitoringDatastore, statisticsSummary);

    checkForUnclassifiedChunks(monitoringDatastore);
    checkForUnrootedChunks(monitoringDatastore);

    return monitoringDatastore;
  }

  public static IDatastore createAnalysisDatastore(
      IMemoryStatistic statistics,
      AResourcesExtension resourcesExtension) {
    final IDatastore datastore = createAnalysisDatastore(resourcesExtension);
    feedStatisticsIntoDatastore(statistics, datastore);
    return datastore;
  }

  public static IDatastore createAnalysisDatastore(AResourcesExtension resourcesExtension) {
    return resourcesExtension.create(StartBuilding.datastore()
        .setSchemaDescription(new MemoryAnalysisDatastoreDescription())
        ::build);
  }

  public static String ownershipCountMdxExpression(final String hierarchyUniqueName) {
    return "DistinctCount("
        + "  Generate("
        + "    NonEmpty("
        + "      [Chunks].[ChunkId].[ALL].[AllMember].Children,"
        + "      {[Measures].[contributors.COUNT]}"
        + "    ),"
        + "    NonEmpty("
        + "      " + hierarchyUniqueName + ".[ALL].[AllMember].Children,"
        + "      {[Measures].[contributors.COUNT]}"
        + "    )"
        + "  )"
        + ")";
  }

  /**
   * Asserts the monitoring datastore contains chunks consistent with what the statistics summary
   * says.
   *
   * @param monitoringDatastore The monitoring datastore in which the statistics was loaded.
   * @param statisticsSummary The statistics summary we want to compare the datastore with.
   */
  private static void assertDatastoreConsistentWithSummary(
      IDatastore monitoringDatastore, StatisticsSummary statisticsSummary) {

    final Map<Long, VersionedChunkInfo> latestChunkInfos =
        extractLatestChunkInfos(monitoringDatastore);

    final long chunkCount = latestChunkInfos.size();
    final long totalChunkOffHeapSize = latestChunkInfos.values().stream()
        .mapToLong(chunk -> chunk.offHeapSize)
        .sum();
    Multimap<String, Long> chunkIdsByClass = HashMultimap.create();
    latestChunkInfos.forEach((key, value) -> chunkIdsByClass.put(value.chunkClass, key));

    SoftAssertions.assertSoftly(
        assertions -> {
          assertions
              .assertThat(totalChunkOffHeapSize)
              .as("off-heap memory computed on monitoring datastore")
              .isEqualTo(statisticsSummary.offHeapMemory);
          assertions
              .assertThat(chunkCount)
              .as("total number of chunks loaded in monitoring store")
              .isEqualTo(statisticsSummary.numberDistinctChunks);
          assertions
              .assertThat(chunkIdsByClass.asMap())
              .as("Classes of the loaded chunks")
              .containsAllEntriesOf(statisticsSummary.chunkIdsByClass);
        });
  }

  private static Map<Long, VersionedChunkInfo> extractLatestChunkInfos(final IDatastore datastore) {
    IDictionaryCursor cursor = datastore
        .getHead()
        .getQueryRunner()
        .forStore(CHUNK_STORE)
        .withoutCondition()
        .selecting(CHUNK_ID, VERSION__EPOCH_ID, CHUNK__OFF_HEAP_SIZE, CHUNK__CLASS)
        .onCurrentThread()
        .run();

    final Map<Long, VersionedChunkInfo> latestChunkInfos = new HashMap<>();
    for (final IRecordReader reader : cursor) {
      final long chunkId = reader.readLong(0);
      final long epochId = reader.readLong(1);

      final VersionedChunkInfo chunkInfo = latestChunkInfos.get(chunkId);
      if (chunkInfo == null || chunkInfo.epochId < epochId) {
        final long offHeapSize = reader.readLong(2);
        final String chunkClass = (String) reader.read(3);
        latestChunkInfos.put(
            chunkId,
            new VersionedChunkInfo(epochId, offHeapSize, chunkClass));
      }
    }

    return latestChunkInfos;
  }

  private static void checkForUnclassifiedChunks(IDatastore monitoringDatastore) {
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

  /**
   * Checks that all chunks have an owner and a component.
   */
  private static void checkForUnrootedChunks(final IDatastore monitoringDatastore) {
    final Set<Long> unrootedChunks =
        retrieveAllChunkIds(
            monitoringDatastore,
            BaseConditions.Or(
                BaseConditions.Equal(
                    DatastoreConstants.OWNER__OWNER,
                    NoOwner.getInstance()),
                BaseConditions.Equal(
                    DatastoreConstants.OWNER__COMPONENT,
                    ParentType.NO_COMPONENT)));

    Assertions.assertThat(unrootedChunks).isEmpty();
  }

  private static Set<Long> retrieveAllChunkIds(
      final IDatastore monitoringDatastore,
      final ICondition condition) {
    final IDictionaryCursor cursor =
        monitoringDatastore
            .getHead()
            .getQueryRunner()
            .forStore(DatastoreConstants.CHUNK_STORE)
            .withCondition(condition)
            .selecting(DatastoreConstants.CHUNK_ID)
            .onCurrentThread()
            .run();

    return StreamSupport.stream(cursor.spliterator(), false)
        .map(reader -> reader.readLong(0))
        .collect(Collectors.toSet());
  }

  @Data
  protected static class VersionedChunkInfo {

    public final long epochId;
    public final long offHeapSize;
    public final String chunkClass;
  }
}
