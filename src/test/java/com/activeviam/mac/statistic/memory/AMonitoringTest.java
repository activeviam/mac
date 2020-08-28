/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.activeviam.fwk.ActiveViamRuntimeException;
import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.statistic.memory.visitor.impl.FeedVisitor;
import com.activeviam.pivot.builders.StartBuilding;
import com.activeviam.properties.impl.ActiveViamProperty;
import com.activeviam.properties.impl.ActiveViamPropertyExtension;
import com.qfs.chunk.direct.impl.SlabDirectChunkAllocator;
import com.qfs.chunk.impl.Chunks;
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
import java.util.Collection;
import java.util.stream.Collectors;
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

  protected Collection<IMemoryStatistic> loadMemoryStatistic(final Path path) throws IOException {
    return Files.list(path)
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

  protected void loadStatisticsIntoDatastore(
      final Collection<? extends IMemoryStatistic> statistics, final IDatastore analysisDatastore) {
    analysisDatastore.edit(transactionManager ->
        statistics.forEach(statistic ->
            statistic.accept(
                new FeedVisitor(analysisDatastore.getSchemaMetadata(), transactionManager,
                    "dump"))));
  }
}
