/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.entities.ChunkOwner;
import com.activeviam.mac.entities.CubeOwner;
import com.activeviam.mac.entities.StoreOwner;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.statistic.memory.visitor.impl.EpochView;
import com.activeviam.pivot.builders.StartBuilding;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.pivot.monitoring.impl.MemoryAnalysisService;
import com.qfs.store.IDatastore;
import com.qfs.store.query.ICursor;
import com.qfs.store.record.IRecordReader;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.qfs.store.transaction.ITransactionManager;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.IMultiVersionActivePivot;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.contributions.impl.ClasspathContributionProvider;
import com.quartetfs.fwk.impl.Pair;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestViewEpochs extends ATestMemoryStatistic {

  private Pair<IDatastore, IActivePivotManager> monitoredApp;
  private Pair<IDatastore, IActivePivotManager> monitoringApp;
  private IMemoryStatistic statistics;

  @BeforeClass
  public static void setupRegistry() {
    Registry.setContributionProvider(new ClasspathContributionProvider());
  }

  @Before
  public void setup() throws AgentException, DatastoreTransactionException {
    initializeApplication();

    final Path exportPath = generateMemoryStatistics();

    statistics = loadMemoryStatFromFolder(exportPath);

    initializeMonitoringApplication(statistics);

    IMultiVersionActivePivot pivot =
        monitoringApp.getRight().getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);
    Assertions.assertThat(pivot).isNotNull();
  }

  private void initializeApplication() throws DatastoreTransactionException {
    monitoredApp = createMicroApplicationWithIsolatedStoreAndKeepAllEpochPolicy();

    final ITransactionManager transactionManager = monitoredApp.getLeft().getTransactionManager();

    // epoch 1 -> store A + cube
    transactionManager.startTransaction("A");
    IntStream.range(0, 10).forEach(i -> transactionManager.add("A", i, 0.));
    transactionManager.commitTransaction();

    // epoch 2 -> store B
    transactionManager.startTransaction("B");
    IntStream.range(0, 10).forEach(i -> transactionManager.add("B", i, 0.));
    transactionManager.commitTransaction();

    // epoch 3 -> store A + cube
    transactionManager.startTransaction("A");
    IntStream.range(10, 20).forEach(i -> transactionManager.add("A", i, 1.));
    transactionManager.commitTransaction();

    // epoch 4 -> store B
    transactionManager.startTransaction("B");
    IntStream.range(10, 20).forEach(i -> transactionManager.add("B", i, 1.));
    transactionManager.commitTransaction();
  }

  private Path generateMemoryStatistics() {
    monitoredApp.getLeft().getEpochManager().forceDiscardEpochs(node -> true);
    performGC();

    final MemoryAnalysisService analysisService =
        (MemoryAnalysisService) createService(monitoredApp.getLeft(), monitoredApp.getRight());
    return analysisService.exportApplication("testEpochs");
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
    monitoringApp = new Pair<>(monitoringDatastore, manager);

    ATestMemoryStatistic.feedMonitoringApplication(
        monitoringDatastore, List.of(data), "testViewEpochs");
  }

  @After
  public void tearDown() throws AgentException {
    monitoringApp.getLeft().close();
    monitoringApp.getRight().stop();
  }

  @Test
  public void testStatisticConsistency() {
    ATestMemoryStatistic.assertLoadsCorrectly(statistics.getChildren(), TestEpochs.class);
  }

  @Test
  public void testExpectedEpochs() {
    final Multimap<ChunkOwner, Long> epochIds = retrieveEpochIdsPerOwner();

    final ChunkOwner storeA = new StoreOwner("A");
    final ChunkOwner storeB = new StoreOwner("B");
    final ChunkOwner cube = new CubeOwner("Cube");

    SoftAssertions.assertSoftly(
        assertions -> {
          assertions
              .assertThat(epochIds.keySet())
              .as("application owners")
              .containsExactlyInAnyOrder(storeA, storeB, cube);

          assertions
              .assertThat(epochIds.get(storeA))
              .as("store A epochs")
              .containsExactlyInAnyOrder(1L, 3L);
          assertions
              .assertThat(epochIds.get(storeB))
              .as("store B epochs")
              .containsExactlyInAnyOrder(0L, 2L, 4L);
          assertions
              .assertThat(epochIds.get(cube))
              .as("cube epochs")
              .containsExactlyInAnyOrder(1L, 3L);
        });
  }

  @Test
  public void testViewEpochMapping() {
    final Map<ChunkOwner, Multimap<Long, Long>> viewEpochs = retrieveViewEpochIdsPerOwner();

    final ChunkOwner storeA = new StoreOwner("A");
    final ChunkOwner storeB = new StoreOwner("B");
    final ChunkOwner cube = new CubeOwner("Cube");

    SoftAssertions.assertSoftly(
        assertions -> {
          assertions
              .assertThat(viewEpochs.keySet())
              .as("application owners")
              .containsExactlyInAnyOrder(storeA, storeB, cube);

          assertions
              .assertThat(viewEpochs.get(storeA).asMap())
              .as("store A epochs")
              .containsExactlyInAnyOrderEntriesOf(
                  Map.of(
                      1L, Set.of(1L, 2L),
                      3L, Set.of(3L, 4L)));
          assertions
              .assertThat(viewEpochs.get(storeB).asMap())
              .as("store B epochs")
              .containsExactlyInAnyOrderEntriesOf(
                  Map.of(
                      0L, Set.of(0L, 1L),
                      2L, Set.of(2L, 3L),
                      4L, Set.of(4L)));
          assertions
              .assertThat(viewEpochs.get(cube).asMap())
              .as("cube epochs")
              .containsExactlyInAnyOrderEntriesOf(
                  Map.of(
                      1L, Set.of(1L, 2L),
                      3L, Set.of(3L, 4L)));
        });
  }

  protected Multimap<ChunkOwner, Long> retrieveEpochIdsPerOwner() {
    final ICursor cursor =
        monitoringApp
            .getLeft()
            .getHead()
            .getQueryRunner()
            .forStore(DatastoreConstants.CHUNK_STORE)
            .withoutCondition()
            .selecting(DatastoreConstants.OWNER__OWNER, DatastoreConstants.VERSION__EPOCH_ID)
            .onCurrentThread()
            .run();

    final Multimap<ChunkOwner, Long> epochs = HashMultimap.create();
    for (final IRecordReader record : cursor) {
      epochs.put((ChunkOwner) record.read(0), record.readLong(1));
    }

    return epochs;
  }

  protected Map<ChunkOwner, Multimap<Long, Long>> retrieveViewEpochIdsPerOwner() {
    final ICursor cursor =
        monitoringApp
            .getLeft()
            .getHead()
            .getQueryRunner()
            .forStore(DatastoreConstants.EPOCH_VIEW_STORE)
            .withoutCondition()
            .selecting(
                DatastoreConstants.EPOCH_VIEW__OWNER,
                DatastoreConstants.EPOCH_VIEW__BASE_EPOCH_ID,
                DatastoreConstants.EPOCH_VIEW__VIEW_EPOCH_ID)
            .onCurrentThread()
            .run();

    final Map<ChunkOwner, Multimap<Long, Long>> epochs = new HashMap<>();
    for (final IRecordReader record : cursor) {
      epochs
          .computeIfAbsent((ChunkOwner) record.read(0), key -> HashMultimap.create())
          .put(record.readLong(1), ((EpochView) record.read(2)).getEpochId());
    }

    return epochs;
  }
}
