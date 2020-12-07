/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.activeviam.mac.entities.ChunkOwner;
import com.activeviam.mac.entities.CubeOwner;
import com.activeviam.mac.entities.StoreOwner;
import com.activeviam.mac.statistic.memory.visitor.impl.EpochVisitor;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.impl.DefaultMemoryStatistic;
import com.qfs.pivot.impl.MultiVersionDistributedActivePivot;
import com.qfs.service.monitoring.IMemoryAnalysisService;
import com.qfs.store.IDatastore;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.qfs.store.transaction.ITransactionManager;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.contributions.impl.ClasspathContributionProvider;
import com.quartetfs.fwk.impl.Pair;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import org.assertj.core.api.SoftAssertions;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestEpochVisitor extends ATestMemoryStatistic {

  private Pair<IDatastore, IActivePivotManager> monitoredApp;
  private Pair<IDatastore, IActivePivotManager> distributedMonitoredApp;
  private IMemoryStatistic appStatistics;
  private IMemoryStatistic distributedAppStatistics;

  @BeforeClass
  public static void setupRegistry() {
    Registry.setContributionProvider(new ClasspathContributionProvider());
  }

  @Before
  public void setup() throws DatastoreTransactionException {
    initializeApplication();

    Path exportPath =
        generateMemoryStatistics(
            monitoredApp.getLeft(),
            monitoredApp.getRight(),
            IMemoryAnalysisService::exportApplication);
    appStatistics = loadMemoryStatFromFolder(exportPath);

    initializeMonitoredApplication();

    exportPath =
        generateMemoryStatistics(
            distributedMonitoredApp.getLeft(),
            distributedMonitoredApp.getRight(),
            IMemoryAnalysisService::exportMostRecentVersion);
    distributedAppStatistics = loadMemoryStatFromFolder(exportPath);
  }

  @Test
  public void testSimpleEpochCollection() {
    final EpochVisitor epochVisitor = new EpochVisitor();
    epochVisitor.visit((DefaultMemoryStatistic) appStatistics);

    final ChunkOwner storeA = new StoreOwner("A");
    final ChunkOwner storeB = new StoreOwner("B");
    final ChunkOwner cube = new CubeOwner("Cube");

    SoftAssertions.assertSoftly(
        assertions -> {
          assertions
              .assertThat(epochVisitor.getDatastoreEpochs().toArray())
              .as("datastore epochs")
              .containsExactlyInAnyOrder(0L, 1L, 2L, 3L, 4L);

          assertions
              .assertThat(epochVisitor.getDistributedEpochsPerOwner())
              .as("distributed epochs")
              .isEmpty();

          assertions
              .assertThat(epochVisitor.getRegularEpochsPerOwner())
              .as("non-distributed epochs")
              .containsExactlyInAnyOrderEntriesOf(
                  Map.of(
                      storeA, new TreeSet<>(Set.of(1L, 3L)),
                      storeB, new TreeSet<>(Set.of(0L, 2L, 4L)),
                      cube, new TreeSet<>(Set.of(1L, 3L))));
        });
  }

  @Test
  public void testEpochCollectionWithDistributedCube() {
    final EpochVisitor epochVisitor = new EpochVisitor();
    epochVisitor.visit((DefaultMemoryStatistic) distributedAppStatistics);

    final ChunkOwner storeA = new StoreOwner("A");
    final ChunkOwner dataCube = new CubeOwner("Data");
    final ChunkOwner queryCubeA = new CubeOwner("QueryCubeA");
    final ChunkOwner queryCubeB = new CubeOwner("QueryCubeB");

    SoftAssertions.assertSoftly(
        assertions -> {
          assertions
              .assertThat(epochVisitor.getDatastoreEpochs().toArray())
              .as("datastore epochs")
              .containsExactlyInAnyOrder(1L);

          assertions
              .assertThat(epochVisitor.getDistributedEpochsPerOwner())
              .as("distributed epochs")
              .containsExactlyInAnyOrderEntriesOf(
                  Map.of(
                      queryCubeA, Collections.singleton(5L),
                      queryCubeB, Collections.singleton(1L)));

          assertions
              .assertThat(epochVisitor.getRegularEpochsPerOwner())
              .as("non-distributed epochs")
              .containsExactlyInAnyOrderEntriesOf(
                  Map.of(
                      storeA, new TreeSet<>(Collections.singleton(1L)),
                      dataCube, new TreeSet<>(Collections.singleton(1L))));
        });
  }

  private void initializeApplication() throws DatastoreTransactionException {
    monitoredApp = createMicroApplicationWithIsolatedStoreAndKeepAllEpochPolicy();
    final ITransactionManager transactionManager = monitoredApp.getLeft().getTransactionManager();
    fillApplication(transactionManager);
  }

  private void initializeMonitoredApplication() {
    distributedMonitoredApp = createDistributedApplicationWithKeepAllEpochPolicy();
    fillMonitoredApplication();
  }

  private void fillApplication(final ITransactionManager transactionManager)
      throws DatastoreTransactionException {
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

  private void fillMonitoredApplication() {
    // epoch 1
    distributedMonitoredApp
        .getLeft()
        .edit(
            transactionManager -> {
              IntStream.range(0, 10).forEach(i -> transactionManager.add("A", i, 0.));
            });

    // emulate commits on the query cubes at a greater epoch that does not exist in the datastore
    MultiVersionDistributedActivePivot queryCubeA =
        ((MultiVersionDistributedActivePivot)
            distributedMonitoredApp.getRight().getActivePivots().get("QueryCubeA"));

    // produces distributed epochs 1 to 5
    for (int i = 0; i < 5; ++i) {
      queryCubeA.removeMembersFromCube(Collections.emptySet(), 0, false);
    }

    MultiVersionDistributedActivePivot queryCubeB =
        ((MultiVersionDistributedActivePivot)
            distributedMonitoredApp.getRight().getActivePivots().get("QueryCubeB"));

    // produces distributed epoch 1
    queryCubeB.removeMembersFromCube(Collections.emptySet(), 0, false);
  }

  private Path generateMemoryStatistics(
      final IDatastore datastore,
      final IActivePivotManager manager,
      final BiFunction<IMemoryAnalysisService, String, Path> exportMethod) {
    datastore.getEpochManager().forceDiscardEpochs(node -> true);
    performGC();

    final IMemoryAnalysisService analysisService = createService(datastore, manager);
    return exportMethod.apply(analysisService, "testEpochs");
  }
}
