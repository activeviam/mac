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
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.statistic.memory.visitor.impl.EpochView;
import com.activeviam.pivot.builders.StartBuilding;
import com.qfs.condition.impl.BaseConditions;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.pivot.impl.MultiVersionDistributedActivePivot;
import com.qfs.service.monitoring.IMemoryAnalysisService;
import com.qfs.store.IDatastore;
import com.qfs.store.IDatastoreVersion;
import com.qfs.store.query.ICursor;
import com.qfs.store.query.impl.DatastoreQueryHelper;
import com.qfs.store.record.IRecordReader;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.qfs.store.transaction.ITransactionManager;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.contributions.impl.ClasspathContributionProvider;
import com.quartetfs.fwk.impl.Pair;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestAnalysisDatastoreFeeder extends ATestMemoryStatistic {

  private Pair<IDatastore, IActivePivotManager> monitoredApp;
  private Pair<IDatastore, IActivePivotManager> monitoringApp;
  private Pair<IDatastore, IActivePivotManager> distributedMonitoredApp;
  private IMemoryStatistic appStatistics;
  private IMemoryStatistic distributedAppStatistics;

  @BeforeAll
  public static void setupRegistry() {
    Registry.setContributionProvider(new ClasspathContributionProvider());
  }

  @BeforeEach
  public void setup() throws DatastoreTransactionException, AgentException {
    initializeApplication();

    Path exportPath =
        generateMemoryStatistics(
            this.monitoredApp.getLeft(),
            this.monitoredApp.getRight(),
            IMemoryAnalysisService::exportApplication);
    this.appStatistics = loadMemoryStatFromFolder(exportPath);

    initializeMonitoredApplication();

    exportPath =
        generateMemoryStatistics(
            this.distributedMonitoredApp.getLeft(),
            this.distributedMonitoredApp.getRight(),
            IMemoryAnalysisService::exportMostRecentVersion);
    this.distributedAppStatistics = loadMemoryStatFromFolder(exportPath);

    initializeMonitoringApplication();
  }

  @Test
  public void testDifferentDumps() {
    final IDatastore monitoringDatastore = this.monitoringApp.getLeft();

    ATestMemoryStatistic.feedMonitoringApplication(
        monitoringDatastore, List.of(this.appStatistics), "app");
    ATestMemoryStatistic.feedMonitoringApplication(
        monitoringDatastore, List.of(this.appStatistics), "app2");

    Assertions.assertThat(
            DatastoreQueryHelper.selectDistinct(
                monitoringDatastore.getMostRecentVersion(),
                DatastoreConstants.APPLICATION_STORE,
                DatastoreConstants.APPLICATION__DUMP_NAME))
        .containsExactlyInAnyOrder("app", "app2");
  }

  @Test
  public void testEpochReplicationForAlreadyExistingChunks() {
    final IDatastore monitoringDatastore = this.monitoringApp.getLeft();

    ATestMemoryStatistic.feedMonitoringApplication(
        monitoringDatastore, List.of(this.distributedAppStatistics), "app");

    TLongSet epochs =
        collectEpochViewsForOwner(
            monitoringDatastore.getMostRecentVersion(), new CubeOwner("Data"));

    Assertions.assertThat(epochs.toArray()).containsExactlyInAnyOrder(1L);

    ATestMemoryStatistic.feedMonitoringApplication(
        monitoringDatastore, List.of(this.appStatistics), "app");

    epochs =
        collectEpochViewsForOwner(
            monitoringDatastore.getMostRecentVersion(), new CubeOwner("Data"));

    // within its statistic, the Data cube only has epoch 1
    // upon the second feedDatastore call, its chunks should be mapped to the new incoming datastore
    // epochs
    Assertions.assertThat(epochs.toArray()).containsExactlyInAnyOrder(1L, 2L, 3L, 4L);
  }

  @Test
  public void testEpochReplicationForAlreadyExistingEpochs() {
    final IDatastore monitoringDatastore = this.monitoringApp.getLeft();

    ATestMemoryStatistic.feedMonitoringApplication(
        monitoringDatastore, List.of(this.appStatistics), "app");
    ATestMemoryStatistic.feedMonitoringApplication(
        monitoringDatastore, List.of(this.distributedAppStatistics), "app");

    TLongSet epochs =
        collectEpochViewsForOwner(
            monitoringDatastore.getMostRecentVersion(), new CubeOwner("Data"));

    // within its statistic, the Data cube only has epoch 1
    // it should be mapped to the epochs 1, 2, 3 and 4 of the first feedDatastore call
    Assertions.assertThat(epochs.toArray()).containsExactlyInAnyOrder(1L, 2L, 3L, 4L);
  }

  private TLongSet collectEpochViewsForOwner(
      final IDatastoreVersion monitoringDatastore, final ChunkOwner owner) {
    final ICursor queryResult =
        monitoringDatastore
            .getQueryRunner()
            .forStore(DatastoreConstants.EPOCH_VIEW_STORE)
            .withCondition(BaseConditions.Equal(DatastoreConstants.EPOCH_VIEW__OWNER, owner))
            .selecting(DatastoreConstants.EPOCH_VIEW__VIEW_EPOCH_ID)
            .onCurrentThread()
            .run();

    final TLongSet epochs = new TLongHashSet();
    for (final IRecordReader recordReader : queryResult) {
      epochs.add(((EpochView) recordReader.read(0)).getEpochId());
    }

    return epochs;
  }

  private void initializeApplication() throws DatastoreTransactionException {
    this.monitoredApp = createMicroApplicationWithIsolatedStoreAndKeepAllEpochPolicy();

    resources.register(this.monitoredApp.getRight()::stop);
    resources.register(this.monitoredApp.getLeft()::stop);

    final ITransactionManager transactionManager =
        this.monitoredApp.getLeft().getTransactionManager();
    fillMicroApplication(transactionManager);
  }

  private void fillMicroApplication(final ITransactionManager transactionManager)
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

  private void initializeMonitoredApplication() {
    this.distributedMonitoredApp =
        createDistributedApplicationWithKeepAllEpochPolicy("analysis-feeder");
    resources.register(this.distributedMonitoredApp.getLeft()::stop);
    resources.register(this.distributedMonitoredApp.getRight()::stop);
    fillDistributedApplication();
  }

  private void fillDistributedApplication() {
    // epoch 1
    this.distributedMonitoredApp
        .getLeft()
        .edit(
            transactionManager ->
                IntStream.range(0, 10).forEach(i -> transactionManager.add("A", i, 0.)));

    // emulate commits on the query cubes at a greater epoch that does not exist in the datastore
    MultiVersionDistributedActivePivot queryCubeA =
        ((MultiVersionDistributedActivePivot)
            this.distributedMonitoredApp.getRight().getActivePivots().get("QueryCubeA"));

    // produces distributed epochs 1 to 5
    for (int i = 0; i < 5; ++i) {
      queryCubeA.removeMembersFromCube(Collections.emptySet(), 0, false);
    }

    MultiVersionDistributedActivePivot queryCubeB =
        ((MultiVersionDistributedActivePivot)
            this.distributedMonitoredApp.getRight().getActivePivots().get("QueryCubeB"));

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

  private void initializeMonitoringApplication() throws AgentException {
    final ManagerDescriptionConfig config = new ManagerDescriptionConfig();
    final IDatastore monitoringDatastore =
        resources.create(
            StartBuilding.datastore().setSchemaDescription(config.schemaDescription())::build);

    final IActivePivotManager manager =
        StartBuilding.manager()
            .setDescription(config.managerDescription())
            .setDatastoreAndPermissions(monitoringDatastore)
            .buildAndStart();
    resources.register(manager::stop);

    this.monitoringApp = new Pair<>(monitoringDatastore, manager);
  }
}
