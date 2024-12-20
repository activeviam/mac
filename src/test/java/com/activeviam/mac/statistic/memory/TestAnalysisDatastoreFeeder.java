/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.activeviam.activepivot.core.impl.internal.utils.ApplicationInTests;
import com.activeviam.activepivot.core.intf.api.cube.IActivePivotManager;
import com.activeviam.activepivot.server.intf.api.observability.IMemoryAnalysisService;
import com.activeviam.activepivot.server.spring.api.config.IDatastoreSchemaDescriptionConfig;
import com.activeviam.database.api.IDatabaseVersion;
import com.activeviam.database.api.conditions.BaseConditions;
import com.activeviam.database.api.query.AliasedField;
import com.activeviam.database.api.query.ListQuery;
import com.activeviam.database.api.schema.FieldPath;
import com.activeviam.database.datastore.api.IDatastore;
import com.activeviam.database.datastore.api.transaction.DatastoreTransactionException;
import com.activeviam.database.datastore.api.transaction.ITransactionManager;
import com.activeviam.database.datastore.internal.IInternalDatastore;
import com.activeviam.database.datastore.internal.query.impl.DatastoreQueryHelper;
import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.cfg.impl.RegistryInitializationConfig;
import com.activeviam.mac.entities.ChunkOwner;
import com.activeviam.mac.entities.CubeOwner;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig;
import com.activeviam.mac.statistic.memory.visitor.impl.EpochView;
import com.activeviam.tech.core.api.agent.AgentException;
import com.activeviam.tech.mvcc.api.IEpochHistory;
import com.activeviam.tech.observability.internal.memory.AMemoryStatistic;
import com.activeviam.tech.records.api.ICursor;
import com.activeviam.tech.records.api.IRecordReader;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import java.nio.file.Path;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestAnalysisDatastoreFeeder extends ATestMemoryStatistic {

  private ApplicationInTests<IDatastore> monitoredApp;
  private ApplicationInTests<IInternalDatastore> monitoringApp;
  private ApplicationInTests<IDatastore> distributedMonitoredApp;
  private AMemoryStatistic appStatistics;
  private AMemoryStatistic distributedAppHeadStatistics;

  @BeforeAll
  public static void setupRegistry() {
    RegistryInitializationConfig.setupRegistry();
  }

  @BeforeEach
  public void setup() throws DatastoreTransactionException, AgentException {
    initializeApplication();
    initializeMonitoredClusterApplication();

    this.appStatistics =
        loadMemoryStatFromFolder(
            generateMemoryStatistics(
                this.monitoredApp.getDatabase(),
                this.monitoredApp.getManager(),
                IMemoryAnalysisService::exportApplication));

    this.distributedAppHeadStatistics =
        loadMemoryStatFromFolder(
            generateMemoryStatistics(
                this.distributedMonitoredApp.getDatabase(),
                this.distributedMonitoredApp.getManager(),
                IMemoryAnalysisService::exportMostRecentVersion));

    initializeMonitoringApplication();
  }

  /** Ensures the same statistics can be loaded in different dump names. */
  @Test
  public void testDifferentDumps() {
    final IInternalDatastore monitoringDatastore = this.monitoringApp.getDatabase();

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

  /**
   * Ensures that, when adding a complete application (with multiple epochs) to an already existing
   * loaded dataset on the same dumpname, the dataset is replicated for each of the application's
   * epochs
   */
  @Test
  public void testEpochReplicationForAlreadyExistingChunks() {
    final IInternalDatastore monitoringDatastore = this.monitoringApp.getDatabase();

    // load a cluster's single epoch statistics
    ATestMemoryStatistic.feedMonitoringApplication(
        monitoringDatastore, List.of(this.distributedAppHeadStatistics), "app");

    TLongSet epochs =
        collectEpochViewsForOwner(
            monitoringDatastore.getMostRecentVersion(), new CubeOwner("Data"));

    Assertions.assertThat(epochs.toArray())
        .containsExactlyInAnyOrder(
            distributedMonitoredApp
                .getManager()
                .getActivePivot("Data")
                .getMostRecentVersion()
                .getEpochId());

    // Load the stats of a complete App into the same dumpName
    ATestMemoryStatistic.feedMonitoringApplication(
        monitoringDatastore, List.of(this.appStatistics), "app");
    // Start the cube
    this.monitoringApp.start();
    // Await for pivot notification to make sure everything is stable and committed before testing
    // things
    this.monitoringApp.getSingleCube().awaitNotifications();

    final long newestNonDistEpoch =
        this.monitoredApp.getDatabase().getEpochManager().getHistories().values().stream()
            .mapToLong(IEpochHistory::getCurrentEpoch)
            .max()
            .orElseThrow();

    final long newestDistEpoch =
        distributedMonitoredApp
            .getManager()
            .getActivePivot("Data")
            .getMostRecentVersion()
            .getEpochId();
    // within its statistic, the Data cube only has one epoch (usually 1)
    // upon the second feedDatastore call, its chunks should be mapped to the new incoming datastore
    // epochs from the oldest existing on the Data cube (NOT the app)  to  the most recent on the
    // app
    long[] expectedReplicatedEpochs =
        LongStream.range(newestDistEpoch, newestNonDistEpoch + 1).toArray();
    if (expectedReplicatedEpochs.length == 0) {
      expectedReplicatedEpochs = new long[] {newestDistEpoch};
    }

    epochs =
        collectEpochViewsForOwner(
            monitoringDatastore.getMostRecentVersion(), new CubeOwner("Data"));

    Assertions.assertThat(epochs.toArray()).containsExactlyInAnyOrder(expectedReplicatedEpochs);
  }

  @Test
  public void testEpochReplicationForAlreadyExistingEpochs() {
    final IInternalDatastore monitoringDatastore = this.monitoringApp.getDatabase();

    ATestMemoryStatistic.feedMonitoringApplication(
        monitoringDatastore, List.of(this.appStatistics), "app");
    ATestMemoryStatistic.feedMonitoringApplication(
        monitoringDatastore, List.of(this.distributedAppHeadStatistics), "app");

    TLongSet epochs =
        epochs =
            collectEpochViewsForOwner(
                monitoringDatastore.getMostRecentVersion(), new CubeOwner("Data"));

    final long newestNonDistEpoch =
        this.monitoredApp.getDatabase().getEpochManager().getHistories().values().stream()
            .mapToLong(IEpochHistory::getCurrentEpoch)
            .max()
            .orElseThrow();

    final long newestDistEpoch =
        distributedMonitoredApp
            .getManager()
            .getActivePivot("Data")
            .getMostRecentVersion()
            .getEpochId();
    // within its statistic, the Data cube only has one epoch (usually 1)
    // upon the second feedDatastore call, its chunks should be mapped to the new incoming datastore
    // epochs from the oldest existing on the Data cube (NOT the app)  to  the most recent on the
    // app
    long[] expectedReplicatedEpochs =
        LongStream.range(newestDistEpoch, newestNonDistEpoch + 1).toArray();
    if (expectedReplicatedEpochs.length == 0) {
      expectedReplicatedEpochs = new long[] {newestDistEpoch};
    }

    Assertions.assertThat(epochs.toArray()).containsExactlyInAnyOrder(expectedReplicatedEpochs);
  }

  private TLongSet collectEpochViewsForOwner(
      final IDatabaseVersion monitoringDatastore, final ChunkOwner owner) {
    final ListQuery query =
        monitoringDatastore
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.EPOCH_VIEW_STORE)
            .withCondition(
                BaseConditions.equal(FieldPath.of(DatastoreConstants.EPOCH_VIEW__OWNER), owner))
            .withAliasedFields(
                AliasedField.fromFieldName(DatastoreConstants.EPOCH_VIEW__VIEW_EPOCH_ID))
            .toQuery();
    final ICursor queryResult = monitoringDatastore.getQueryRunner().listQuery(query).run();

    final TLongSet epochs = new TLongHashSet();
    for (final IRecordReader recordReader : queryResult) {
      epochs.add(((EpochView) recordReader.read(0)).getEpochId());
    }

    return epochs;
  }

  private void initializeApplication() throws DatastoreTransactionException {
    this.monitoredApp = createMicroApplicationWithIsolatedStoreAndKeepAllEpochPolicy();
    fillMicroApplication();
  }

  private void fillMicroApplication() throws DatastoreTransactionException {
    final ITransactionManager transactionManager =
        this.monitoredApp.getDatabase().getTransactionManager();
    // epoch 1 -> store A + cube
    transactionManager.startTransaction("A");
    IntStream.range(0, 10).forEach(i -> transactionManager.add("A", i, 0.));
    transactionManager.commitTransaction();
    this.monitoredApp.getSingleCube().awaitNotifications();

    // epoch 2 -> store B
    transactionManager.startTransaction("B");
    IntStream.range(0, 10).forEach(i -> transactionManager.add("B", i, 0.));
    transactionManager.commitTransaction();
    this.monitoredApp.getSingleCube().awaitNotifications();

    // epoch 3 -> store A + cube
    transactionManager.startTransaction("A");
    IntStream.range(10, 20).forEach(i -> transactionManager.add("A", i, 1.));
    transactionManager.commitTransaction();
    this.monitoredApp.getSingleCube().awaitNotifications();

    // epoch 4 -> store B
    transactionManager.startTransaction("B");
    IntStream.range(10, 20).forEach(i -> transactionManager.add("B", i, 1.));
    transactionManager.commitTransaction();
    this.monitoredApp.getSingleCube().awaitNotifications();
  }

  private void initializeMonitoredClusterApplication() {
    this.distributedMonitoredApp =
        createDistributedApplicationWithKeepAllEpochPolicy("analysis-feeder");
    fillDistributedApplication();
  }

  private void fillDistributedApplication() {
    // epoch 1
    this.distributedMonitoredApp
        .getDatabase()
        .edit(
            transactionManager ->
                IntStream.range(0, 10).forEach(i -> transactionManager.add("A", i, 0.)));

    this.distributedMonitoredApp.getManager().getActivePivot("QueryCubeA").awaitNotifications();
    this.distributedMonitoredApp.getManager().getActivePivot("QueryCubeB").awaitNotifications();
    this.distributedMonitoredApp.getManager().getActivePivot("Data").awaitNotifications();
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

  private void initializeMonitoringApplication() {
    final ManagerDescriptionConfig config = new ManagerDescriptionConfig();
    final IDatastoreSchemaDescriptionConfig schemaConfig =
        new MemoryAnalysisDatastoreDescriptionConfig();

    final ApplicationInTests<IInternalDatastore> application =
        ApplicationInTests.builder()
            .withManager(config.managerDescription())
            .withDatastore(schemaConfig.datastoreSchemaDescription())
            .build();

    this.monitoringApp = application;
  }
}
