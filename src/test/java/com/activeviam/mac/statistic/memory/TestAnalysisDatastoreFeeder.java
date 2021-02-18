/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.activeviam.mac.entities.ChunkOwner;
import com.activeviam.mac.entities.CubeOwner;
import com.activeviam.mac.memory.AnalysisDatastoreFeeder;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.statistic.memory.descriptions.DistributedApplicationDescription;
import com.activeviam.mac.statistic.memory.descriptions.MicroApplicationDescriptionWithIsolatedStoreAndKeepAllPolicy;
import com.activeviam.mac.statistic.memory.junit.RegistrySetupExtension;
import com.activeviam.mac.statistic.memory.visitor.impl.EpochView;
import com.activeviam.properties.impl.ActiveViamProperty;
import com.activeviam.properties.impl.ActiveViamPropertyExtension;
import com.activeviam.properties.impl.ActiveViamPropertyExtension.ActiveViamPropertyExtensionBuilder;
import com.qfs.condition.impl.BaseConditions;
import com.qfs.junit.LocalResourcesExtension;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.pivot.impl.MultiVersionDistributedActivePivot;
import com.qfs.store.IDatastore;
import com.qfs.store.IDatastoreVersion;
import com.qfs.store.query.ICursor;
import com.qfs.store.query.impl.DatastoreQueryHelper;
import com.qfs.store.record.IRecordReader;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.qfs.store.transaction.ITransactionManager;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.fwk.AgentException;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import java.nio.file.Path;
import java.util.Collections;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(RegistrySetupExtension.class)
public class TestAnalysisDatastoreFeeder {

  @RegisterExtension
  protected static ActiveViamPropertyExtension propertyExtension =
      new ActiveViamPropertyExtensionBuilder()
          .withProperty(ActiveViamProperty.ACTIVEVIAM_TEST_PROPERTY, true)
          .build();

  @RegisterExtension
  protected final LocalResourcesExtension resources = new LocalResourcesExtension();

  @TempDir
  protected static Path tempDir;

  protected Application monitoredApplication;
  protected Application distributedMonitoredApplication;
  protected Application monitoringApplication;

  protected IMemoryStatistic appStatistics;
  protected IMemoryStatistic distributedAppStatistics;

  @BeforeEach
  public void setup(TestInfo testInfo) throws AgentException {
    monitoredApplication = MonitoringTestUtils.setupApplication(
        new MicroApplicationDescriptionWithIsolatedStoreAndKeepAllPolicy(),
        resources,
        TestAnalysisDatastoreFeeder::fillMicroApplication);
    Path exportPath = MonitoringTestUtils.exportApplication(
        monitoredApplication.getDatastore(),
        monitoredApplication.getManager(),
        tempDir,
        this.getClass().getSimpleName());
    appStatistics = MonitoringTestUtils.loadMemoryStatFromFolder(exportPath);

    distributedMonitoredApplication = MonitoringTestUtils.setupApplication(
        new DistributedApplicationDescription(testInfo.getDisplayName()),
        resources,
        TestAnalysisDatastoreFeeder::fillDistributedApplication);
    exportPath = MonitoringTestUtils.exportMostRecentVersion(
        distributedMonitoredApplication.getDatastore(),
        distributedMonitoredApplication.getManager(),
        tempDir,
        this.getClass().getSimpleName());
    distributedAppStatistics = MonitoringTestUtils.loadMemoryStatFromFolder(exportPath);

    monitoringApplication = MonitoringTestUtils.setupMonitoringApplication(resources);
  }

  private static void fillDistributedApplication(
      IDatastore datastore, IActivePivotManager manager) {
    // epoch 1
    datastore.edit(transactionManager -> IntStream.range(0, 10)
        .forEach(i -> transactionManager.add("A", i, (double) i)));

    // emulate commits on the query cubes at a greater epoch that does not exist in the datastore
    MultiVersionDistributedActivePivot queryCubeA =
        ((MultiVersionDistributedActivePivot) manager.getActivePivots().get("QueryCubeA"));

    // produces distributed epochs 1 to 5
    for (int i = 0; i < 5; ++i) {
      queryCubeA.removeMembersFromCube(Collections.emptySet(), 0, false);
    }

    MultiVersionDistributedActivePivot queryCubeB =
        ((MultiVersionDistributedActivePivot) manager.getActivePivots().get("QueryCubeB"));

    // produces distributed epoch 1
    queryCubeB.removeMembersFromCube(Collections.emptySet(), 0, false);
  }

  @Test
  public void testDifferentDumps() {
    final IDatastore monitoringDatastore = monitoringApplication.getDatastore();

    AnalysisDatastoreFeeder feeder = new AnalysisDatastoreFeeder("app");
    feeder.loadInto(monitoringDatastore, Stream.of(appStatistics));

    feeder = new AnalysisDatastoreFeeder("app2");
    feeder.loadInto(monitoringDatastore, Stream.of(appStatistics));

    Assertions.assertThat(
        DatastoreQueryHelper.selectDistinct(
            monitoringDatastore.getMostRecentVersion(),
            DatastoreConstants.APPLICATION_STORE,
            DatastoreConstants.APPLICATION__DUMP_NAME))
        .containsExactlyInAnyOrder("app", "app2");
  }

  @Test
  public void testEpochReplicationForAlreadyExistingChunks() {
    final IDatastore monitoringDatastore = monitoringApplication.getDatastore();

    AnalysisDatastoreFeeder feeder = new AnalysisDatastoreFeeder("app");
    feeder.loadInto(monitoringDatastore, Stream.of(distributedAppStatistics));

    TLongSet epochs = collectEpochViewsForOwner(
        monitoringDatastore.getMostRecentVersion(),
        new CubeOwner("Data"));

    Assertions.assertThat(epochs.toArray()).containsExactlyInAnyOrder(1L);

    feeder = new AnalysisDatastoreFeeder("app");
    feeder.loadInto(monitoringDatastore, Stream.of(appStatistics));

    epochs = collectEpochViewsForOwner(
        monitoringDatastore.getMostRecentVersion(),
        new CubeOwner("Data"));

    // within its statistic, the Data cube only has epoch 1
    // upon the second feedDatastore call, its chunks should be mapped to the new incoming datastore
    // epochs
    Assertions.assertThat(epochs.toArray()).containsExactlyInAnyOrder(1L, 2L, 3L, 4L);
  }

  @Test
  public void testEpochReplicationForAlreadyExistingEpochs() {
    final IDatastore monitoringDatastore = monitoringApplication.getDatastore();

    AnalysisDatastoreFeeder feeder = new AnalysisDatastoreFeeder("app");
    feeder.loadInto(monitoringDatastore, Stream.of(appStatistics));
    feeder = new AnalysisDatastoreFeeder("app");
    feeder.loadInto(monitoringDatastore, Stream.of(distributedAppStatistics));

    TLongSet epochs = collectEpochViewsForOwner(
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

  private static void fillMicroApplication(IDatastore datastore, IActivePivotManager manager)
      throws DatastoreTransactionException {
    final ITransactionManager transactionManager = datastore.getTransactionManager();

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
}
