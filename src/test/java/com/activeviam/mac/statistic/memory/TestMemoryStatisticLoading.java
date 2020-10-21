/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.statistic.memory;

import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription.ParentType;
import com.activeviam.mac.statistic.memory.descriptions.FullApplicationDescription;
import com.activeviam.mac.statistic.memory.descriptions.FullApplicationDescriptionWithBranches;
import com.activeviam.mac.statistic.memory.descriptions.FullApplicationDescriptionWithDuplicateVectors;
import com.activeviam.mac.statistic.memory.descriptions.FullApplicationDescriptionWithVectors;
import com.activeviam.mac.statistic.memory.descriptions.ITestApplicationDescription;
import com.activeviam.mac.statistic.memory.junit.RegistrySetupExtension;
import com.activeviam.properties.impl.ActiveViamProperty;
import com.activeviam.properties.impl.ActiveViamPropertyExtension;
import com.activeviam.properties.impl.ActiveViamPropertyExtension.ActiveViamPropertyExtensionBuilder;
import com.qfs.condition.impl.BaseConditions;
import com.qfs.junit.LocalResourcesExtension;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;
import com.qfs.monitoring.statistic.memory.visitor.impl.AMemoryStatisticWithPredicate;
import com.qfs.store.IDatastore;
import com.qfs.store.query.IDictionaryCursor;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.qfs.util.impl.QfsFileTestUtils;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.util.impl.TruePredicate;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(RegistrySetupExtension.class)
public class TestMemoryStatisticLoading {

  @RegisterExtension
  protected static ActiveViamPropertyExtension propertyExtension =
      new ActiveViamPropertyExtensionBuilder()
          .withProperty(ActiveViamProperty.ACTIVEVIAM_TEST_PROPERTY, true)
          .build();

  @RegisterExtension
  protected final LocalResourcesExtension resources = new LocalResourcesExtension();

  protected static Path tempDir = QfsFileTestUtils.createTempDirectory(TestMACMeasures.class);

  /**
   * Assert the number of offheap chunks by filling the datastore used for monitoring AND doing a
   * query on it for counting. Comparing the value from counting from {@link IMemoryStatistic}.
   */
  @Test
  public void testLoadDatastoreStats() throws AgentException, DatastoreTransactionException {
    final Application monitoredApplication = MonitoringTestUtils
        .setupApplication(new FullApplicationDescription(), resources);

    final Path exportPath = MonitoringTestUtils.exportMostRecentVersion(
        monitoredApplication.getDatastore(),
        monitoredApplication.getManager(),
        tempDir,
        "testLoadDatastoreStats");

    final IMemoryStatistic storeStat =
        MonitoringTestUtils.loadDatastoreMemoryStatFromFolder(exportPath);

    Assertions.assertThat(storeStat.getChildren()).isNotEmpty();
    MonitoringTestUtils.assertLoadsCorrectly(storeStat, resources);
  }

  @Test
  public void testLoadPivotStats() throws AgentException, DatastoreTransactionException {
    final Application monitoredApplication = MonitoringTestUtils
        .setupApplication(new FullApplicationDescription(), resources);

    final Path exportPath = MonitoringTestUtils.exportMostRecentVersion(
        monitoredApplication.getDatastore(),
        monitoredApplication.getManager(),
        tempDir,
        "testLoadPivotStats");

    final IMemoryStatistic storeStat =
        MonitoringTestUtils.loadPivotMemoryStatFromFolder(exportPath);

    Assertions.assertThat(storeStat.getChildren()).isNotEmpty();
    MonitoringTestUtils.assertLoadsCorrectly(storeStat, resources);
  }

  @Test
  public void testLoadFullStats() throws AgentException, DatastoreTransactionException {
    final Application monitoredApplication = MonitoringTestUtils
        .setupApplication(new FullApplicationDescription(), resources);

    final Path exportPath = MonitoringTestUtils.exportMostRecentVersion(
        monitoredApplication.getDatastore(),
        monitoredApplication.getManager(),
        tempDir,
        "testLoadFullStats");

    final IMemoryStatistic storeStat =
        MonitoringTestUtils.loadMemoryStatFromFolder(exportPath);

    Assertions.assertThat(storeStat.getChildren()).isNotEmpty();
    MonitoringTestUtils.assertLoadsCorrectly(storeStat, resources);
  }

  @Test
  public void testLoadFullStatsWithBranches() throws AgentException, DatastoreTransactionException {
    final Set<String> branches = new HashSet<>();
    branches.add("branch1");
    branches.add("branch2");

    final Application monitoredApplication = MonitoringTestUtils.setupApplication(
        new FullApplicationDescriptionWithBranches(branches),
        resources);

    final Path exportPath = MonitoringTestUtils.exportBranches(
        monitoredApplication.getDatastore(),
        monitoredApplication.getManager(),
        tempDir,
        "testLoadFullStatsWithBranches",
        branches);

    final IMemoryStatistic storeStat =
        MonitoringTestUtils.loadMemoryStatFromFolder(exportPath);

    Assertions.assertThat(storeStat.getChildren()).isNotEmpty();
    MonitoringTestUtils.assertLoadsCorrectly(storeStat, resources);
  }

  @Test
  public void testLoadFullStatsWithEpochs() throws AgentException, DatastoreTransactionException {
    final Set<String> branches = new HashSet<>();
    branches.add("branch1");
    branches.add("branch2");
    final long[] epochs = new long[] {1L, 2L};

    final Application monitoredApplication = MonitoringTestUtils.setupApplication(
        new FullApplicationDescriptionWithBranches(branches),
        resources);

    final Path exportPath = MonitoringTestUtils.exportVersions(
        monitoredApplication.getDatastore(),
        monitoredApplication.getManager(),
        tempDir,
        "testLoadFullStatsWithEpochs",
        epochs);

    final IMemoryStatistic storeStat =
        MonitoringTestUtils.loadMemoryStatFromFolder(exportPath);

    Assertions.assertThat(storeStat.getChildren()).isNotEmpty();
    MonitoringTestUtils.assertLoadsCorrectly(storeStat, resources);
  }

  @Test
  public void testLoadMonitoringDatastoreWithVectorsWODuplicate() throws Exception {
    doTestLoadMonitoringDatastoreWithVectors(new FullApplicationDescriptionWithVectors());
  }

  @Test
  public void testLoadMonitoringDatastoreWithDuplicate() throws Exception {
    doTestLoadMonitoringDatastoreWithVectors(new FullApplicationDescriptionWithDuplicateVectors());
  }

  public void doTestLoadMonitoringDatastoreWithVectors(
      ITestApplicationDescription applicationDescription) throws Exception {

    final Application monitoredApplication =
        MonitoringTestUtils.setupApplication(applicationDescription, resources);

    final Path exportPath = MonitoringTestUtils.exportMostRecentVersion(
        monitoredApplication.getDatastore(),
        monitoredApplication.getManager(), tempDir, "testLoadMonitoringDatastoreWithVectors");

    final IMemoryStatistic storeStat = MonitoringTestUtils.loadMemoryStatFromFolder(exportPath);

    Assertions.assertThat(storeStat.getChildren()).isNotEmpty();
    final IDatastore monitoringDatastore =
        MonitoringTestUtils.assertLoadsCorrectly(storeStat, resources);

    final Set<Long> storeIds = retrieveChunksOfType(monitoringDatastore, ParentType.VECTOR_BLOCK);
    Assertions.assertThat(storeIds).isNotEmpty();

    final Set<Long> statIds = new HashSet<>();
    storeStat.getChildren().forEach(stat ->
        stat.accept(
            new AMemoryStatisticWithPredicate<Void>(TruePredicate.get()) {
              @Override
              protected Void getResult() {
                return null;
              }

              @Override
              protected boolean match(final IMemoryStatistic statistic) {
                if (statistic
                    .getName()
                    .equals(MemoryStatisticConstants.STAT_NAME_VECTOR_BLOCK)) {
                  statIds.add(
                      statistic
                          .getAttribute(MemoryStatisticConstants.ATTR_NAME_CHUNK_ID)
                          .asLong());
                }
                return true; // Iterate over every item
              }
            }));
    Assertions.assertThat(storeIds).isEqualTo(statIds);
  }

  private Set<Long> retrieveChunksOfType(
      final IDatastore datastore, final ParentType component) {
    final IDictionaryCursor cursor =
        datastore
            .getHead()
            .getQueryRunner()
            .forStore(DatastoreConstants.OWNER_STORE)
            .withCondition(
                BaseConditions.Equal(DatastoreConstants.OWNER__COMPONENT, component))
            .selecting(DatastoreConstants.CHUNK_ID)
            .onCurrentThread()
            .run();

    return StreamSupport.stream(cursor.spliterator(), false)
        .map(reader -> reader.readLong(0))
        .collect(Collectors.toSet());
  }
}
