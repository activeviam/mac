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
import com.activeviam.mac.statistic.memory.descriptions.FullApplicationDescriptionWithVectors;
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
import com.qfs.store.NoTransactionException;
import com.qfs.store.query.IDictionaryCursor;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.qfs.store.transaction.ITransactionManager;
import com.qfs.util.impl.QfsFileTestUtils;
import com.qfs.util.impl.ThrowingLambda.ThrowingBiConsumer;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.util.impl.TruePredicate;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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

  protected static Path tempDir =
      QfsFileTestUtils.createTempDirectory(TestMemoryStatisticLoading.class);

  /**
   * Assert the number of offheap chunks by filling the datastore used for monitoring AND doing a
   * query on it for counting. Comparing the value from counting from {@link IMemoryStatistic}.
   */
  @Test
  public void testLoadDatastoreStats() throws AgentException, DatastoreTransactionException {
    final Application monitoredApplication = MonitoringTestUtils
        .setupApplication(new FullApplicationDescription(), resources,
            FullApplicationDescription::fillWithGenericData);

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
        .setupApplication(new FullApplicationDescription(), resources,
            FullApplicationDescription::fillWithGenericData);

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
        .setupApplication(new FullApplicationDescription(), resources,
            FullApplicationDescription::fillWithGenericData);

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
        new FullApplicationDescription(),
        resources,
        (datastore, manager) -> fillDatastore(datastore, manager, branches));

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

  protected void fillDatastore(
      IDatastore datastore, IActivePivotManager manager, Set<String> branches) {
    FullApplicationDescription.fillWithGenericData(datastore, manager);

    branches.forEach(
        br_string -> {
          ITransactionManager tm = datastore.getTransactionManager();
          try {
            tm.startTransactionOnBranch(br_string, "Sales");
          } catch (IllegalArgumentException | DatastoreTransactionException e) {
            throw new RuntimeException(e);
          }
          final Random r = new Random(47605);
          IntStream.range(0, 1000)
              .forEach(
                  i -> {
                    final int seller = r.nextInt(FullApplicationDescription.STORE_PEOPLE_COUNT);
                    int buyer;
                    do {
                      buyer = r.nextInt(FullApplicationDescription.STORE_PEOPLE_COUNT);
                    } while (buyer == seller);
                    tm.add(
                        "Sales",
                        i,
                        String.valueOf(seller),
                        String.valueOf(buyer),
                        LocalDate.now().plusDays(-r.nextInt(7)),
                        (long) r.nextInt(FullApplicationDescription.STORE_PRODUCT_COUNT));
                  });
          try {
            tm.commitTransaction();
          } catch (NoTransactionException | DatastoreTransactionException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Test
  public void testLoadFullStatsWithEpochs() throws AgentException, DatastoreTransactionException {
    final Set<String> branches = new HashSet<>();
    branches.add("branch1");
    branches.add("branch2");
    final long[] epochs = new long[] {1L, 2L};

    final Application monitoredApplication = MonitoringTestUtils.setupApplication(
        new FullApplicationDescription(),
        resources,
        (datastore, manager) -> fillDatastore(datastore, manager, branches));

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
    doTestLoadMonitoringDatastoreWithVectors(
        FullApplicationDescriptionWithVectors::fillWithGenericData);
  }

  @Test
  public void testLoadMonitoringDatastoreWithDuplicate() throws Exception {
    doTestLoadMonitoringDatastoreWithVectors(
        FullApplicationDescriptionWithVectors::fillWithDuplicateVectors);
  }

  public void doTestLoadMonitoringDatastoreWithVectors(
      ThrowingBiConsumer<IDatastore, IActivePivotManager> operations) throws Exception {

    final Application monitoredApplication =
        MonitoringTestUtils.setupApplication(new FullApplicationDescriptionWithVectors(), resources,
            operations);

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
            .forStore(DatastoreConstants.CHUNK_STORE)
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
