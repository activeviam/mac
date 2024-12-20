/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.statistic.memory;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.activeviam.activepivot.server.intf.api.observability.IMemoryAnalysisService;
import com.activeviam.database.api.conditions.BaseConditions;
import com.activeviam.database.api.query.AliasedField;
import com.activeviam.database.api.query.ListQuery;
import com.activeviam.database.api.schema.FieldPath;
import com.activeviam.database.datastore.api.IDatastore;
import com.activeviam.database.datastore.internal.monitoring.AMemoryStatisticWithPredicate;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig.ParentType;
import com.activeviam.tech.core.internal.util.TruePredicate;
import com.activeviam.tech.observability.api.memory.IMemoryStatistic;
import com.activeviam.tech.observability.internal.memory.AMemoryStatistic;
import com.activeviam.tech.observability.internal.memory.MemoryStatisticConstants;
import com.activeviam.tech.records.api.ICursor;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMemoryStatisticLoading extends ATestMemoryStatistic {

  /**
   * Assert the number of offheap chunks by filling the datastore used for monitoring AND doing a
   * query on it for counting. Comparing the value from counting from {@link IMemoryStatistic}.
   */
  @Test
  public void testLoadDatastoreStats() {
    createApplication(
        (monitoredDatastore, monitoredManager) -> {
          fillApplication(monitoredDatastore);

          final IMemoryAnalysisService analysisService =
              createService(monitoredDatastore, monitoredManager);
          final Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
          final Collection<AMemoryStatistic> storeStats =
              loadDatastoreMemoryStatFromFolder(exportPath);
          assertNotEquals(0, storeStats.size());
          assertLoadsCorrectly(storeStats, getClass());
        });
  }

  public void doTestLoadMonitoringDatastoreWithVectors(boolean duplicateVectors) {
    createApplicationWithVector(
        duplicateVectors,
        (monitoredDatastore, monitoredManager) -> {
          commitDataInDatastoreWithVectors(monitoredDatastore, duplicateVectors);

          final IMemoryAnalysisService analysisService =
              createService(monitoredDatastore, monitoredManager);
          final Path exportPath =
              analysisService.exportMostRecentVersion(
                  "doTestLoadMonitoringDatastoreWithVectors[" + duplicateVectors + "]");
          final Collection<AMemoryStatistic> datastoreStats =
              loadDatastoreMemoryStatFromFolder(exportPath);

          final IDatastore monitoringDatastore = assertLoadsCorrectly(datastoreStats, getClass());

          // Test that we have the correct count of vector blocks
          final Set<Long> storeIds =
              retrieveChunksOfType(monitoringDatastore, ParentType.VECTOR_BLOCK);
          Assertions.assertThat(storeIds).isNotEmpty();

          final Set<Long> statIds = new HashSet<>();
          datastoreStats.forEach(
              stat ->
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
        });
  }

  private Set<Long> retrieveChunksOfType(final IDatastore datastore, final ParentType component) {
    ListQuery query =
        datastore
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.CHUNK_STORE)
            .withCondition(
                BaseConditions.equal(FieldPath.of(DatastoreConstants.OWNER__COMPONENT), component))
            .withAliasedFields(AliasedField.fromFieldName(DatastoreConstants.CHUNK_ID))
            .toQuery();
    try (final ICursor cursor =
        datastore.getHead("master").getQueryRunner().listQuery(query).run()) {

      return StreamSupport.stream(cursor.spliterator(), false)
          .map(reader -> reader.readLong(0))
          .collect(Collectors.toSet());
    }
  }

  @Test
  public void testLoadPivotStats() {
    createApplication(
        (monitoredDatastore, monitoredManager) -> {
          fillApplication(monitoredDatastore);

          final IMemoryAnalysisService analysisService =
              createService(monitoredDatastore, monitoredManager);
          final Path exportPath = analysisService.exportMostRecentVersion("testLoadPivotStats");
          final Collection<AMemoryStatistic> pivotStats = loadPivotMemoryStatFromFolder(exportPath);
          assertNotEquals(0, pivotStats.size());
          assertLoadsCorrectly(pivotStats, getClass());
        });
  }

  @Test
  public void testLoadFullStats() {
    createApplication(
        (monitoredDatastore, monitoredManager) -> {
          fillApplication(monitoredDatastore);

          final IMemoryAnalysisService analysisService =
              createService(monitoredDatastore, monitoredManager);
          final Path exportPath = analysisService.exportMostRecentVersion("testLoadFullStats");
          final AMemoryStatistic fullStats = loadMemoryStatFromFolder(exportPath);
          assertNotEquals(null, fullStats);
          assertLoadsCorrectly(fullStats);
        });
  }

  @Test
  public void testLoadFullStatsWithBranches() {
    createApplication(
        (monitoredDatastore, monitoredManager) -> {
          Set<String> branchSet = new HashSet<>();
          branchSet.add("branch1");
          branchSet.add("branch2");

          fillApplicationWithBranches(monitoredDatastore, branchSet, false);

          // Also export master (?)
          branchSet.add("master");

          final IMemoryAnalysisService analysisService =
              createService(monitoredDatastore, monitoredManager);
          final Path exportPath = analysisService.exportBranches("testLoadFullStats", branchSet);
          final AMemoryStatistic fullStats = loadMemoryStatFromFolder(exportPath);
          assertNotEquals(null, fullStats);
          assertLoadsCorrectly(fullStats);
        });
  }

  @Test
  public void testLoadFullStatsWithEpochs() {
    createApplication(
        (monitoredDatastore, monitoredManager) -> {
          Set<String> branchSet = new HashSet<>();
          branchSet.add("branch1");
          branchSet.add("branch2");
          fillApplication(monitoredDatastore);
          fillApplicationWithBranches(monitoredDatastore, branchSet, true);

          long[] epochs = new long[] {1L, 2L};

          final IMemoryAnalysisService analysisService =
              createService(monitoredDatastore, monitoredManager);
          final Path exportPath = analysisService.exportVersions("testLoadFullStats", epochs);
          final AMemoryStatistic fullStats = loadMemoryStatFromFolder(exportPath);
          assertNotEquals(null, fullStats);
          assertLoadsCorrectly(fullStats);
        });
  }

  @Test
  public void testLoadMonitoringDatastoreWithVectorsWODuplicate() throws Exception {
    doTestLoadMonitoringDatastoreWithVectors(false);
  }

  @Test
  public void testLoadMonitoringDatastoreWithDuplicate() throws Exception {
    doTestLoadMonitoringDatastoreWithVectors(true);
  }

  /**
   * Asserts the chunks number and off-heap memory as computed from the loaded datastore are
   * consistent with the ones computed by visiting the statistic.
   */
  protected void assertLoadsCorrectly(AMemoryStatistic statistic) {
    assertLoadsCorrectly(Collections.singleton(statistic), getClass());
  }
}
