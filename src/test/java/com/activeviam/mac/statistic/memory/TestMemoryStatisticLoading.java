/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.statistic.memory;

import static org.junit.Assert.assertNotEquals;

import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription.ParentType;
import com.activeviam.mac.statistic.memory.descriptions.Application;
import com.activeviam.mac.statistic.memory.descriptions.FullApplication;
import com.activeviam.mac.statistic.memory.descriptions.FullApplicationWithVectors;
import com.qfs.condition.impl.BaseConditions;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;
import com.qfs.monitoring.statistic.memory.visitor.impl.AMemoryStatisticWithPredicate;
import com.qfs.pivot.monitoring.impl.MemoryAnalysisService;
import com.qfs.service.monitoring.IMemoryAnalysisService;
import com.qfs.store.IDatastore;
import com.qfs.store.query.IDictionaryCursor;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.fwk.util.impl.TruePredicate;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class TestMemoryStatisticLoading {

  public static abstract class AGivenFullApplication extends ASingleAppMonitoringTest {
    @Override
    protected IDatastoreSchemaDescription datastoreSchema() {
      return FullApplication.datastoreDescription();
    }

    @Override
    protected IActivePivotManagerDescription managerDescription(
        IDatastoreSchemaDescription datastoreSchema) {
      return FullApplication.managerDescription(datastoreSchema);
    }

    @Override
    protected void beforeExport(
        IDatastore datastore, IActivePivotManager manager) {
      FullApplication.fill(datastore);
    }

    @Override
    protected void loadAndImportStatistics() {
      // disable statistics loading, as it is to be tested
    }
  }

  @Nested
  public class GivenFullApplication extends AGivenFullApplication {
    @Test
    public void testLoadDatastoreStats() throws IOException {
      statistics = loadMemoryStatistics(statisticsPath, path ->
          path.getFileName().toString()
              .startsWith(MemoryAnalysisService.STORE_FILE_PREFIX));
      feedStatisticsIntoDatastore(statistics, monitoringDatastore);

      assertNotEquals(0, statistics.size());
      assertStatisticsConsistency();
    }

    @Test
    public void testLoadPivotStats() throws IOException {
      statistics = loadMemoryStatistics(statisticsPath, path ->
          path.getFileName().toString()
              .startsWith(MemoryAnalysisService.PIVOT_FILE_PREFIX));
      feedStatisticsIntoDatastore(statistics, monitoringDatastore);

      assertNotEquals(0, statistics.size());
      assertStatisticsConsistency();
    }

    @Test
    public void testLoadFullStats() throws IOException {
      statistics = loadMemoryStatistics(statisticsPath);
      feedStatisticsIntoDatastore(statistics, monitoringDatastore);

      assertNotEquals(0, statistics.size());
      assertStatisticsConsistency();
    }
  }

  @Nested
  public class GivenFullApplicationWithBranches extends AGivenFullApplication {

    @Override
    protected void beforeExport(
        IDatastore datastore, IActivePivotManager manager) {
      Set<String> branchSet = new HashSet<>();
      branchSet.add("branch1");
      branchSet.add("branch2");

      FullApplication.fillWithBranches(monitoredDatastore, branchSet);
    }

    @Override
    protected Path exportApplicationMemoryStatistics(
        IDatastore datastore, IActivePivotManager manager, Path exportPath) {
      final IMemoryAnalysisService analysisService = new MemoryAnalysisService(
          datastore, manager, datastore.getEpochManager(), exportPath);
      return analysisService
          .exportBranches("memoryStatistics", Set.of("branch1", "branch2", "master"));
    }

    @Test
    public void testLoadFullStatsWithBranches() throws IOException {
      statistics = loadMemoryStatistics(statisticsPath);
      feedStatisticsIntoDatastore(statistics, monitoringDatastore);

      assertNotEquals(0, statistics.size());
      assertStatisticsConsistency();
    }
  }

  @Nested
  public class GivenFullApplicationWithEpochs extends AGivenFullApplication {

    @Override
    protected void beforeExport(
        IDatastore datastore, IActivePivotManager manager) {
      Set<String> branchSet = new HashSet<>();
      branchSet.add("branch1");
      branchSet.add("branch2");

      FullApplication.fill(monitoredDatastore);
      FullApplication.fillWithBranches(monitoredDatastore, branchSet);
    }

    @Override
    protected Path exportApplicationMemoryStatistics(
        IDatastore datastore, IActivePivotManager manager, Path exportPath) {
      final IMemoryAnalysisService analysisService = new MemoryAnalysisService(
          datastore, manager, datastore.getEpochManager(), exportPath);
      return analysisService
          .exportVersions("memoryStatistics", new long[] {1L, 2L});
    }

    @Test
    public void testLoadFullStatsWithBranches() throws IOException {
      statistics = loadMemoryStatistics(statisticsPath);
      feedStatisticsIntoDatastore(statistics, monitoringDatastore);

      assertNotEquals(0, statistics.size());
      assertStatisticsConsistency();
    }
  }

  public abstract static class AGivenFullApplicationWithVectors extends ASingleAppMonitoringTest {

    @Override
    protected IDatastoreSchemaDescription datastoreSchema() {
      return FullApplicationWithVectors.datastoreDescription();
    }

    @Override
    protected IActivePivotManagerDescription managerDescription(
        IDatastoreSchemaDescription datastoreSchema) {
      return Application.cubelessManagerDescription();
    }

    @Override
    protected void loadAndImportStatistics() {
      // disable statistics loading, as it is to be tested
    }

    protected void assertVectorBlockConsistency() {
      final Set<Long> storeIds = gatherChunkIdsFromMonitoringApp();
      Assertions.assertThat(storeIds).isNotEmpty();

      final Set<Long> statIds = gatherChunkIdsFromStatistics();
      Assertions.assertThat(storeIds).isNotEmpty();

      Assertions.assertThat(storeIds).isEqualTo(statIds);
    }

    protected Set<Long> gatherChunkIdsFromMonitoringApp() {
      final IDictionaryCursor cursor =
          monitoringDatastore
              .getHead()
              .getQueryRunner()
              .forStore(DatastoreConstants.CHUNK_STORE)
              .withCondition(BaseConditions
                  .Equal(DatastoreConstants.CHUNK__COMPONENT, ParentType.VECTOR_BLOCK))
              .selecting(DatastoreConstants.CHUNK_ID)
              .onCurrentThread()
              .run();
      return StreamSupport.stream(cursor.spliterator(), false)
          .map(record -> record.readLong(0))
          .collect(Collectors.toSet());
    }

    protected Set<Long> gatherChunkIdsFromStatistics() {
      final Set<Long> chunkIds = new HashSet<>();
      statistics.forEach(
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
                        chunkIds.add(
                            statistic
                                .getAttribute(MemoryStatisticConstants.ATTR_NAME_CHUNK_ID)
                                .asLong());
                      }
                      return true; // Iterate over every item
                    }
                  }));
      return chunkIds;
    }
  }

  @Nested
  public class GivenFullApplicationWithVectorsWithoutDuplicates extends AGivenFullApplicationWithVectors {
    @Override
    protected void beforeExport(
        IDatastore datastore, IActivePivotManager manager) {
      FullApplicationWithVectors.fill(datastore);
    }

    @Test
    public void testLoad() throws IOException {
      statistics = loadMemoryStatistics(statisticsPath);
      feedStatisticsIntoDatastore(statistics, monitoringDatastore);

      assertNotEquals(0, statistics.size());
      assertStatisticsConsistency();
      assertVectorBlockConsistency();
    }
  }

  @Nested
  public class GivenFullApplicationWithVectorsWithDuplicates extends AGivenFullApplicationWithVectors {
    @Override
    protected void beforeExport(
        IDatastore datastore, IActivePivotManager manager) {
      FullApplicationWithVectors.fillWithDuplicates(datastore);
    }

    @Test
    public void testLoad() throws IOException {
      statistics = loadMemoryStatistics(statisticsPath);
      feedStatisticsIntoDatastore(statistics, monitoringDatastore);

      assertNotEquals(0, statistics.size());
      assertStatisticsConsistency();
      assertVectorBlockConsistency();
    }
  }
}
