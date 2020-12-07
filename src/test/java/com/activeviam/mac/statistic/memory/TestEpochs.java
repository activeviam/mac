/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.activeviam.copper.testing.CubeTester;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription.ParentType;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription.UsedByVersion;
import com.activeviam.mac.statistic.memory.descriptions.MicroApplicationDescription;
import com.activeviam.mac.statistic.memory.descriptions.MicroApplicationDescriptionWithKeepAllEpochPolicy;
import com.activeviam.mac.statistic.memory.junit.RegistrySetupExtension;
import com.activeviam.properties.impl.ActiveViamProperty;
import com.activeviam.properties.impl.ActiveViamPropertyExtension;
import com.activeviam.properties.impl.ActiveViamPropertyExtension.ActiveViamPropertyExtensionBuilder;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.qfs.condition.ICondition;
import com.qfs.condition.impl.BaseConditions;
import com.qfs.junit.LocalResourcesExtension;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils.StatisticsSummary;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.multiversion.impl.Epoch;
import com.qfs.store.query.ICursor;
import com.qfs.store.record.IRecordReader;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.qfs.util.impl.QfsFileTestUtils;
import com.quartetfs.fwk.AgentException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(RegistrySetupExtension.class)
public class TestEpochs {

  @RegisterExtension
  protected static ActiveViamPropertyExtension propertyExtension =
      new ActiveViamPropertyExtensionBuilder()
          .withProperty(ActiveViamProperty.ACTIVEVIAM_TEST_PROPERTY, true)
          .build();

  @RegisterExtension
  protected final LocalResourcesExtension resources = new LocalResourcesExtension();

  protected static Path tempDir = QfsFileTestUtils.createTempDirectory(TestEpochs.class);

  protected Application monitoredApplication;
  protected Application monitoringApplication;
  protected IMemoryStatistic stats;
  protected StatisticsSummary statisticsSummary;
  protected CubeTester tester;

  @BeforeEach
  public void setup() throws AgentException {
    monitoredApplication = MonitoringTestUtils.setupApplication(
        new MicroApplicationDescriptionWithKeepAllEpochPolicy(),
        resources,
        (datastore, manager) -> {
          // epoch 1
          datastore.edit(transactionManager -> {
            IntStream.range(0, 10)
                .forEach(i -> transactionManager.add("A", i, 0.));
          });

          // epoch 2
          datastore.edit(transactionManager -> {
            IntStream.range(10, 20)
                .forEach(i -> transactionManager.add("A", i, 1.));
          });

          // epoch 3
          // drop partition from epoch 2
          datastore.edit(transactionManager -> {
            transactionManager.removeWhere("A", BaseConditions.Equal("value", 1.));
          });

          // epoch 4
          // make sure to add a new chunk on the 0-valued partition
          datastore.edit(transactionManager -> {
            IntStream.range(20, 20 + MicroApplicationDescription.CHUNK_SIZE)
                .forEach(i -> transactionManager.add("A", i, 0.));
          });

          // epoch 5
          // remaining chunks from epoch 4, but not used by version
          datastore.edit(transactionManager -> {
            transactionManager.removeWhere("A", BaseConditions.GreaterOrEqual("id", 20));
          });
        });

    monitoredApplication.getManager().getActivePivots().get("Cube")
        .commit(new Epoch(10L));

    final Path exportPath = MonitoringTestUtils.exportApplication(
        monitoredApplication.getDatastore(),
        monitoredApplication.getManager(),
        tempDir,
        this.getClass().getSimpleName());

    stats = MonitoringTestUtils.loadMemoryStatFromFolder(exportPath);
    statisticsSummary = MemoryStatisticsTestUtils.getStatisticsSummary(stats);

    monitoringApplication = MonitoringTestUtils.setupMonitoringApplication(stats, resources);

    tester = MonitoringTestUtils.createMonitoringCubeTester(monitoringApplication.getManager());
  }

  @Test
  public void testStatisticConsistency() throws AgentException, DatastoreTransactionException {
    MonitoringTestUtils.assertLoadsCorrectly(stats, resources);
  }

  @Test
  public void testExpectedEpochs() {
    final Set<Long> epochIds = retrieveEpochIds();

    Assertions.assertThat(epochIds)
        .containsExactlyInAnyOrder(1L, 2L, 3L, 4L, 5L, 10L);
  }

  @Test
  public void testAddedPartitions() {
    final Multimap<Long, Integer> partitionsPerEpoch = retrievePartitionsPerEpoch();

    // the same number of records is added between epochs 0 -> 1 and 1 -> 2
    // with a different value on the partitioned field: twice as many partitions
    Assertions.assertThat(partitionsPerEpoch.get(2L))
        .containsAll(partitionsPerEpoch.get(1L))
        .hasSize(2 * partitionsPerEpoch.get(1L).size());
  }

  @Test
  public void testDroppedPartitions() {
    final Multimap<Long, Integer> partitionsPerEpoch = retrievePartitionsPerEpoch();

    Assertions.assertThat(partitionsPerEpoch.get(3L))
        .hasSizeLessThan(partitionsPerEpoch.get(2L).size())
        .containsExactlyInAnyOrderElementsOf(partitionsPerEpoch.get(1L));
  }

  @Test
  public void testChunkInclusions() {
    final Set<Long> recordChunks = retrieveRecordChunks();
    final Multimap<Long, Long> chunksPerEpoch =
        retrieveChunksPerEpoch(recordChunks, BaseConditions.True());

    Assertions.assertThat(chunksPerEpoch.get(2L))
        .containsAll(chunksPerEpoch.get(1L))
        .hasSizeGreaterThan(chunksPerEpoch.get(1L).size());

    Assertions.assertThat(chunksPerEpoch.get(3L))
        .containsExactlyInAnyOrderElementsOf(chunksPerEpoch.get(1L));

    Assertions.assertThat(chunksPerEpoch.get(4L))
        .containsAll(chunksPerEpoch.get(3L));

    Assertions.assertThat(chunksPerEpoch.get(5L))
        .isEqualTo(chunksPerEpoch.get(3L));
  }

  @Test
  public void testUsedByVersionFlag() {
    final Set<Long> recordChunks = retrieveRecordChunks();

    final Multimap<Long, Long> chunksPerEpochUsedByVersion = retrieveChunksPerEpoch(recordChunks,
        BaseConditions.Equal(DatastoreConstants.CHUNK__USED_BY_VERSION, UsedByVersion.TRUE));

    final Multimap<Long, Long> chunksPerEpochNoFilter = retrieveChunksPerEpoch(recordChunks,
        BaseConditions.True());

    Assertions.assertThat(chunksPerEpochNoFilter.get(4L))
        .isEqualTo(chunksPerEpochNoFilter.get(5L));

    Assertions.assertThat(chunksPerEpochUsedByVersion.get(4L))
        .hasSizeGreaterThan(chunksPerEpochUsedByVersion.get(5L).size())
        .containsAll(chunksPerEpochUsedByVersion.get(5L));
  }

  @Test
  public void testDictionaryVersioning() {
    final Multimap<Long, Long> dictionariesPerEpoch = retrieveDictionariesPerEpoch();

    Assertions.assertThat(dictionariesPerEpoch.get(3L))
        .hasSizeLessThan(dictionariesPerEpoch.get(2L).size())
        .containsExactlyInAnyOrderElementsOf(dictionariesPerEpoch.get(1L));
  }

  @Test
  public void testIndexVersioning() {
    final Multimap<Long, Long> indicesPerEpoch = retrieveIndicesPerEpoch();

    Assertions.assertThat(indicesPerEpoch.get(3L))
        .containsExactlyInAnyOrderElementsOf(indicesPerEpoch.get(1L));

    Assertions.assertThat(indicesPerEpoch.get(2L))
        .hasSizeGreaterThan(indicesPerEpoch.get(1L).size())
        .containsAll(indicesPerEpoch.get(1L));
  }

  protected Set<Long> retrieveRecordChunks() {
    final ICursor cursor = monitoringApplication.getDatastore().getHead().getQueryRunner()
        .forStore(DatastoreConstants.CHUNK_STORE)
        .withCondition(
            BaseConditions.Equal(DatastoreConstants.OWNER__COMPONENT, ParentType.RECORDS))
        .selecting(DatastoreConstants.CHUNK_ID)
        .onCurrentThread()
        .run();

    return StreamSupport.stream(cursor.spliterator(), false)
        .map(c -> c.readLong(0))
        .collect(Collectors.toSet());
  }

  protected Multimap<Long, Long> retrieveChunksPerEpoch(
      final Collection<Long> chunkSet, final ICondition filter) {
    final ICursor cursor = monitoringApplication.getDatastore().getHead().getQueryRunner()
        .forStore(DatastoreConstants.CHUNK_STORE)
        .withCondition(BaseConditions.And(
            BaseConditions.In(DatastoreConstants.CHUNK_ID, chunkSet.toArray()),
            filter))
        .selecting(DatastoreConstants.VERSION__EPOCH_ID, DatastoreConstants.CHUNK_ID)
        .onCurrentThread()
        .run();

    final Multimap<Long, Long> chunksPerEpoch = HashMultimap.create();
    for (final IRecordReader reader : cursor) {
      final long epochId = reader.readLong(0);
      final long chunkId = reader.readLong(1);
      chunksPerEpoch.put(epochId, chunkId);
    }

    return chunksPerEpoch;
  }

  protected Set<Long> retrieveEpochIds() {
    final ICursor cursor = monitoringApplication.getDatastore().getHead().getQueryRunner()
        .forStore(DatastoreConstants.CHUNK_STORE)
        .withoutCondition()
        .selecting(DatastoreConstants.VERSION__EPOCH_ID)
        .onCurrentThread()
        .run();

    return StreamSupport.stream(cursor.spliterator(), false)
        .map(c -> c.readLong(0))
        .collect(Collectors.toSet());
  }

  protected Multimap<Long, Integer> retrievePartitionsPerEpoch() {
    final ICursor cursor = monitoringApplication.getDatastore().getHead().getQueryRunner()
        .forStore(DatastoreConstants.CHUNK_STORE)
        .withoutCondition()
        .selecting(DatastoreConstants.VERSION__EPOCH_ID, DatastoreConstants.CHUNK__PARTITION_ID)
        .onCurrentThread()
        .run();

    final Multimap<Long, Integer> partitionsPerEpoch = HashMultimap.create();
    for (final IRecordReader reader : cursor) {
      final long epochId = reader.readLong(0);
      final int partitionId = reader.readInt(1);
      if (partitionId != MemoryAnalysisDatastoreDescription.NO_PARTITION) {
        partitionsPerEpoch.put(epochId, partitionId);
      }
    }

    return partitionsPerEpoch;
  }

  protected Multimap<Long, Long> retrieveDictionariesPerEpoch() {
    final ICursor cursor = monitoringApplication.getDatastore().getHead().getQueryRunner()
        .forStore(DatastoreConstants.DICTIONARY_STORE)
        .withoutCondition()
        .selecting(DatastoreConstants.VERSION__EPOCH_ID, DatastoreConstants.DICTIONARY_ID)
        .onCurrentThread()
        .run();

    final Multimap<Long, Long> dictionariesPerEpoch = HashMultimap.create();
    for (final IRecordReader reader : cursor) {
      final long epochId = reader.readLong(0);
      final long dictionaryId = reader.readLong(1);
      dictionariesPerEpoch.put(epochId, dictionaryId);
    }

    return dictionariesPerEpoch;
  }

  protected Multimap<Long, Long> retrieveIndicesPerEpoch() {
    final ICursor cursor = monitoringApplication.getDatastore().getHead().getQueryRunner()
        .forStore(DatastoreConstants.INDEX_STORE)
        .withoutCondition()
        .selecting(DatastoreConstants.VERSION__EPOCH_ID, DatastoreConstants.INDEX_ID)
        .onCurrentThread()
        .run();

    final Multimap<Long, Long> indicesPerEpoch = HashMultimap.create();
    for (final IRecordReader reader : cursor) {
      final long epochId = reader.readLong(0);
      final long indexId = reader.readLong(1);
      indicesPerEpoch.put(epochId, indexId);
    }

    return indicesPerEpoch;
  }
}
