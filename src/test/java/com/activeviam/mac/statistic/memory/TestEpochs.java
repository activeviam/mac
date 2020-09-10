/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription.ParentType;
import com.activeviam.mac.statistic.memory.visitor.impl.FeedVisitor;
import com.activeviam.pivot.builders.StartBuilding;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.qfs.condition.impl.BaseConditions;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.multiversion.impl.Epoch;
import com.qfs.pivot.monitoring.impl.MemoryAnalysisService;
import com.qfs.store.IDatastore;
import com.qfs.store.query.ICursor;
import com.qfs.store.record.IRecordReader;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.IMultiVersionActivePivot;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.contributions.impl.ClasspathContributionProvider;
import com.quartetfs.fwk.impl.Pair;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestEpochs extends ATestMemoryStatistic {

  private Pair<IDatastore, IActivePivotManager> monitoredApp;
  private Pair<IDatastore, IActivePivotManager> monitoringApp;
  private IMemoryStatistic statistics;

  @BeforeClass
  public static void setupRegistry() {
    Registry.setContributionProvider(new ClasspathContributionProvider());
  }

  @Before
  public void setup() throws AgentException {
    initializeApplication();

    final Path exportPath = generateMemoryStatistics();

    statistics = loadMemoryStatFromFolder(exportPath);

    initializeMonitoringApplication(statistics);

    IMultiVersionActivePivot pivot =
        monitoringApp.getRight().getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);
    Assertions.assertThat(pivot).isNotNull();
  }

  private void initializeApplication() {
    monitoredApp = createMicroApplicationWithKeepAllEpochPolicy();

    monitoredApp.getLeft().edit(transactionManager -> {
      IntStream.range(0, 10)
          .forEach(i -> transactionManager.add("A", i, 0.));
    });

    monitoredApp.getLeft().edit(transactionManager -> {
      IntStream.range(10, 20)
          .forEach(i -> transactionManager.add("A", i, 1.));
    });

    monitoredApp.getLeft().edit(transactionManager -> {
      transactionManager.removeWhere("A", BaseConditions.Equal("value", 1.));
    });

   monitoredApp.getRight().getActivePivots().get("Cube")
        .commit(new Epoch(10L));
  }

  private Path generateMemoryStatistics() {
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

    monitoringDatastore.edit(
        tm -> data.accept(new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "storeA")));
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
    final Set<Long> epochIds = retrieveEpochIds();

    Assertions.assertThat(epochIds)
        .containsExactlyInAnyOrder(1L, 2L, 3L, 10L);
  }

  @Test
  public void testAddedPartitions() {
    final Multimap<Long, Integer> partitionsPerEpoch = retrievePartitionsPerEpoch();

    // the same number of records is added between epochs 0 -> 1 and 1 -> 2
    // with a different value on the partitioned field: twice as many partitions
    Assertions.assertThat(partitionsPerEpoch.get(2L))
        .hasSize(2 * partitionsPerEpoch.get(1L).size());

    Assertions.assertThat(partitionsPerEpoch.get(2L))
        .containsAll(partitionsPerEpoch.get(1L));
  }

  @Test
  public void testDroppedPartitions() {
    final Multimap<Long, Integer> partitionsPerEpoch = retrievePartitionsPerEpoch();

    Assertions.assertThat(partitionsPerEpoch.get(3L))
        .hasSizeLessThan(partitionsPerEpoch.get(2L).size());

    Assertions.assertThat(partitionsPerEpoch.get(3L))
        .containsExactlyInAnyOrderElementsOf(partitionsPerEpoch.get(1L));
  }

  @Test
  public void testChunkVersioning() {
    final Set<Long> recordChunks = retrieveRecordChunks();
    final Multimap<Long, Long> chunksPerEpoch = retrieveChunksPerEpoch(recordChunks);

    Assertions.assertThat(chunksPerEpoch.get(3L))
        .containsExactlyInAnyOrderElementsOf(chunksPerEpoch.get(1L));

    Assertions.assertThat(chunksPerEpoch.get(2L))
        .hasSize(2 * chunksPerEpoch.get(1L).size());

    Assertions.assertThat(chunksPerEpoch.get(2L))
        .containsAll(chunksPerEpoch.get(1L));
  }

  @Test
  public void testDictionaryVersioning() {
    final Multimap<Long, Long> dictionariesPerEpoch = retrieveDictionariesPerEpoch();

    Assertions.assertThat(dictionariesPerEpoch.get(3L))
        .hasSizeLessThan(dictionariesPerEpoch.get(2L).size());

    Assertions.assertThat(dictionariesPerEpoch.get(3L))
        .containsExactlyInAnyOrderElementsOf(dictionariesPerEpoch.get(1L));
  }

  @Test
  public void testIndexVersioning() {
    final Multimap<Long, Long> indicesPerEpoch = retrieveIndicesPerEpoch();

    Assertions.assertThat(indicesPerEpoch.get(3L))
        .containsExactlyInAnyOrderElementsOf(indicesPerEpoch.get(1L));

    Assertions.assertThat(indicesPerEpoch.get(2L))
        .hasSizeGreaterThan(indicesPerEpoch.get(1L).size());

    Assertions.assertThat(indicesPerEpoch.get(2L))
        .containsAll(indicesPerEpoch.get(1L));
  }

  protected Set<Long> retrieveRecordChunks() {
    final ICursor cursor = monitoringApp.getLeft().getHead().getQueryRunner()
        .forStore(DatastoreConstants.OWNER_STORE)
        .withCondition(
            BaseConditions.Equal(DatastoreConstants.OWNER__COMPONENT, ParentType.RECORDS))
        .selecting(DatastoreConstants.CHUNK_ID)
        .onCurrentThread()
        .run();

    return StreamSupport.stream(cursor.spliterator(), false)
        .map(c -> c.readLong(0))
        .collect(Collectors.toSet());
  }

  protected Multimap<Long, Long> retrieveChunksPerEpoch(final Collection<Long> chunkSet) {
    final ICursor cursor = monitoringApp.getLeft().getHead().getQueryRunner()
        .forStore(DatastoreConstants.CHUNK_STORE)
        .withCondition(
            BaseConditions.In(DatastoreConstants.CHUNK_ID, chunkSet.toArray()))
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
    final ICursor cursor = monitoringApp.getLeft().getHead().getQueryRunner()
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
    final ICursor cursor = monitoringApp.getLeft().getHead().getQueryRunner()
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
    final ICursor cursor = monitoringApp.getLeft().getHead().getQueryRunner()
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
    final ICursor cursor = monitoringApp.getLeft().getHead().getQueryRunner()
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
