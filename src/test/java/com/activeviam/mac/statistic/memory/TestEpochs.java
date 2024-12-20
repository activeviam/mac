/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.activeviam.activepivot.core.impl.internal.utils.ApplicationInTests;
import com.activeviam.activepivot.core.intf.api.cube.IMultiVersionActivePivot;
import com.activeviam.activepivot.server.impl.private_.observability.memory.MemoryAnalysisService;
import com.activeviam.activepivot.server.spring.api.config.IDatastoreSchemaDescriptionConfig;
import com.activeviam.database.api.ICondition;
import com.activeviam.database.api.conditions.BaseConditions;
import com.activeviam.database.api.query.AliasedField;
import com.activeviam.database.api.query.ListQuery;
import com.activeviam.database.api.schema.FieldPath;
import com.activeviam.database.datastore.api.IDatastore;
import com.activeviam.database.datastore.internal.IInternalDatastore;
import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.cfg.impl.RegistryInitializationConfig;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig.ParentType;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig.UsedByVersion;
import com.activeviam.tech.core.api.agent.AgentException;
import com.activeviam.tech.observability.internal.memory.AMemoryStatistic;
import com.activeviam.tech.records.api.ICursor;
import com.activeviam.tech.records.api.IRecordReader;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestEpochs extends ATestMemoryStatistic {

  private ApplicationInTests<IDatastore> monitoredApp;
  private ApplicationInTests<IInternalDatastore> monitoringApp;
  private AMemoryStatistic statistics;

  @BeforeAll
  public static void setupRegistry() {
    RegistryInitializationConfig.setupRegistry();
  }

  @BeforeEach
  public void setup() throws AgentException {
    initializeApplication();

    final Path exportPath = generateMemoryStatistics();

    this.statistics = loadMemoryStatFromFolder(exportPath);

    initializeMonitoringApplication(this.statistics);

    IMultiVersionActivePivot pivot =
        this.monitoringApp
            .getManager()
            .getActivePivots()
            .get(ManagerDescriptionConfig.MONITORING_CUBE);
    Assertions.assertThat(pivot).isNotNull();
  }

  private void initializeApplication() {
    this.monitoredApp = createMicroApplicationWithKeepAllEpochPolicy();

    // epoch 1
    this.monitoredApp
        .getDatabase()
        .edit(
            transactionManager ->
                IntStream.range(0, 10).forEach(i -> transactionManager.add("A", i, 0.)));

    // epoch 2
    this.monitoredApp
        .getDatabase()
        .edit(
            transactionManager ->
                IntStream.range(10, 20).forEach(i -> transactionManager.add("A", i, 1.)));

    // epoch 3
    // drop partition from epoch 2
    this.monitoredApp
        .getDatabase()
        .edit(
            transactionManager ->
                transactionManager.removeWhere(
                    "A", BaseConditions.equal(FieldPath.of("value"), 1.)));

    // epoch 4
    // make sure to add a new chunk on the 0-valued partition
    this.monitoredApp
        .getDatabase()
        .edit(
            transactionManager ->
                IntStream.range(20, 20 + MICROAPP_CHUNK_SIZE)
                    .forEach(i -> transactionManager.add("A", i, 0.)));

    // epoch 5
    // remaining chunks from epoch 4, but not used by version
    this.monitoredApp
        .getDatabase()
        .edit(
            transactionManager ->
                transactionManager.removeWhere(
                    "A", BaseConditions.greaterOrEqual(FieldPath.of("id"), 20)));
  }

  private Path generateMemoryStatistics() {
    this.monitoredApp.getDatabase().getEpochManager().forceDiscardEpochs(node -> true);
    performGC();

    final MemoryAnalysisService analysisService =
        (MemoryAnalysisService)
            createService(this.monitoredApp.getDatabase(), this.monitoredApp.getManager());
    return analysisService.exportApplication("testEpochs");
  }

  private void initializeMonitoringApplication(final AMemoryStatistic data) throws AgentException {
    ManagerDescriptionConfig config = new ManagerDescriptionConfig();
    IDatastoreSchemaDescriptionConfig schemaConfig = new MemoryAnalysisDatastoreDescriptionConfig();

    this.monitoringApp =
        ApplicationInTests.builder()
            .withDatastore(schemaConfig.datastoreSchemaDescription())
            .withManager(config.managerDescription())
            .build();

    resources.register(this.monitoringApp).start();

    ATestMemoryStatistic.feedMonitoringApplication(
        monitoringApp.getDatabase(), List.of(data), "testEpochs");
  }

  @Test
  public void testStatisticConsistency() {
    assertLoadsCorrectly(this.statistics.getChildren(), TestEpochs.class);
  }

  @Test
  public void testExpectedEpochs() {
    final Set<Long> epochIds = retrieveEpochIds();

    Assertions.assertThat(epochIds).containsExactlyInAnyOrder(1L, 2L, 3L, 4L, 5L);
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
        retrieveChunksPerEpoch(recordChunks, BaseConditions.TRUE);

    Assertions.assertThat(chunksPerEpoch.get(2L))
        .containsAll(chunksPerEpoch.get(1L))
        .hasSizeGreaterThan(chunksPerEpoch.get(1L).size());

    Assertions.assertThat(chunksPerEpoch.get(3L))
        .containsExactlyInAnyOrderElementsOf(chunksPerEpoch.get(1L));

    Assertions.assertThat(chunksPerEpoch.get(4L)).containsAll(chunksPerEpoch.get(3L));

    Assertions.assertThat(chunksPerEpoch.get(5L)).containsAll(chunksPerEpoch.get(3L));
  }

  @Test
  public void testUsedByVersionFlag() {
    final Set<Long> recordChunks = retrieveRecordChunks();

    final Multimap<Long, Long> chunksPerEpochUsedByVersion =
        retrieveChunksPerEpoch(
            recordChunks,
            BaseConditions.equal(
                FieldPath.of(DatastoreConstants.CHUNK__USED_BY_VERSION), UsedByVersion.TRUE));

    final Multimap<Long, Long> chunksPerEpochNoFilter =
        retrieveChunksPerEpoch(recordChunks, BaseConditions.TRUE);

    Assertions.assertThat(chunksPerEpochNoFilter.get(4L)).isEqualTo(chunksPerEpochNoFilter.get(5L));

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
    ListQuery query =
        monitoringApp
            .getDatabase()
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.CHUNK_STORE)
            .withCondition(
                BaseConditions.equal(
                    FieldPath.of(DatastoreConstants.OWNER__COMPONENT), ParentType.RECORDS))
            .withAliasedFields(AliasedField.fromFieldName(DatastoreConstants.CHUNK_ID))
            .toQuery();
    try (final ICursor cursor =
        this.monitoringApp
            .getDatabase()
            .getHead("master")
            .getQueryRunner()
            .listQuery(query)
            .run()) {

      return StreamSupport.stream(cursor.spliterator(), false)
          .map(c -> c.readLong(0))
          .collect(Collectors.toSet());
    }
  }

  protected Multimap<Long, Long> retrieveChunksPerEpoch(
      final Collection<Long> chunkSet, final ICondition filter) {
    ListQuery query =
        this.monitoringApp
            .getDatabase()
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.CHUNK_STORE)
            .withCondition(
                BaseConditions.and(
                    BaseConditions.in(
                        FieldPath.of(DatastoreConstants.CHUNK_ID), chunkSet.toArray()),
                    filter))
            .withAliasedFields(
                AliasedField.fromFieldName(DatastoreConstants.VERSION__EPOCH_ID),
                AliasedField.fromFieldName(DatastoreConstants.CHUNK_ID))
            .toQuery();
    try (final ICursor cursor =
        this.monitoringApp
            .getDatabase()
            .getHead("master")
            .getQueryRunner()
            .listQuery(query)
            .run()) {

      final Multimap<Long, Long> chunksPerEpoch = HashMultimap.create();
      for (final IRecordReader reader : cursor) {
        final long epochId = reader.readLong(0);
        final long chunkId = reader.readLong(1);
        chunksPerEpoch.put(epochId, chunkId);
      }

      return chunksPerEpoch;
    }
  }

  protected Set<Long> retrieveEpochIds() {
    ListQuery query =
        this.monitoringApp
            .getDatabase()
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.CHUNK_STORE)
            .withoutCondition()
            .withAliasedFields(AliasedField.fromFieldName(DatastoreConstants.VERSION__EPOCH_ID))
            .toQuery();
    try (final ICursor cursor =
        this.monitoringApp
            .getDatabase()
            .getHead("master")
            .getQueryRunner()
            .listQuery(query)
            .run()) {

      return StreamSupport.stream(cursor.spliterator(), false)
          .map(c -> c.readLong(0))
          .collect(Collectors.toSet());
    }
  }

  protected Multimap<Long, Integer> retrievePartitionsPerEpoch() {
    ListQuery query =
        this.monitoringApp
            .getDatabase()
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.CHUNK_STORE)
            .withoutCondition()
            .withAliasedFields(
                AliasedField.fromFieldName(DatastoreConstants.VERSION__EPOCH_ID),
                AliasedField.fromFieldName(DatastoreConstants.CHUNK__PARTITION_ID))
            .toQuery();
    try (final ICursor cursor =
        this.monitoringApp
            .getDatabase()
            .getHead("master")
            .getQueryRunner()
            .listQuery(query)
            .run()) {

      final Multimap<Long, Integer> partitionsPerEpoch = HashMultimap.create();
      for (final IRecordReader reader : cursor) {
        final long epochId = reader.readLong(0);
        final int partitionId = reader.readInt(1);
        if (partitionId != MemoryAnalysisDatastoreDescriptionConfig.NO_PARTITION) {
          partitionsPerEpoch.put(epochId, partitionId);
        }
      }

      return partitionsPerEpoch;
    }
  }

  protected Multimap<Long, Long> retrieveDictionariesPerEpoch() {
    ListQuery query =
        this.monitoringApp
            .getDatabase()
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.DICTIONARY_STORE)
            .withoutCondition()
            .withAliasedFields(
                AliasedField.fromFieldName(DatastoreConstants.VERSION__EPOCH_ID),
                AliasedField.fromFieldName(DatastoreConstants.DICTIONARY_ID))
            .toQuery();
    try (final ICursor cursor =
        this.monitoringApp
            .getDatabase()
            .getHead("master")
            .getQueryRunner()
            .listQuery(query)
            .run()) {

      final Multimap<Long, Long> dictionariesPerEpoch = HashMultimap.create();
      for (final IRecordReader reader : cursor) {
        final long epochId = reader.readLong(0);
        final long dictionaryId = reader.readLong(1);
        dictionariesPerEpoch.put(epochId, dictionaryId);
      }

      return dictionariesPerEpoch;
    }
  }

  protected Multimap<Long, Long> retrieveIndicesPerEpoch() {
    ListQuery query =
        this.monitoringApp
            .getDatabase()
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.INDEX_STORE)
            .withoutCondition()
            .withAliasedFields(
                AliasedField.fromFieldName(DatastoreConstants.VERSION__EPOCH_ID),
                AliasedField.fromFieldName(DatastoreConstants.INDEX_ID))
            .toQuery();
    try (final ICursor cursor =
        this.monitoringApp
            .getDatabase()
            .getHead("master")
            .getQueryRunner()
            .listQuery(query)
            .run()) {

      final Multimap<Long, Long> indicesPerEpoch = HashMultimap.create();
      for (final IRecordReader reader : cursor) {
        final long epochId = reader.readLong(0);
        final long indexId = reader.readLong(1);
        indicesPerEpoch.put(epochId, indexId);
      }

      return indicesPerEpoch;
    }
  }
}
