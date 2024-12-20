package com.activeviam.mac.statistic.memory;

import static com.activeviam.mac.memory.DatastoreConstants.CHUNK_STORE;
import static com.activeviam.tech.test.internal.assertj.AssertJConditions.identical;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import com.activeviam.activepivot.core.impl.internal.utils.ApplicationInTests;
import com.activeviam.activepivot.server.intf.api.observability.IMemoryAnalysisService;
import com.activeviam.database.api.conditions.BaseConditions;
import com.activeviam.database.api.query.AliasedField;
import com.activeviam.database.api.query.ListQuery;
import com.activeviam.database.api.schema.FieldPath;
import com.activeviam.database.datastore.api.IDatastore;
import com.activeviam.database.datastore.api.transaction.DatastoreTransactionException;
import com.activeviam.database.datastore.internal.IInternalDatastore;
import com.activeviam.database.datastore.internal.NoTransactionException;
import com.activeviam.database.datastore.internal.impl.Datastore;
import com.activeviam.database.datastore.internal.monitoring.AMemoryStatisticWithPredicate;
import com.activeviam.mac.memory.AnalysisDatastoreFeeder;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig.ParentType;
import com.activeviam.tech.chunks.internal.impl.ChunkSingleVector;
import com.activeviam.tech.chunks.internal.vectors.direct.impl.DirectIntegerVectorBlock;
import com.activeviam.tech.chunks.internal.vectors.direct.impl.DirectLongVectorBlock;
import com.activeviam.tech.core.api.agent.AgentException;
import com.activeviam.tech.core.api.exceptions.ActiveViamRuntimeException;
import com.activeviam.tech.dictionaries.api.IDictionary;
import com.activeviam.tech.observability.api.memory.IMemoryStatistic;
import com.activeviam.tech.observability.internal.memory.AMemoryStatistic;
import com.activeviam.tech.observability.internal.memory.ChunkStatistic;
import com.activeviam.tech.observability.internal.memory.MemoryStatisticConstants;
import com.activeviam.tech.records.api.ICursor;
import com.activeviam.tech.records.api.IRecordReader;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMemoryMonitoringDatastoreContent extends ATestMemoryStatistic {

  /**
   * Tests the consistency between the chunks of an ActivePivot application and its the monitoring
   * data obtained by loading exported data.
   */
  @Test
  public void testDatastoreMonitoringValues() {
    createMinimalApplication(
        (monitoredDatastore, monitoredManager) -> {
          fillApplicationMinimal(monitoredDatastore);
          performGC();

          final IMemoryAnalysisService analysisService =
              createService(monitoredDatastore, monitoredManager);
          final Path exportPath = analysisService.exportApplication("testLoadComplete");

          final AMemoryStatistic fullStats = loadMemoryStatFromFolder(exportPath);
          final Datastore monitoringDatastore = (Datastore) createAnalysisDatastore();

          IDictionary<Object> dic =
              monitoredDatastore.getQueryMetadata().getDictionaries().getDictionary("Sales", "id");

          final long[] chunkIds = new long[2];
          final long[] chunkSizes = new long[2];

          AtomicInteger rnk = new AtomicInteger();
          final long[] monitoredChunkSizes = new long[2];

          // Check all the chunks held by the Dictionary of that key field of the store
          IMemoryStatistic stat = dic.getMemoryStatistic();

          stat.getChildren()
              .forEach(
                  c_st ->
                      c_st.getChildren()
                          .forEach(
                              c_c_st ->
                                  c_c_st
                                      .getChildren()
                                      .forEach(
                                          c_c_c_st ->
                                              c_c_c_st
                                                  .getChildren()
                                                  .forEach(
                                                      c_c_c_c_st -> {
                                                        chunkSizes[rnk.get()] =
                                                            c_c_c_c_st
                                                                .getAttribute(
                                                                    MemoryStatisticConstants
                                                                        .ATTR_NAME_LENGTH)
                                                                .asLong();
                                                        chunkIds[rnk.get()] =
                                                            c_c_c_c_st
                                                                .getAttribute(
                                                                    MemoryStatisticConstants
                                                                        .ATTR_NAME_CHUNK_ID)
                                                                .asLong();
                                                        rnk.incrementAndGet();
                                                      }))));

          ATestMemoryStatistic.feedMonitoringApplication(
              monitoringDatastore, List.of(fullStats), "test");
          for (int i = 0; i < chunkIds.length; i++) {
            ListQuery query =
                monitoringDatastore
                    .getQueryManager()
                    .listQuery()
                    .forTable(DatastoreConstants.CHUNK_STORE)
                    .withCondition(
                        BaseConditions.equal(
                            FieldPath.of(DatastoreConstants.CHUNK_ID), chunkIds[i]))
                    .withAliasedFields(AliasedField.fromFieldName(DatastoreConstants.CHUNK__SIZE))
                    .toQuery();

            final ICursor records =
                monitoringDatastore.getHead("master").getQueryRunner().listQuery(query).run();

            monitoredChunkSizes[i] = records.iterator().next().readLong(0);
            records.close();
          }
          // Now we verify the monitored chunks and the chunk in the Datastore of the Monitoring
          // Cube are identical
          assertArrayEquals(chunkSizes, monitoredChunkSizes);
        });
  }

  @Test
  public void testChunkStructureFieldsWithSingleRecord() {
    final ApplicationInTests<IInternalDatastore> monitoredApp = createMicroApplication();

    // Add a single record
    monitoredApp.getDatabase().edit(tm -> tm.add("A", 0));

    // Force to discard all versions
    monitoredApp.getDatabase().getEpochManager().forceDiscardEpochs(__ -> true);

    final IMemoryAnalysisService analysisService =
        createService(monitoredApp.getDatabase(), monitoredApp.getManager());
    final Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
    final Collection<AMemoryStatistic> storeStats = loadDatastoreMemoryStatFromFolder(exportPath);

    // Make sure there is only one loaded store
    Assertions.assertThat(storeStats).hasSize(1);

    // Start a monitoring datastore with the exported data
    final IInternalDatastore monitoringDatastore = createAnalysisDatastore();
    ATestMemoryStatistic.feedMonitoringApplication(monitoringDatastore, storeStats, "storeA");

    ListQuery query =
        monitoringDatastore
            .getQueryManager()
            .listQuery()
            .forTable(CHUNK_STORE)
            .withCondition(
                BaseConditions.equal(
                    FieldPath.of(DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE),
                    MemoryAnalysisDatastoreDescriptionConfig.ParentType.RECORDS))
            .withAliasedFields(
                AliasedField.fromFieldName(DatastoreConstants.CHUNK__FREE_ROWS),
                AliasedField.fromFieldName(DatastoreConstants.CHUNK__NON_WRITTEN_ROWS),
                AliasedField.fromFieldName(DatastoreConstants.CHUNK__SIZE),
                AliasedField.fromFieldName(DatastoreConstants.CHUNK__OFF_HEAP_SIZE))
            .toQuery();

    // Query record chunks data :
    try (final ICursor cursor =
        monitoringDatastore.getHead("master").getQueryRunner().listQuery(query).run()) {
      final List<Object[]> list = new ArrayList<>();
      cursor.forEach(record -> list.add(record.toTuple()));
      // Expect 3 Chunks :
      // - 1 Chunk for versions : directChunkLong
      // - 2 Chunks for the actual records
      // * 1 Wrapper chunk (DirectChunkPositiveInteger) -> Verify that the offHeap size equal
      // to Zero
      // * 1 Content Chunk (DirectChunkBits) -> "0" is stored as the dictionary value 1,
      // the underlying chunk is promoted to DirectChunkBits (order = 1), which allocates
      // 32B of off-heap memory
      Assertions.assertThat(list)
          .containsExactly(
              new Object[] {0L, 255L, 256L, 0L},
              new Object[] {0L, 255L, 256L, 32L},
              new Object[] {0L, 255L, 256L, 2048L});
    }
  }

  @Test
  public void testChunkStructureFieldsWithFullChunk() {
    final ApplicationInTests<IInternalDatastore> monitoredApp = createMicroApplication();

    // Add a full chunk
    monitoredApp
        .getDatabase()
        .edit(tm -> IntStream.range(0, 256).forEach(i -> tm.add("A", i * i * i)));

    // Force to discard all versions
    monitoredApp.getDatabase().getEpochManager().forceDiscardEpochs(__ -> true);

    final IMemoryAnalysisService analysisService =
        createService(monitoredApp.getDatabase(), monitoredApp.getManager());
    final Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
    final Collection<AMemoryStatistic> storeStats = loadDatastoreMemoryStatFromFolder(exportPath);

    // Make sure there is only one loaded store
    Assertions.assertThat(storeStats).hasSize(1);

    // Start a monitoring datastore with the exported data
    final IInternalDatastore monitoringDatastore = createAnalysisDatastore();
    ATestMemoryStatistic.feedMonitoringApplication(monitoringDatastore, storeStats, "storeA");

    // Query record chunks data :
    ListQuery query =
        monitoringDatastore
            .getQueryManager()
            .listQuery()
            .forTable(CHUNK_STORE)
            .withCondition(
                BaseConditions.equal(
                    FieldPath.of(DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE),
                    MemoryAnalysisDatastoreDescriptionConfig.ParentType.RECORDS))
            .withAliasedFields(
                AliasedField.fromFieldName(DatastoreConstants.CHUNK__FREE_ROWS),
                AliasedField.fromFieldName(DatastoreConstants.CHUNK__NON_WRITTEN_ROWS),
                AliasedField.fromFieldName(DatastoreConstants.CHUNK__SIZE),
                AliasedField.fromFieldName(DatastoreConstants.CHUNK__OFF_HEAP_SIZE))
            .toQuery();

    try (final ICursor cursor =
        monitoringDatastore.getHead("master").getQueryRunner().listQuery(query).run()) {
      final List<Object[]> list = new ArrayList<>();
      cursor.forEach(
          (record) -> {
            Object[] data = record.toTuple();
            list.add(data);
          });
      // Expect 3 Chunks :
      // - 1 Chunk for versions : directChunkLong
      // - 1 ChunkOffset for the records -> verify it does not consume off-heap memory
      // - 1 Chunk for the actual records (ChunkShorts) -> Verify that 256 bytes of data is stored
      // off-heap
      Assertions.assertThat(list)
          .containsExactlyInAnyOrder(
              new Object[] {0L, 0L, 256L, 256L},
              new Object[] {0L, 0L, 256L, 0L},
              new Object[] {0L, 0L, 256L, 2048L});
    }
  }

  @Test
  public void testChunkStructureFieldsWithTwoChunks() {
    final ApplicationInTests<IInternalDatastore> monitoredApp = createMicroApplication();

    // Add a full chunk + 10 records on the second chunk
    monitoredApp
        .getDatabase()
        .edit(tm -> IntStream.range(0, 266).forEach(i -> tm.add("A", i * i * i)));

    // Force to discard all versions
    monitoredApp.getDatabase().getEpochManager().forceDiscardEpochs(__ -> true);

    final IMemoryAnalysisService analysisService =
        createService(monitoredApp.getDatabase(), monitoredApp.getManager());
    final Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
    final Collection<AMemoryStatistic> storeStats = loadDatastoreMemoryStatFromFolder(exportPath);

    // Make sure there is only one loaded store
    Assertions.assertThat(storeStats).hasSize(1);

    // Start a monitoring datastore with the exported data
    final IInternalDatastore monitoringDatastore = createAnalysisDatastore();
    ATestMemoryStatistic.feedMonitoringApplication(monitoringDatastore, storeStats, "storeA");

    ListQuery query =
        monitoringDatastore
            .getQueryManager()
            .listQuery()
            .forTable(CHUNK_STORE)
            .withCondition(
                BaseConditions.equal(
                    FieldPath.of(DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE),
                    MemoryAnalysisDatastoreDescriptionConfig.ParentType.RECORDS))
            .withAliasedFields(
                AliasedField.fromFieldName(DatastoreConstants.CHUNK__FREE_ROWS),
                AliasedField.fromFieldName(DatastoreConstants.CHUNK__NON_WRITTEN_ROWS),
                AliasedField.fromFieldName(DatastoreConstants.CHUNK__SIZE),
                AliasedField.fromFieldName(DatastoreConstants.CHUNK__OFF_HEAP_SIZE))
            .toQuery();

    // Query record chunks data :
    try (final ICursor cursor =
        monitoringDatastore.getHead("master").getQueryRunner().listQuery(query).run()) {
      final List<Object[]> list = new ArrayList<>();
      cursor.forEach(
          (record) -> {
            Object[] data = record.toTuple();
            list.add(data);
          });
      // Expect 6 Chunks :
      // - 2 Chunk for versions : directChunkLong
      // - 4 Chunks for the actual records
      // * 2 Wrapping chunk
      // * 2 Content Chunk
      Assertions.assertThat(list)
          .containsExactlyInAnyOrder(
              // Version chunks
              new Object[] {0L, 0L, 256L, 2048L},
              new Object[] {0L, 246L, 256L, 2048L},
              // Content chunks
              // Chunk full (wrapper+data)
              new Object[] {0L, 0L, 256L, 0L},
              new Object[] {0L, 0L, 256L, 256L},
              // Partially full chunk (wrapper+data)
              new Object[] {0L, 246L, 256L, 0L},
              new Object[] {0L, 246L, 256L, 512L});
    }
  }

  @Test
  public void testChunkStructureFieldsWithFreedRows() {

    final ApplicationInTests<IInternalDatastore> monitoredApp = createMicroApplication();
    // Add 100 records
    monitoredApp.getDatabase().edit(tm -> IntStream.range(0, 100).forEach(i -> tm.add("A", i * i)));
    // Delete 10 records
    monitoredApp
        .getDatabase()
        .edit(
            tm ->
                IntStream.range(50, 50 + 20)
                    .forEach(
                        i -> {
                          try {
                            tm.remove("A", i * i);
                          } catch (NoTransactionException
                              | DatastoreTransactionException
                              | IllegalArgumentException
                              | NullPointerException e) {
                            throw new ActiveViamRuntimeException(e);
                          }
                        }));
    // Force to discard all versions
    monitoredApp.getDatabase().getEpochManager().forceDiscardEpochs(__ -> true);

    final IMemoryAnalysisService analysisService =
        createService(monitoredApp.getDatabase(), monitoredApp.getManager());
    final Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");

    final Collection<AMemoryStatistic> storeStats = loadDatastoreMemoryStatFromFolder(exportPath);
    // Start a monitoring datastore with the exported data
    final IInternalDatastore monitoringDatastore = createAnalysisDatastore();
    ATestMemoryStatistic.feedMonitoringApplication(monitoringDatastore, storeStats, "storeA");

    // Query record chunks data :
    ListQuery query =
        monitoringDatastore
            .getQueryManager()
            .listQuery()
            .forTable(CHUNK_STORE)
            .withCondition(
                BaseConditions.equal(
                    FieldPath.of(DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE),
                    MemoryAnalysisDatastoreDescriptionConfig.ParentType.RECORDS))
            .withAliasedFields(
                AliasedField.fromFieldName(DatastoreConstants.CHUNK__FREE_ROWS),
                AliasedField.fromFieldName(DatastoreConstants.CHUNK__NON_WRITTEN_ROWS),
                AliasedField.fromFieldName(DatastoreConstants.CHUNK__SIZE),
                AliasedField.fromFieldName(DatastoreConstants.CHUNK__OFF_HEAP_SIZE))
            .toQuery();

    try (final ICursor cursor =
        monitoringDatastore.getHead("master").getQueryRunner().listQuery(query).run()) {
      final List<Object[]> list = new ArrayList<>();
      cursor.forEach(
          (record) -> {
            Object[] data = record.toTuple();
            list.add(data);
          });
      // Expect 3 Chunks :
      // - 1 Chunk for versions : directChunkLong
      // - 2 Chunks for the actual records
      // * 1 Wrapping chunk
      // * 1 Content Chunk
      Assertions.assertThat(list)
          .containsExactlyInAnyOrder(
              // Version chunks
              new Object[] {0L, 156L, 256L, 2048L},
              // Content chunks
              // Partially full chunk (wrapper+data)
              new Object[] {20L, 156L, 256L, 0L},
              new Object[] {20L, 156L, 256L, 256L});
    }
  }

  @Test
  public void testLevelStoreContent() {

    final ApplicationInTests<?> monitoredApp = createMicroApplication();

    final IDatastore datastore = (IDatastore) monitoredApp.getDatabase();
    // Add 10 records
    datastore.edit(tm -> IntStream.range(0, 10).forEach(i -> tm.add("A", i * i * i)));

    // Force to discard all versions
    datastore.getEpochManager().forceDiscardEpochs(__ -> true);

    final IMemoryAnalysisService analysisService =
        createService(datastore, monitoredApp.getManager());
    final Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
    final Collection<AMemoryStatistic> pivotStats = loadPivotMemoryStatFromFolder(exportPath);

    // Make sure there is only one loaded store
    Assertions.assertThat(pivotStats).hasSize(1);

    // Start a monitoring datastore with the exported data
    final IInternalDatastore monitoringDatastore = createAnalysisDatastore();
    ATestMemoryStatistic.feedMonitoringApplication(monitoringDatastore, pivotStats, "Cube");

    // Query record of level data :
    ListQuery query =
        monitoringDatastore
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.CHUNK_TO_LEVEL_STORE)
            .withCondition(BaseConditions.TRUE)
            .withAliasedFields(
                AliasedField.fromFieldName(DatastoreConstants.CHUNK_TO_LEVEL__PARENT_ID))
            .toQuery();

    try (final ICursor cursor =
        monitoringDatastore.getHead("master").getQueryRunner().listQuery(query).run()) {
      final List<Object[]> list = new ArrayList<>();
      cursor.forEach(
          (record) -> {
            Object[] data = record.toTuple();
            list.add(data);
          });
      Assertions.assertThat(list).hasSize(1);
    }
  }

  @Test
  public void testMappingFromChunkToFields() {
    final ApplicationInTests<?> monitoredApp = createMicroApplication();
    final IDatastore datastore = (IDatastore) monitoredApp.getDatabase();
    final IMemoryAnalysisService analysisService =
        createService(datastore, monitoredApp.getManager());

    // Add 100 records
    datastore.edit(tm -> IntStream.range(0, 10).forEach(i -> tm.add("A", i * i)));
    // Initial Dump
    Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
    final AMemoryStatistic stats = loadMemoryStatFromFolder(exportPath);

    final IInternalDatastore monitoringDatastore = createAnalysisDatastore();
    ATestMemoryStatistic.feedMonitoringApplication(monitoringDatastore, List.of(stats), "appAInit");

    final List<AMemoryStatistic> dics =
        collectStatistics(
            stats,
            List.of(
                stat ->
                    stat.getName().equals(MemoryStatisticConstants.STAT_NAME_STORE)
                        && stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_STORE_NAME)
                            .asText()
                            .equals("A"),
                stat ->
                    stat.getName().equals(MemoryStatisticConstants.STAT_NAME_DICTIONARY_MANAGER),
                stat -> stat.getAttribute("field").asText().equals("id")));
    Assertions.assertThat(dics)
        .extracting(
            stat -> stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_DICTIONARY_ID).asLong())
        .are(identical());
    final var dicForFieldId =
        dics.get(0).getAttribute(MemoryStatisticConstants.ATTR_NAME_DICTIONARY_ID).asLong();

    final var dicChunks =
        dics.get(0)
            .accept(
                new AMemoryStatisticWithPredicate<Set<IMemoryStatistic>>(
                    ChunkStatistic.class::isInstance) {

                  final Set<IMemoryStatistic> collected = new HashSet<>();

                  @Override
                  protected Set<IMemoryStatistic> getResult() {
                    return this.collected;
                  }

                  @Override
                  protected boolean match(IMemoryStatistic stat) {
                    this.collected.add(stat);
                    return true;
                  }
                });
    final var chunkIds =
        dicChunks.stream()
            .map(stat -> stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_CHUNK_ID).asLong())
            .collect(toList());

    ListQuery query =
        monitoringDatastore
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.CHUNK_STORE)
            .withoutCondition()
            .withAliasedFields(
                AliasedField.fromFieldName(DatastoreConstants.CHUNK_ID),
                AliasedField.fromFieldName(DatastoreConstants.CHUNK__PARENT_DICO_ID))
            .toQuery();
    try (final ICursor cursor =
        monitoringDatastore.getHead("master").getQueryRunner().listQuery(query).run()) {
      cursor.forEach(
          (record) -> {
            final var chunkId = record.readLong(0);
            final var dictionaryId = record.readLong(1);
            if (chunkIds.contains(chunkId)) {
              Assertions.assertThat(dictionaryId)
                  .as("Dic for #" + chunkId)
                  .isEqualTo(dicForFieldId);
            } else {
              Assertions.assertThat(dictionaryId)
                  .as("Dic for #" + chunkId)
                  .isNotEqualTo(dicForFieldId);
            }
          });
    }
  }

  @Test
  public void testCompareDumpsOfDifferentEpochs() {
    final ApplicationInTests<?> monitoredApp = createMicroApplication();
    final IDatastore datastore = (IDatastore) monitoredApp.getDatabase();
    final IMemoryAnalysisService analysisService =
        createService(datastore, monitoredApp.getManager());

    // Add 100 records
    datastore.edit(tm -> IntStream.range(0, 100).forEach(i -> tm.add("A", i * i)));
    // Initial Dump
    Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
    final AMemoryStatistic stats = loadMemoryStatFromFolder(exportPath);
    // Delete 10 records
    datastore.edit(
        tm ->
            IntStream.range(50, 50 + 10)
                .forEach(
                    i -> {
                      try {
                        tm.remove("A", i * i);
                      } catch (NoTransactionException
                          | DatastoreTransactionException
                          | IllegalArgumentException
                          | NullPointerException e) {
                        throw new ActiveViamRuntimeException(e);
                      }
                    }));
    datastore.getEpochManager().forceDiscardEpochs(__ -> true);

    exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
    final AMemoryStatistic statsEpoch2 = loadMemoryStatFromFolder(exportPath);

    final IInternalDatastore monitoringDatastore = createAnalysisDatastore();
    monitoringDatastore.edit(
        tm -> {
          new AnalysisDatastoreFeeder("appAInit", monitoringDatastore)
              .loadWithTransaction(tm, Stream.of(stats));
          new AnalysisDatastoreFeeder("appAEpoch2", monitoringDatastore)
              .loadWithTransaction(tm, Stream.of(statsEpoch2));
        });

    // Verify that chunkIds are the same for the two dumps by checking that the Ids are there twice
    ListQuery firstQuery =
        monitoringDatastore
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.CHUNK_STORE)
            .withCondition(BaseConditions.TRUE)
            .withAliasedFields(
                AliasedField.fromFieldName(DatastoreConstants.CHUNK__DUMP_NAME),
                AliasedField.fromFieldName(DatastoreConstants.CHUNK_ID))
            .toQuery();
    try (final ICursor cursor =
        monitoringDatastore.getHead("master").getQueryRunner().listQuery(firstQuery).run()) {

      final SetMultimap<String, Long> chunksPerDump = HashMultimap.create();
      for (final IRecordReader reader : cursor) {
        chunksPerDump.put((String) reader.read(0), reader.readLong(1));
      }

      Assertions.assertThat(chunksPerDump.get("appAEpoch2"))
          .containsExactlyInAnyOrderElementsOf(chunksPerDump.get("appAInit"));
    }

    // Verify that the changes between dumps have been registered
    ListQuery secondQuery =
        monitoringDatastore
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.CHUNK_STORE)
            .withCondition(
                BaseConditions.equal(FieldPath.of(DatastoreConstants.CHUNK__DUMP_NAME), "appAInit"))
            .withAliasedFields(AliasedField.fromFieldName(DatastoreConstants.CHUNK__FREE_ROWS))
            .toQuery();

    try (final ICursor cursor2 =
        monitoringDatastore.getHead("master").getQueryRunner().listQuery(secondQuery).run()) {
      final List<Object[]> list2 = new ArrayList<>();
      cursor2.forEach(
          (record) -> {
            Object[] data = record.toTuple();
            list2.add(data);
          });
      Assertions.assertThat(list2).containsOnly(new Object[] {0L});
    }

    ListQuery thirdQuery =
        monitoringDatastore
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.CHUNK_STORE)
            .withCondition(
                BaseConditions.equal(
                    FieldPath.of(DatastoreConstants.CHUNK__DUMP_NAME), "appAEpoch2"))
            .withAliasedFields(AliasedField.fromFieldName(DatastoreConstants.CHUNK__FREE_ROWS))
            .toQuery();

    try (final ICursor cursor3 =
        monitoringDatastore.getHead("master").getQueryRunner().listQuery(thirdQuery).run()) {
      final List<Object[]> list3 = new ArrayList<>();
      cursor3.forEach(
          (record) -> {
            Object[] data = record.toTuple();
            list3.add(data);
          });
      Assertions.assertThat(list3).contains(new Object[] {10L});
    }
  }

  @Test
  public void testCompareDumpsOfDifferentApps() throws AgentException {
    // Create First App
    final ApplicationInTests<IInternalDatastore> monitoredApp = createMicroApplication();
    IInternalDatastore datastore = monitoredApp.getDatabase();
    final IMemoryAnalysisService analysisService =
        createService(datastore, monitoredApp.getManager());
    datastore.edit(tm -> IntStream.range(0, 100).forEach(i -> tm.add("A", i * i)));
    // Export first app
    Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
    final AMemoryStatistic stats = loadMemoryStatFromFolder(exportPath);
    // Create second app
    final ApplicationInTests<IDatastore> monitoredAppWithBitmap =
        createMicroApplicationWithLeafBitmap();
    final IMemoryAnalysisService analysisServiceWithBitmap =
        createService(monitoredAppWithBitmap.getDatabase(), monitoredAppWithBitmap.getManager());
    monitoredAppWithBitmap
        .getDatabase()
        .edit(tm -> IntStream.range(0, 100).forEach(i -> tm.add("A", i * i)));
    // Export second app
    exportPath = analysisServiceWithBitmap.exportMostRecentVersion("testLoadDatastoreStats");

    final AMemoryStatistic statsWithBitmap = loadMemoryStatFromFolder(exportPath);
    final IInternalDatastore monitoringDatastore = createAnalysisDatastore();
    monitoringDatastore.edit(
        tm -> {
          new AnalysisDatastoreFeeder("App", monitoringDatastore)
              .loadWithTransaction(tm, Stream.of(stats));
          new AnalysisDatastoreFeeder("AppWithBitmap", monitoringDatastore)
              .loadWithTransaction(tm, Stream.of(statsWithBitmap));
        });

    ListQuery firstQuery =
        monitoringDatastore
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.CHUNK_STORE)
            .withCondition(
                BaseConditions.and(
                    BaseConditions.equal(
                        FieldPath.of(DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE),
                        ParentType.AGGREGATE_STORE),
                    BaseConditions.equal(FieldPath.of(DatastoreConstants.CHUNK__DUMP_NAME), "App")))
            .withAliasedFields(AliasedField.fromFieldName(DatastoreConstants.CHUNK_ID))
            .toQuery();

    final ICursor cursor =
        monitoringDatastore.getHead("master").getQueryRunner().listQuery(firstQuery).run();
    final List<Object[]> list = new ArrayList<>();
    cursor.forEach(
        (record) -> {
          Object[] data = record.toTuple();
          list.add(data);
        });
    Assertions.assertThat(list).isEmpty();
    cursor.close();
    ListQuery secondQuery =
        monitoringDatastore
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.CHUNK_STORE)
            .withCondition(
                BaseConditions.and(
                    BaseConditions.equal(
                        FieldPath.of(DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE),
                        ParentType.AGGREGATE_STORE),
                    BaseConditions.equal(
                        FieldPath.of(DatastoreConstants.CHUNK__DUMP_NAME), "AppWithBitmap")))
            .withAliasedFields(AliasedField.fromFieldName(DatastoreConstants.CHUNK_ID))
            .toQuery();

    final ICursor cursor2 =
        monitoringDatastore.getHead("master").getQueryRunner().listQuery(secondQuery).run();
    final List<Object[]> list2 = new ArrayList<>();
    cursor2.forEach(
        (record) -> {
          Object[] data = record.toTuple();
          list2.add(data);
        });
    Assertions.assertThat(list2).isNotEmpty();
    cursor2.close();
  }

  @Test
  public void testChunkToReferencesDatastoreContentWithReference() throws AgentException {
    final ApplicationInTests<IDatastore> monitoredApp = createMicroApplicationWithReference();
    final IMemoryAnalysisService analysisService =
        createService(monitoredApp.getDatabase(), monitoredApp.getManager());

    monitoredApp
        .getDatabase()
        .edit(
            tm ->
                IntStream.range(0, 100)
                    .forEach(
                        i -> {
                          tm.add("A", i * i, i);
                          tm.add("B", i);
                        }));
    // Export first app
    Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
    final AMemoryStatistic stats = loadMemoryStatFromFolder(exportPath);
    final IInternalDatastore monitoringDatastore = createAnalysisDatastore();
    ATestMemoryStatistic.feedMonitoringApplication(monitoringDatastore, List.of(stats), "App");

    ListQuery query =
        monitoringDatastore
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.REFERENCE_STORE)
            .withCondition(BaseConditions.TRUE)
            .withAliasedFields(AliasedField.fromFieldName(DatastoreConstants.REFERENCE_ID))
            .toQuery();

    try (final ICursor cursor =
        monitoringDatastore.getHead("master").getQueryRunner().listQuery(query).run()) {
      final List<Object[]> list = new ArrayList<>();
      cursor.forEach(
          (record) -> {
            Object[] data = record.toTuple();
            list.add(data);
          });
      Assertions.assertThat(list).isNotEmpty();
    }
  }

  @Test
  public void testBlocksOfSingleVectors() {
    createApplicationWithVector(
        true,
        (monitoredDatastore, monitoredManager) -> {
          monitoredDatastore.edit(
              tm ->
                  IntStream.range(0, 24)
                      .forEach(
                          i ->
                              tm.add(
                                  VECTOR_STORE_NAME,
                                  i,
                                  IntStream.rangeClosed(1, 5).toArray(),
                                  IntStream.rangeClosed(10, 20).toArray(),
                                  LongStream.rangeClosed(3, 8).toArray())));

          monitoredDatastore.getEpochManager().forceDiscardEpochs(node -> true);

          final IMemoryAnalysisService analysisService =
              createService(monitoredDatastore, monitoredManager);
          final Path exportPath =
              analysisService.exportMostRecentVersion("testBlocksOfSingleVectors");
          final Collection<AMemoryStatistic> datastoreStats =
              loadDatastoreMemoryStatFromFolder(exportPath);

          final IDatastore monitoringDatastore = assertLoadsCorrectly(datastoreStats, getClass());

          // Test that we have chunks of single values
          final Set<String> recordTypes =
              retrieveClassesOfChunks(
                  monitoringDatastore,
                  retrieveChunksOfType(monitoringDatastore, ParentType.RECORDS));
          Assertions.assertThat(recordTypes).contains(ChunkSingleVector.class.getName());

          // Test that we have chunks of single values
          final Set<String> vectorTypes =
              retrieveClassesOfChunks(
                  monitoringDatastore,
                  retrieveChunksOfType(monitoringDatastore, ParentType.VECTOR_BLOCK));
          Assertions.assertThat(vectorTypes)
              .contains(
                  DirectIntegerVectorBlock.class.getName(), DirectLongVectorBlock.class.getName());
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

  private Set<String> retrieveClassesOfChunks(
      final IDatastore datastore, final Collection<Long> chunks) {
    ListQuery query =
        datastore
            .getQueryManager()
            .listQuery()
            .forTable(CHUNK_STORE)
            .withCondition(
                BaseConditions.in(FieldPath.of(DatastoreConstants.CHUNK_ID), chunks.toArray()))
            .withAliasedFields(AliasedField.fromFieldName(DatastoreConstants.CHUNK__CLASS))
            .toQuery();

    try (final ICursor cursor =
        datastore.getHead("master").getQueryRunner().listQuery(query).run()) {

      return StreamSupport.stream(cursor.spliterator(), false)
          .map(reader -> (String) reader.read(0))
          .collect(Collectors.toSet());
    }
  }

  // TODO Test content of all stores similarly

  private List<AMemoryStatistic> collectStatistics(
      final AMemoryStatistic root, final List<Predicate<AMemoryStatistic>> predicates) {
    return predicates.stream()
        .reduce(
            List.of(root),
            (result, predicate) ->
                result.stream()
                    .flatMap(stat -> stat.getChildren().stream())
                    .filter(predicate)
                    .collect(toList()),
            (a, b) -> {
              throw new UnsupportedOperationException();
            });
  }
}
