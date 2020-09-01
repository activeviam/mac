package com.activeviam.mac.statistic.memory;

import static com.activeviam.mac.memory.DatastoreConstants.CHUNK_STORE;
import static com.activeviam.mac.memory.DatastoreConstants.CHUNK__COMPONENT;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertArrayEquals;

import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription.ParentType;
import com.activeviam.mac.statistic.memory.descriptions.Application;
import com.activeviam.mac.statistic.memory.descriptions.FullApplicationWithVectors;
import com.activeviam.mac.statistic.memory.descriptions.MicroApplication;
import com.activeviam.mac.statistic.memory.descriptions.MicroApplicationWithReference;
import com.activeviam.mac.statistic.memory.descriptions.MinimalApplication;
import com.activeviam.mac.statistic.memory.visitor.impl.FeedVisitor;
import com.qfs.assertj.QfsConditions;
import com.qfs.chunk.impl.ChunkSingleVector;
import com.qfs.condition.impl.BaseConditions;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.dic.IDictionary;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;
import com.qfs.monitoring.statistic.memory.impl.ChunkStatistic;
import com.qfs.monitoring.statistic.memory.visitor.impl.AMemoryStatisticWithPredicate;
import com.qfs.pivot.monitoring.impl.MemoryAnalysisService;
import com.qfs.store.IDatastore;
import com.qfs.store.NoTransactionException;
import com.qfs.store.impl.Datastore;
import com.qfs.store.query.ICompiledGetByKey;
import com.qfs.store.query.IDictionaryCursor;
import com.qfs.store.query.impl.CompiledGetByKey;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.qfs.vector.direct.impl.DirectIntegerVectorBlock;
import com.qfs.vector.direct.impl.DirectLongVectorBlock;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.QuartetRuntimeException;
import com.quartetfs.fwk.query.QueryException;
import com.quartetfs.fwk.query.UnsupportedQueryException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

public class TestMemoryMonitoringDatastoreContent extends AMonitoringTest {

  /**
   * Tests the consistency between the chunks of an ActivePivot application and its the monitoring
   * data obtained by loading exported data.
   */
  @Test
  public void testDatastoreMonitoringValues() throws AgentException, IOException {
    final IDatastoreSchemaDescription datastoreSchemaDescription =
        MinimalApplication.datastoreDescription();
    final IActivePivotManagerDescription managerDescription =
        MinimalApplication.managerDescription(datastoreSchemaDescription);

    final ExportedApplication monitoredApplication =
        setupAndExportMonitoredApplication(datastoreSchemaDescription, managerDescription,
            (monitoredDatastore, monitoredManager) -> {
              MinimalApplication.fillApplication(monitoredDatastore);
            });
    final IDatastore monitoredDatastore = monitoredApplication.monitoredDatastore;

    setupMac();
    final Collection<IMemoryStatistic> statistics =
        loadAndImportStatistics(monitoredApplication.statisticsPath);

    IDictionary<Object> dic =
        monitoredDatastore.getDictionaries().getDictionary("Sales", "id");

    final long[] chunkIds = new long[2];
    final long[] chunkSizes = new long[2];

    AtomicInteger rnk = new AtomicInteger();
    final long[] monitoredChunkSizes = new long[2];

    // Check all the chunks held by the Dictionary of that key field of the store
    IMemoryStatistic stat = dic.getMemoryStatistic();

    stat.getChildren()
        .forEach(
            (c_st) -> {
              c_st.getChildren()
                  .forEach(
                      (c_c_st) -> {
                        c_c_st
                            .getChildren()
                            .forEach(
                                (c_c_c_st) -> {
                                  chunkSizes[rnk.get()] =
                                      c_c_c_st.getAttribute("length").asLong();
                                  chunkIds[rnk.get()] =
                                      c_c_c_st.getAttribute("chunkId").asLong();
                                  rnk.getAndIncrement();
                                });
                      });
            });

    monitoringDatastore.edit(
        tm -> {
          statistics.forEach(statistic -> {
            statistic.accept(
                new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "test"));

            final ICompiledGetByKey query =
                new CompiledGetByKey(
                    ((Datastore) monitoringDatastore).getSchema(),
                    monitoringDatastore
                        .getSchemaMetadata()
                        .getStoreMetadata(DatastoreConstants.CHUNK_STORE),
                    ((Datastore) monitoringDatastore)
                        .getSchema()
                        .getStore(DatastoreConstants.CHUNK_STORE)
                        .getDictionaryProvider(),
                    0,
                    Arrays.asList(
                        DatastoreConstants.CHUNK__OWNER,
                        DatastoreConstants.CHUNK__PARENT_ID,
                        DatastoreConstants.CHUNK__CLASS,
                        DatastoreConstants.CHUNK__SIZE));
            Object keys[] = new Object[2];

            int chunkDumpNameFieldIdx =
                ((Datastore) monitoringDatastore)
                    .getSchema()
                    .getStore(DatastoreConstants.CHUNK_STORE)
                    .getFieldIndex(DatastoreConstants.CHUNK__DUMP_NAME);

            for (int i = 0; i < chunkIds.length; i++) {
              keys[0] = chunkIds[i];
              keys[1] =
                  ((Datastore) monitoringDatastore)
                      .getSchema()
                      .getStore(DatastoreConstants.CHUNK_STORE)
                      .getDictionaryProvider()
                      .getDictionary(chunkDumpNameFieldIdx)
                      .read(1);

              Object[] top_obj = query.runInTransaction(keys, true).toTuple();
              monitoredChunkSizes[i] = Long.valueOf(top_obj[3].toString());
            }
          });
          // Now we verify the monitored chunks and the chunk in the Datastore of the Monitoring
          // Cube are identical
          assertArrayEquals(chunkSizes, monitoredChunkSizes);
        });
  }

  @Test
  public void testChunkStructureFieldsWithSingleRecord() throws IOException, AgentException {
    final IDatastoreSchemaDescription datastoreSchemaDescription =
        MicroApplication.datastoreDescription();
    final IActivePivotManagerDescription managerDescription =
        Application.cubelessManagerDescription();

    final ExportedApplication monitoredApplication =
        setupAndExportMonitoredApplication(datastoreSchemaDescription, managerDescription,
            (monitoredDatastore, monitoredManager) -> {
              monitoredDatastore.edit(
                  tm -> {
                    IntStream.range(0, 1)
                        .forEach(
                            i -> {
                              tm.add("A", i * i * 100000000);
                            });
                  });
              monitoredDatastore.getEpochManager().forceDiscardEpochs(__ -> true);
            });

    setupMac();
    final Collection<IMemoryStatistic> statistics =
        loadAndImportStatistics(monitoredApplication.statisticsPath);

    // Make sure there is only one loaded store
    Assert.assertEquals(1, statistics.size());

    // Query record chunks data :
    final IDictionaryCursor cursor =
        monitoringDatastore
            .getHead()
            .getQueryRunner()
            .forStore(CHUNK_STORE)
            .withCondition(
                BaseConditions.Equal(
                    DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE,
                    MemoryAnalysisDatastoreDescription.ParentType.RECORDS))
            .selecting(
                DatastoreConstants.CHUNK__FREE_ROWS,
                DatastoreConstants.CHUNK__NON_WRITTEN_ROWS,
                DatastoreConstants.CHUNK__SIZE,
                DatastoreConstants.CHUNK__OFF_HEAP_SIZE)
            .onCurrentThread()
            .run();
    final List<Object[]> list = new ArrayList<>();
    cursor.forEach(
        (record) -> {
          Object[] data = record.toTuple();
          list.add(data);
        });

    // Expect 3 Chunks :
    // - 1 Chunk for versions : directChunkLong
    // - 2 Chunks for the actual records
    //         * 1 Wrapper chunk (DirectChunkPositiveInteger) -> Verify that the offHeap size equal
    // to Zero
    //         * 1 Content Chunk (ChunkSingleInteger) -> Verify that no data is stored offheap
    Assertions.assertThat(list.size()).isEqualTo(3);
    Assertions.assertThat(list)
        .containsExactly(
            new Object[] {0L, 255L, 256L, 0L},
            new Object[] {0L, 255L, 256L, 0L},
            new Object[] {0L, 255L, 256L, 2048L});
  }

  @Test
  public void testChunkStructureFieldsWithFullChunk() throws IOException, AgentException {
    final IDatastoreSchemaDescription datastoreSchemaDescription =
        MicroApplication.datastoreDescription();
    final IActivePivotManagerDescription managerDescription =
        Application.cubelessManagerDescription();

    final ExportedApplication monitoredApplication =
        setupAndExportMonitoredApplication(datastoreSchemaDescription, managerDescription,
            (monitoredDatastore, monitoredManager) -> {
              monitoredDatastore.edit(
                  tm -> {
                    // Add a full chunk
                    IntStream.range(0, 256)
                        .forEach(
                            i -> {
                              tm.add("A", i * i * i);
                            });
                  });
              monitoredDatastore.getEpochManager().forceDiscardEpochs(__ -> true);
            });

    setupMac();
    final Collection<IMemoryStatistic> statistics =
        loadAndImportStatistics(monitoredApplication.statisticsPath);

    // Make sure there is only one loaded store
    Assert.assertEquals(1, statistics.size());

    // Query record chunks data :
    final IDictionaryCursor cursor =
        monitoringDatastore
            .getHead()
            .getQueryRunner()
            .forStore(CHUNK_STORE)
            .withCondition(
                BaseConditions.Equal(
                    DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE,
                    MemoryAnalysisDatastoreDescription.ParentType.RECORDS))
            .selecting(
                DatastoreConstants.CHUNK__FREE_ROWS,
                DatastoreConstants.CHUNK__NON_WRITTEN_ROWS,
                DatastoreConstants.CHUNK__SIZE,
                DatastoreConstants.CHUNK__OFF_HEAP_SIZE)
            .onCurrentThread()
            .run();
    final List<Object[]> list = new ArrayList<>();
    cursor.forEach(
        (record) -> {
          Object[] data = record.toTuple();
          list.add(data);
        });
    // Expect 2 Chunks :
    // - 1 Chunk for versions : directChunkLong
    // - 1 Chunks for the actual records (ChunkShorts) -> Verify that 256 bytes of data is stored
    // offheap
    Assertions.assertThat(list.size()).isEqualTo(2);
    Assertions.assertThat(list)
        .containsExactly(new Object[] {0L, 0L, 256L, 256L}, new Object[] {0L, 0L, 256L, 2048L});
  }

  @Test
  public void testChunkStructureFieldsWithTwoChunks() throws IOException, AgentException {
    final IDatastoreSchemaDescription datastoreSchemaDescription =
        MicroApplication.datastoreDescription();
    final IActivePivotManagerDescription managerDescription =
        Application.cubelessManagerDescription();

    final ExportedApplication monitoredApplication =
        setupAndExportMonitoredApplication(datastoreSchemaDescription, managerDescription,
            (monitoredDatastore, monitoredManager) -> {
              monitoredDatastore.edit(
                  tm -> {
                    // Add a full chunk + 10 records on the second chunk
                    IntStream.range(0, 266)
                        .forEach(
                            i -> {
                              tm.add("A", i * i * i);
                            });
                  });
              monitoredDatastore.getEpochManager().forceDiscardEpochs(__ -> true);
            });

    setupMac();
    final Collection<IMemoryStatistic> statistics =
        loadAndImportStatistics(monitoredApplication.statisticsPath);

    // Make sure there is only one loaded store
    Assert.assertEquals(1, statistics.size());

    // Query record chunks data :
    final IDictionaryCursor cursor =
        monitoringDatastore
            .getHead()
            .getQueryRunner()
            .forStore(CHUNK_STORE)
            .withCondition(
                BaseConditions.Equal(
                    DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE,
                    MemoryAnalysisDatastoreDescription.ParentType.RECORDS))
            .selecting(
                DatastoreConstants.CHUNK__FREE_ROWS,
                DatastoreConstants.CHUNK__NON_WRITTEN_ROWS,
                DatastoreConstants.CHUNK__SIZE,
                DatastoreConstants.CHUNK__OFF_HEAP_SIZE)
            .onCurrentThread()
            .run();
    final List<Object[]> list = new ArrayList<>();
    cursor.forEach(
        (record) -> {
          Object[] data = record.toTuple();
          list.add(data);
        });
    // Expect 5 Chunks :
    // - 2 Chunk for versions : directChunkLong
    // - 3 Chunks for the actual records
    //		   * 1 Wrapping chunk
    //         * 2 Content Chunk
    Assertions.assertThat(list.size()).isEqualTo(5);
    Assertions.assertThat(list)
        .containsExactlyInAnyOrder(
            // Version chunks
            new Object[] {0L, 0L, 256L, 2048L},
            new Object[] {0L, 246L, 256L, 2048L},
            // Content chunks
            // Chunk full (not wrapped)
            new Object[] {0L, 0L, 256L, 256L},
            // Partially full chunk (wrapper+data)
            new Object[] {0L, 246L, 256L, 0L},
            new Object[] {0L, 246L, 256L, 512L});
  }

  @Test
  public void testChunkStructureFieldsWithFreedRows() throws IOException, AgentException {
    final IDatastoreSchemaDescription datastoreSchemaDescription =
        MicroApplication.datastoreDescription();
    final IActivePivotManagerDescription managerDescription =
        Application.cubelessManagerDescription();

    final ExportedApplication monitoredApplication =
        setupAndExportMonitoredApplication(datastoreSchemaDescription, managerDescription,
            (monitoredDatastore, monitoredManager) -> {
              // Add 100 records
              monitoredDatastore.edit(
                  tm -> {
                    IntStream.range(0, 100)
                        .forEach(
                            i -> {
                              tm.add("A", i * i);
                            });
                  });
              // Delete 10 records
              monitoredDatastore.edit(
                  tm -> {
                    IntStream.range(50, 50 + 20)
                        .forEach(
                            i -> {
                              try {
                                tm.remove("A", i * i);
                              } catch (NoTransactionException
                                  | DatastoreTransactionException
                                  | IllegalArgumentException
                                  | NullPointerException e) {
                                throw new QuartetRuntimeException(e);
                              }
                            });
                  });
              monitoredDatastore.getEpochManager().forceDiscardEpochs(__ -> true);
            });

    setupMac();
    loadAndImportStatistics(monitoredApplication.statisticsPath);

    // Query record chunks data :
    final IDictionaryCursor cursor =
        monitoringDatastore
            .getHead()
            .getQueryRunner()
            .forStore(CHUNK_STORE)
            .withCondition(
                BaseConditions.Equal(
                    DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE,
                    MemoryAnalysisDatastoreDescription.ParentType.RECORDS))
            .selecting(
                DatastoreConstants.CHUNK__FREE_ROWS,
                DatastoreConstants.CHUNK__NON_WRITTEN_ROWS,
                DatastoreConstants.CHUNK__SIZE,
                DatastoreConstants.CHUNK__OFF_HEAP_SIZE)
            .onCurrentThread()
            .run();
    final List<Object[]> list = new ArrayList<>();
    cursor.forEach(
        (record) -> {
          Object[] data = record.toTuple();
          list.add(data);
        });
    // Expect 3 Chunks :
    // - 1 Chunk for versions : directChunkLong
    // - 2 Chunks for the actual records
    //		   * 1 Wrapping chunk
    //         * 1 Content Chunk
    Assertions.assertThat(list.size()).isEqualTo(3);
    Assertions.assertThat(list)
        .containsExactlyInAnyOrder(
            // Version chunks
            new Object[] {0L, 156L, 256L, 2048L},
            // Content chunks
            // Partially full chunk (wrapper+data)
            new Object[] {20L, 156L, 256L, 0L},
            new Object[] {20L, 156L, 256L, 256L});
  }

  @Test
  public void testLevelStoreContent()
      throws AgentException, IOException, UnsupportedQueryException, QueryException {

    final IDatastoreSchemaDescription datastoreSchemaDescription =
        MicroApplication.datastoreDescription();
    final IActivePivotManagerDescription managerDescription =
        MicroApplication.managerDescription(datastoreSchemaDescription);

    final ExportedApplication monitoredApplication =
        setupAndExportMonitoredApplication(datastoreSchemaDescription, managerDescription,
            (monitoredDatastore, monitoredManager) -> {
              monitoredDatastore.edit(
                  tm -> {
                    IntStream.range(0, 10)
                        .forEach(
                            i -> {
                              tm.add("A", i * i * i);
                            });
                  });
              monitoredDatastore.getEpochManager().forceDiscardEpochs(__ -> true);
            });

    setupMac();
    final Collection<IMemoryStatistic> statistics =
        loadAndImportStatistics(monitoredApplication.statisticsPath, path ->
            path.getFileName().toString().startsWith(MemoryAnalysisService.PIVOT_FILE_PREFIX));

    // Make sure there is only one loaded store
    Assert.assertEquals(1, statistics.size());

    // Query record of level data :
    final IDictionaryCursor cursor =
        monitoringDatastore
            .getHead()
            .getQueryRunner()
            .forStore(DatastoreConstants.CHUNK_TO_LEVEL_STORE)
            .withCondition(BaseConditions.TRUE)
            .selecting(DatastoreConstants.CHUNK_TO_LEVEL__PARENT_ID)
            .onCurrentThread()
            .run();
    final List<Object[]> list = new ArrayList<>();
    cursor.forEach(
        (record) -> {
          Object[] data = record.toTuple();
          list.add(data);
        });
    Assertions.assertThat(list.size()).isEqualTo(1);
  }

  @Test
  public void testMappingFromChunkToFields() throws AgentException, IOException {
    final IDatastoreSchemaDescription datastoreSchemaDescription =
        MicroApplication.datastoreDescription();
    final IActivePivotManagerDescription managerDescription =
        Application.cubelessManagerDescription();

    final ExportedApplication monitoredApplication =
        setupAndExportMonitoredApplication(datastoreSchemaDescription, managerDescription,
            (monitoredDatastore, monitoredManager) -> {
              monitoredDatastore.edit(
                  tm -> {
                    IntStream.range(0, 10)
                        .forEach(
                            i -> {
                              tm.add("A", i * i);
                            });
                  });
              monitoredDatastore.getEpochManager().forceDiscardEpochs(__ -> true);
            });

    setupMac();
    final Collection<IMemoryStatistic> statistics =
        loadAndImportStatistics(monitoredApplication.statisticsPath);

    final List<IMemoryStatistic> dics =
        collectStatistics(
            aggregateMemoryStatistics(statistics),
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
        .are(QfsConditions.identical());
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

    final IDictionaryCursor cursor =
        monitoringDatastore
            .getHead()
            .getQueryRunner()
            .forStore(DatastoreConstants.CHUNK_STORE)
            .withoutCondition()
            .selecting(DatastoreConstants.CHUNK_ID, DatastoreConstants.CHUNK__PARENT_DICO_ID)
            .onCurrentThread()
            .run();
    cursor.forEach(
        (record) -> {
          final var chunkId = record.readLong(0);
          final var dictionaryId = record.readLong(1);
          if (chunkIds.contains(chunkId)) {
            Assertions.assertThat(dictionaryId).as("Dic for #" + chunkId).isEqualTo(dicForFieldId);
          } else {
            Assertions.assertThat(dictionaryId)
                .as("Dic for #" + chunkId)
                .isNotEqualTo(dicForFieldId);
          }
        });
  }

  @Test
  public void testCompareDumpsOfDifferentEpochs() throws AgentException, IOException {
    final IDatastoreSchemaDescription datastoreSchemaDescription =
        MicroApplication.datastoreDescription();
    final IActivePivotManagerDescription managerDescription =
        Application.cubelessManagerDescription();

    final ExportedApplication monitoredApplication =
        setupAndExportMonitoredApplication(datastoreSchemaDescription, managerDescription,
            (monitoredDatastore, monitoredManager) -> {
              monitoredDatastore.edit(
                  tm -> {
                    IntStream.range(0, 100)
                        .forEach(
                            i -> {
                              tm.add("A", i * i);
                            });
                  });
              monitoredDatastore.getEpochManager().forceDiscardEpochs(__ -> true);
            });
    final IDatastore monitoredDatastore = monitoredApplication.monitoredDatastore;
    final IActivePivotManager monitoredManager = monitoredApplication.monitoredManager;

    setupMac();

    // Initial Dump
    Path exportPath =
        exportApplicationMemoryStatistics(monitoredDatastore, monitoredManager, getTempDirectory(),
            "initialDump");
    final Collection<IMemoryStatistic> stats = loadMemoryStatistics(exportPath);

    // Delete 10 records
    monitoredDatastore.edit(
        tm -> {
          IntStream.range(50, 50 + 10)
              .forEach(
                  i -> {
                    try {
                      tm.remove("A", i * i);
                    } catch (NoTransactionException
                        | DatastoreTransactionException
                        | IllegalArgumentException
                        | NullPointerException e) {
                      throw new QuartetRuntimeException(e);
                    }
                  });
        });
    monitoredDatastore.getEpochManager().forceDiscardEpochs(__ -> true);

    exportPath =
        exportApplicationMemoryStatistics(monitoredDatastore, monitoredManager, getTempDirectory());
    final Collection<IMemoryStatistic> statsEpoch2 = loadMemoryStatistics(exportPath);

    feedStatisticsIntoDatastore(stats, monitoringDatastore, "appAInit");
    feedStatisticsIntoDatastore(statsEpoch2, monitoringDatastore, "appAEpoch2");

    // Verify that chunkIds are the same for the two dumps by checking that the Ids are there twice
    final IDictionaryCursor cursor =
        monitoringDatastore
            .getHead()
            .getQueryRunner()
            .forStore(DatastoreConstants.CHUNK_STORE)
            .withCondition(BaseConditions.TRUE)
            .selecting(DatastoreConstants.CHUNK_ID)
            .onCurrentThread()
            .run();
    final List<Object> list = new ArrayList<>();
    cursor.forEach(
        (record) -> {
          Object[] data = record.toTuple();
          list.add(data[0]);
        });
    Assertions.assertThat(list).allMatch(o -> Collections.frequency(list, o) == 2);
    // Verify that the changes between dumps have been registered
    final IDictionaryCursor cursor2 =
        monitoringDatastore
            .getHead()
            .getQueryRunner()
            .forStore(DatastoreConstants.CHUNK_STORE)
            .withCondition(BaseConditions.Equal(DatastoreConstants.CHUNK__DUMP_NAME, "appAInit"))
            .selecting(DatastoreConstants.CHUNK__FREE_ROWS)
            .onCurrentThread()
            .run();
    final List<Object[]> list2 = new ArrayList<>();
    cursor2.forEach(
        (record) -> {
          Object[] data = record.toTuple();
          list2.add(data);
        });
    Assertions.assertThat(list2).containsOnly(new Object[] {0L});

    final IDictionaryCursor cursor3 =
        monitoringDatastore
            .getHead()
            .getQueryRunner()
            .forStore(DatastoreConstants.CHUNK_STORE)
            .withCondition(BaseConditions.Equal(DatastoreConstants.CHUNK__DUMP_NAME, "appAEpoch2"))
            .selecting(DatastoreConstants.CHUNK__FREE_ROWS)
            .onCurrentThread()
            .run();
    final List<Object[]> list3 = new ArrayList<>();
    cursor3.forEach(
        (record) -> {
          Object[] data = record.toTuple();
          list3.add(data);
        });
    Assertions.assertThat(list3).contains(new Object[] {10L});
  }

  @Test
  public void testCompareDumpsOfDifferentApps() throws AgentException, IOException {
    // Create First App
    IDatastoreSchemaDescription datastoreSchemaDescription =
        MicroApplication.datastoreDescription();
    IActivePivotManagerDescription managerDescription =
        Application.cubelessManagerDescription();

    // Export first app
    ExportedApplication monitoredApplication =
        setupAndExportMonitoredApplication(datastoreSchemaDescription, managerDescription,
            (monitoredDatastore, monitoredManager) -> {
              monitoredDatastore.edit(
                  tm -> {
                    IntStream.range(0, 100)
                        .forEach(
                            i -> {
                              tm.add("A", i * i);
                            });
                  });
            },
            "testLoadDatastoreStats");
    final Collection<IMemoryStatistic> stats =
        loadMemoryStatistics(monitoredApplication.statisticsPath);

    // Create second app
    datastoreSchemaDescription =
        MicroApplication.datastoreDescription();
    managerDescription =
        MicroApplication.managerDescriptionWithLeafBitmap(datastoreSchemaDescription);

    // Export second app
    monitoredApplication =
        setupAndExportMonitoredApplication(datastoreSchemaDescription, managerDescription,
            (monitoredDatastore, monitoredManager) -> {
              monitoredDatastore.edit(
                  tm -> {
                    IntStream.range(0, 100)
                        .forEach(
                            i -> {
                              tm.add("A", i * i);
                            });
                  });
            },
            "testLoadDatastoreStats");
    final Collection<IMemoryStatistic> statsWithBitmap =
        loadMemoryStatistics(monitoredApplication.statisticsPath);

    setupMac();
    feedStatisticsIntoDatastore(stats, monitoringDatastore, "App");
    feedStatisticsIntoDatastore(statsWithBitmap, monitoringDatastore, "AppWithBitmap");

    final IDictionaryCursor cursor =
        monitoringDatastore
            .getHead()
            .getQueryRunner()
            .forStore(DatastoreConstants.CHUNK_STORE)
            .withCondition(
                BaseConditions.And(
                    BaseConditions.Equal(
                        DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE, ParentType.AGGREGATE_STORE),
                    BaseConditions.Equal(DatastoreConstants.CHUNK__DUMP_NAME, "App")))
            .selecting(DatastoreConstants.CHUNK_ID)
            .onCurrentThread()
            .run();
    final List<Object[]> list = new ArrayList<>();
    cursor.forEach(
        (record) -> {
          Object[] data = record.toTuple();
          list.add(data);
        });
    Assertions.assertThat(list).isEmpty();
    final IDictionaryCursor cursor2 =
        monitoringDatastore
            .getHead()
            .getQueryRunner()
            .forStore(DatastoreConstants.CHUNK_STORE)
            .withCondition(
                BaseConditions.And(
                    BaseConditions.Equal(
                        DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE, ParentType.AGGREGATE_STORE),
                    BaseConditions.Equal(DatastoreConstants.CHUNK__DUMP_NAME, "AppWithBitmap")))
            .selecting(DatastoreConstants.CHUNK_ID)
            .onCurrentThread()
            .run();
    final List<Object[]> list2 = new ArrayList<>();
    cursor2.forEach(
        (record) -> {
          Object[] data = record.toTuple();
          list2.add(data);
        });
    Assertions.assertThat(list2).isNotEmpty();
  }

  @Test
  public void testChunkToReferencesDatastoreContentWithReference()
      throws AgentException, IOException {
    final IDatastoreSchemaDescription datastoreSchemaDescription =
        MicroApplicationWithReference.datastoreDescription();
    final IActivePivotManagerDescription managerDescription =
        Application.cubelessManagerDescription();

    final ExportedApplication monitoredApplication =
        setupAndExportMonitoredApplication(datastoreSchemaDescription, managerDescription,
            (monitoredDatastore, monitoredManager) -> {
              monitoredDatastore.edit(
                  tm -> {
                    IntStream.range(0, 100)
                        .forEach(
                            i -> {
                              tm.add("A", i * i, i);
                              tm.add("B", i);
                            });
                  });
            });

    setupMac();
    loadAndImportStatistics(monitoredApplication.statisticsPath);

    final IDictionaryCursor cursor =
        monitoringDatastore
            .getHead()
            .getQueryRunner()
            .forStore(DatastoreConstants.REFERENCE_STORE)
            .withCondition(BaseConditions.TRUE)
            .selecting(DatastoreConstants.REFERENCE_ID)
            .onCurrentThread()
            .run();
    final List<Object[]> list = new ArrayList<>();
    cursor.forEach(
        (record) -> {
          Object[] data = record.toTuple();
          list.add(data);
        });
    Assertions.assertThat(list).isNotEmpty();
  }

  @Test
  public void testBlocksOfSingleVectors() throws AgentException, IOException {
    final IDatastoreSchemaDescription datastoreSchemaDescription =
        FullApplicationWithVectors.datastoreDescription();
    final IActivePivotManagerDescription managerDescription =
        Application.cubelessManagerDescription();

    final ExportedApplication monitoredApplication =
        setupAndExportMonitoredApplication(datastoreSchemaDescription, managerDescription,
            (monitoredDatastore, monitoredManager) -> {
              monitoredDatastore.edit(
                  tm -> {
                    IntStream.range(0, 24)
                        .forEach(
                            i -> {
                              tm.add(
                                  FullApplicationWithVectors.VECTOR_STORE_NAME,
                                  i,
                                  IntStream.rangeClosed(1, 5).toArray(),
                              IntStream.rangeClosed(10, 20).toArray(),
                              LongStream.rangeClosed(3, 8).toArray());
                        });
              });
        });

    setupMac();
    final Collection<IMemoryStatistic> statistics =
        loadAndImportStatistics(monitoredApplication.statisticsPath);

    assertStatisticsConsistency(statistics);

    // Test that we have chunks of single values
    final IDictionaryCursor recordCursor =
        monitoringDatastore
            .getHead()
            .getQueryRunner()
            .forStore(CHUNK_STORE)
            .withCondition(BaseConditions.Equal(CHUNK__COMPONENT, ParentType.RECORDS))
            .selecting(DatastoreConstants.CHUNK__CLASS)
            .onCurrentThread()
            .run();
    final Set<String> recordTypes =
        StreamSupport.stream(recordCursor.spliterator(), false)
            .map(record -> (String) record.read(0))
            .collect(Collectors.toSet());
    Assertions.assertThat(recordTypes).contains(ChunkSingleVector.class.getName());

    // Test that we have chunks of single values
    final IDictionaryCursor vectorCursor =
        monitoringDatastore
            .getHead()
            .getQueryRunner()
            .forStore(CHUNK_STORE)
            .withCondition(BaseConditions.Equal(CHUNK__COMPONENT, ParentType.VECTOR_BLOCK))
            .selecting(DatastoreConstants.CHUNK__CLASS)
            .onCurrentThread()
            .run();
    final Set<String> vectorTypes =
        StreamSupport.stream(vectorCursor.spliterator(), false)
            .map(record -> (String) record.read(0))
            .collect(Collectors.toSet());
    Assertions.assertThat(vectorTypes)
        .contains(
            DirectIntegerVectorBlock.class.getName(), DirectLongVectorBlock.class.getName());
  }

  // TODO Test content of all stores similarly

  private List<IMemoryStatistic> collectStatistics(
      final IMemoryStatistic root, final List<Predicate<IMemoryStatistic>> predicates) {
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
