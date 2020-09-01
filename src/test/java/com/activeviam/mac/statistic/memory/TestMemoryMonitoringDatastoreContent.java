package com.activeviam.mac.statistic.memory;

import static com.activeviam.mac.memory.DatastoreConstants.CHUNK_STORE;
import static com.activeviam.mac.memory.DatastoreConstants.CHUNK__COMPONENT;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertArrayEquals;

import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription.ParentType;
import com.activeviam.mac.statistic.memory.visitor.impl.FeedVisitor;
import com.qfs.assertj.QfsConditions;
import com.qfs.chunk.impl.ChunkSingleVector;
import com.qfs.condition.impl.BaseConditions;
import com.qfs.dic.IDictionary;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;
import com.qfs.monitoring.statistic.memory.impl.ChunkStatistic;
import com.qfs.monitoring.statistic.memory.visitor.impl.AMemoryStatisticWithPredicate;
import com.qfs.service.monitoring.IMemoryAnalysisService;
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
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.QuartetRuntimeException;
import com.quartetfs.fwk.impl.Pair;
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
import org.junit.Test;

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
              ATestMemoryStatistic.createService(monitoredDatastore, monitoredManager);
          final Path exportPath = analysisService.exportApplication("testLoadComplete");

          final IMemoryStatistic fullStats =
              ATestMemoryStatistic.loadMemoryStatFromFolder(exportPath);
          final Datastore monitoringDatastore =
              (Datastore) ATestMemoryStatistic.createAnalysisDatastore();

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
                fullStats.accept(
                    new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "test"));

                final ICompiledGetByKey query =
                    new CompiledGetByKey(
                        monitoringDatastore.getSchema(),
                        monitoringDatastore
                            .getSchemaMetadata()
                            .getStoreMetadata(DatastoreConstants.CHUNK_STORE),
                        monitoringDatastore
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
                    monitoringDatastore
                        .getSchema()
                        .getStore(DatastoreConstants.CHUNK_STORE)
                        .getFieldIndex(DatastoreConstants.CHUNK__DUMP_NAME);

                for (int i = 0; i < chunkIds.length; i++) {
                  keys[0] = chunkIds[i];
                  keys[1] =
                      monitoringDatastore
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
    final Pair<IDatastore, IActivePivotManager> monitoredApp = createMicroApplicationWithNoCube();

    // Add a single record
    monitoredApp
        .getLeft()
        .edit(
            tm -> {
              IntStream.range(0, 1)
                  .forEach(
                      i -> {
                        tm.add("A", i * i * 100000000);
                      });
            });

    // Force to discard all versions
    monitoredApp.getLeft().getEpochManager().forceDiscardEpochs(__ -> true);

    final IMemoryAnalysisService analysisService =
        createService(monitoredApp.getLeft(), monitoredApp.getRight());
    final Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
    final Collection<IMemoryStatistic> storeStats = loadDatastoreMemoryStatFromFolder(exportPath);

    // Make sure there is only one loaded store
    Assert.assertEquals(1, storeStats.size());

    // Start a monitoring datastore with the exported data
    final IDatastore monitoringDatastore = createAnalysisDatastore();
    monitoringDatastore.edit(
        tm -> {
          storeStats.forEach(
              (stats) -> {
                stats.accept(
                    new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "storeA"));
              });
        });
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
    final Pair<IDatastore, IActivePivotManager> monitoredApp = createMicroApplicationWithNoCube();

    // Add a full chunk
    monitoredApp
        .getLeft()
        .edit(
            tm -> {
              IntStream.range(0, 256)
                  .forEach(
                      i -> {
                        tm.add("A", i * i * i);
                      });
            });

    // Force to discard all versions
    monitoredApp.getLeft().getEpochManager().forceDiscardEpochs(__ -> true);
    final int storeAIdx = monitoredApp.getLeft().getSchemaMetadata().getStoreId("A");

    final IMemoryAnalysisService analysisService =
        createService(monitoredApp.getLeft(), monitoredApp.getRight());
    final Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
    final Collection<IMemoryStatistic> storeStats = loadDatastoreMemoryStatFromFolder(exportPath);

    // Make sure there is only one loaded store
    Assert.assertEquals(1, storeStats.size());

    // Start a monitoring datastore with the exported data
    final IDatastore monitoringDatastore = createAnalysisDatastore();

    storeStats.forEach(
        (stats) -> {
          monitoringDatastore.edit(
              tm -> {
                stats.accept(
                    new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "storeA"));
              });
        });

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
    final Pair<IDatastore, IActivePivotManager> monitoredApp = createMicroApplicationWithNoCube();

    // Add a full chunk + 10 records on the second chunk
    monitoredApp
        .getLeft()
        .edit(
            tm -> {
              IntStream.range(0, 266)
                  .forEach(
                      i -> {
                        tm.add("A", i * i * i);
                      });
            });

    // Force to discard all versions
    monitoredApp.getLeft().getEpochManager().forceDiscardEpochs(__ -> true);
    final int storeAIdx = monitoredApp.getLeft().getSchemaMetadata().getStoreId("A");

    final IMemoryAnalysisService analysisService =
        createService(monitoredApp.getLeft(), monitoredApp.getRight());
    final Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
    final Collection<IMemoryStatistic> storeStats = loadDatastoreMemoryStatFromFolder(exportPath);

    // Make sure there is only one loaded store
    Assert.assertEquals(1, storeStats.size());

    // Start a monitoring datastore with the exported data
    final IDatastore monitoringDatastore = createAnalysisDatastore();

    storeStats.forEach(
        (stats) -> {
          monitoringDatastore.edit(
              tm -> {
                stats.accept(
                    new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "storeA"));
              });
        });

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

    final Pair<IDatastore, IActivePivotManager> monitoredApp = createMicroApplicationWithNoCube();
    // Add 100 records
    monitoredApp
        .getLeft()
        .edit(
            tm -> {
              IntStream.range(0, 100)
                  .forEach(
                      i -> {
                        tm.add("A", i * i);
                      });
            });
    // Delete 10 records
    monitoredApp
        .getLeft()
        .edit(
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
    // Force to discard all versions
    monitoredApp.getLeft().getEpochManager().forceDiscardEpochs(__ -> true);
    final int storeAIdx = monitoredApp.getLeft().getSchemaMetadata().getStoreId("A");

    final IMemoryAnalysisService analysisService =
        createService(monitoredApp.getLeft(), monitoredApp.getRight());
    final Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");

    final Collection<IMemoryStatistic> storeStats = loadDatastoreMemoryStatFromFolder(exportPath);
    // Start a monitoring datastore with the exported data
    final IDatastore monitoringDatastore = createAnalysisDatastore();

    storeStats.forEach(
        (stats) -> {
          monitoringDatastore.edit(
              tm -> {
                stats.accept(
                    new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "storeA"));
              });
        });

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

    final Pair<IDatastore, IActivePivotManager> monitoredApp = createMicroApplication();

    // Add 10 records
    monitoredApp
        .getLeft()
        .edit(
            tm -> {
              IntStream.range(0, 10)
                  .forEach(
                      i -> {
                        tm.add("A", i * i * i);
                      });
            });

    // Force to discard all versions
    monitoredApp.getLeft().getEpochManager().forceDiscardEpochs(__ -> true);
    final int storeAIdx = monitoredApp.getLeft().getSchemaMetadata().getStoreId("A");

    final IMemoryAnalysisService analysisService =
        createService(monitoredApp.getLeft(), monitoredApp.getRight());
    final Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
    final Collection<IMemoryStatistic> pivotStats = loadPivotMemoryStatFromFolder(exportPath);

    // Make sure there is only one loaded store
    Assert.assertEquals(1, pivotStats.size());

    // Start a monitoring datastore with the exported data
    final IDatastore monitoringDatastore = createAnalysisDatastore();
    monitoringDatastore.edit(
        tm -> {
          pivotStats.forEach(
              (stats) -> {
                stats.accept(new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "Cube"));
              });
        });

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
  public void testMappingFromChunkToFields() {
    final Pair<IDatastore, IActivePivotManager> monitoredApp = createMicroApplicationWithNoCube();
    final IMemoryAnalysisService analysisService =
        createService(monitoredApp.getLeft(), monitoredApp.getRight());

    // Add 100 records
    monitoredApp
        .getLeft()
        .edit(
            tm -> {
              IntStream.range(0, 10)
                  .forEach(
                      i -> {
                        tm.add("A", i * i);
                      });
            });
    // Initial Dump
    Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
    final IMemoryStatistic stats = loadMemoryStatFromFolder(exportPath);

    final IDatastore monitoringDatastore = createAnalysisDatastore();
    monitoringDatastore.edit(
        tm -> {
          stats.accept(new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "appAInit"));
        });

    final List<IMemoryStatistic> dics =
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
  public void testCompareDumpsOfDifferentEpochs() {
    final Pair<IDatastore, IActivePivotManager> monitoredApp = createMicroApplicationWithNoCube();
    final IMemoryAnalysisService analysisService =
        createService(monitoredApp.getLeft(), monitoredApp.getRight());

    // Add 100 records
    monitoredApp
        .getLeft()
        .edit(
            tm -> {
              IntStream.range(0, 100)
                  .forEach(
                      i -> {
                        tm.add("A", i * i);
                      });
            });
    // Initial Dump
    Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
    final IMemoryStatistic stats = loadMemoryStatFromFolder(exportPath);
    // Delete 10 records
    monitoredApp
        .getLeft()
        .edit(
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
    monitoredApp.getLeft().getEpochManager().forceDiscardEpochs(__ -> true);

    exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
    final IMemoryStatistic statsEpoch2 = loadMemoryStatFromFolder(exportPath);

    final IDatastore monitoringDatastore = createAnalysisDatastore();
    monitoringDatastore.edit(
        tm -> {
          stats.accept(new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "appAInit"));
          statsEpoch2.accept(
              new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "appAEpoch2"));
        });
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
  public void testCompareDumpsOfDifferentApps() throws IOException, AgentException {
    // Create First App
    final Pair<IDatastore, IActivePivotManager> monitoredApp = createMicroApplicationWithNoCube();
    final IMemoryAnalysisService analysisService =
        createService(monitoredApp.getLeft(), monitoredApp.getRight());
    monitoredApp
        .getLeft()
        .edit(
            tm -> {
              IntStream.range(0, 100)
                  .forEach(
                      i -> {
                        tm.add("A", i * i);
                      });
            });
    // Export first app
    Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
    final IMemoryStatistic stats = loadMemoryStatFromFolder(exportPath);
    // Create second app
    final Pair<IDatastore, IActivePivotManager> monitoredAppWithBitmap =
        createMicroApplicationWithLeafBitmap();
    final IMemoryAnalysisService analysisServiceWithBitmap =
        createService(monitoredAppWithBitmap.getLeft(), monitoredAppWithBitmap.getRight());
    monitoredAppWithBitmap
        .getLeft()
        .edit(
            tm -> {
              IntStream.range(0, 100)
                  .forEach(
                      i -> {
                        tm.add("A", i * i);
                      });
            });
    // Export second app
    exportPath = analysisServiceWithBitmap.exportMostRecentVersion("testLoadDatastoreStats");

    final IMemoryStatistic statsWithBitmap = loadMemoryStatFromFolder(exportPath);
    final IDatastore monitoringDatastore = createAnalysisDatastore();
    monitoringDatastore.edit(
        tm -> {
          stats.accept(new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "App"));
          statsWithBitmap.accept(
              new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "AppWithBitmap"));
        });
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
    final Pair<IDatastore, IActivePivotManager> monitoredApp =
        createMicroApplicationWithReferenceAndNoCube();
    final IMemoryAnalysisService analysisService =
        createService(monitoredApp.getLeft(), monitoredApp.getRight());

    monitoredApp
        .getLeft()
        .edit(
            tm -> {
              IntStream.range(0, 100)
                  .forEach(
                      i -> {
                        tm.add("A", i * i, i);
                        tm.add("B", i);
                      });
            });
    // Export first app
    Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
    final IMemoryStatistic stats = loadMemoryStatFromFolder(exportPath);
    final IDatastore monitoringDatastore = createAnalysisDatastore();
    monitoringDatastore.edit(
        tm -> {
          stats.accept(new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "App"));
        });

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
  public void testBlocksOfSingleVectors() {
    createApplicationWithVectorAndNoCube(
        (monitoredDatastore, monitoredManager) -> {
          monitoredDatastore.edit(
              tm -> {
                IntStream.range(0, 24)
                    .forEach(
                        i -> {
                          tm.add(
                              VECTOR_STORE_NAME,
                              i,
                              IntStream.rangeClosed(1, 5).toArray(),
                              IntStream.rangeClosed(10, 20).toArray(),
                              LongStream.rangeClosed(3, 8).toArray());
                        });
              });

          final IMemoryAnalysisService analysisService =
              createService(monitoredDatastore, monitoredManager);
          final Path exportPath =
              analysisService.exportMostRecentVersion("testBlocksOfSingleVectors");
          final Collection<IMemoryStatistic> datastoreStats =
              loadDatastoreMemoryStatFromFolder(exportPath);

          final IDatastore monitoringDatastore = assertLoadsCorrectly(datastoreStats, getClass());

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
        });
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
