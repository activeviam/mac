package com.activeviam.mac.statistic.memory;

import static com.activeviam.mac.memory.DatastoreConstants.CHUNK_ID;
import static com.activeviam.mac.memory.DatastoreConstants.CHUNK_STORE;
import static com.activeviam.mac.memory.DatastoreConstants.CHUNK__CLASS;
import static com.activeviam.mac.memory.DatastoreConstants.CHUNK__PARENT_ID;
import static com.activeviam.mac.memory.DatastoreConstants.CHUNK__PARENT_TYPE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.activeviam.mac.Tools;
import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription;
import com.activeviam.mac.statistic.memory.visitor.impl.FeedVisitor;
import com.activeviam.pivot.builders.StartBuilding;
import com.qfs.condition.impl.BaseConditions;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.dic.IDictionary;
import com.qfs.junit.ResourceRule;
import com.qfs.literal.ILiteralType;
import com.qfs.monitoring.memory.impl.OnHeapPivotMemoryQuantifierPlugin;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.service.monitoring.IMemoryAnalysisService;
import com.qfs.store.IDatastore;
import com.qfs.store.NoTransactionException;
import com.qfs.store.impl.Datastore;
import com.qfs.store.query.ICompiledGetByKey;
import com.qfs.store.query.IDictionaryCursor;
import com.qfs.store.query.impl.CompiledGetByKey;
import com.qfs.store.record.IRecordFormat;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.qfs.util.impl.ThrowingLambda;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.IMultiVersionActivePivot;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.definitions.impl.ActivePivotDatastorePostProcessor;
import com.quartetfs.biz.pivot.dto.CellSetDTO;
import com.quartetfs.biz.pivot.query.impl.MDXQuery;
import com.quartetfs.biz.pivot.test.util.PivotTestUtils;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.QuartetRuntimeException;
import com.quartetfs.fwk.impl.Pair;
import com.quartetfs.fwk.query.QueryException;
import com.quartetfs.fwk.query.UnsupportedQueryException;

import org.assertj.core.api.Assertions;
import org.junit.Assert;

public class TestMemoryMonitoringDatastoreContent extends ATestMemoryStatistic {
	
	/**
	 * Tests the consistency between the chunks of an ActivePivot application and its the monitoring data obtained by loading exported data.
	 */
	@Test
	public void testDatastoreMonitoringValues() {

		createMinimalApplication((monitoredDatastore, monitoredManager) -> {

			fillApplicationMinimal(monitoredDatastore);
			performGC();
			
			final IMemoryAnalysisService analysisService = TestMemoryStatisticLoading.createService(monitoredDatastore, monitoredManager);
			final Path exportPath = analysisService.exportApplication("testLoadComplete");

			final IMemoryStatistic fullStats = TestMemoryStatisticLoading.loadMemoryStatFromFolder(exportPath);
			final Datastore monitoringDatastore = (Datastore) TestMemoryStatisticLoading.createAnalysisDatastore();
			
			IDictionary<Object> dic = monitoredDatastore.getDictionaries().getDictionary("Sales", "id");
			
			final long[] chunkIds = new long[2];
			final long[] chunkSizes = new long[2];
			
			AtomicInteger rnk = new AtomicInteger();
			
			final long[] monitoredChunkSizes = new long[2];
			
			// Check all the chunks held by the Dictionary of that key field of the store			
			IMemoryStatistic stat = dic.getMemoryStatistic();

			stat.getChildren().forEach((c_st)->{
				c_st.getChildren().forEach((c_c_st)->{
					c_c_st.getChildren().forEach((c_c_c_st)->{
						chunkSizes[rnk.get()]=c_c_c_st.getAttribute("length").asLong();						
						chunkIds[rnk.get()]=c_c_c_st.getAttribute("chunkId").asLong();
						rnk.getAndIncrement();
					});
					});
				});

			monitoringDatastore.edit(tm -> {
				fullStats.accept(new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "test"));


				final ICompiledGetByKey query= new CompiledGetByKey(monitoringDatastore.getSchema(),
						monitoringDatastore.getSchemaMetadata().getStoreMetadata(DatastoreConstants.CHUNK_STORE),
						monitoringDatastore.getSchema().getStore(DatastoreConstants.CHUNK_STORE).getDictionaryProvider(),
						0,
						Arrays.asList(DatastoreConstants.CHUNK__OWNER,
								DatastoreConstants.CHUNK__PARENT_ID,
								DatastoreConstants.CHUNK__CLASS,
								DatastoreConstants.CHUNK__SIZE));
				Object keys[] = new Object[2];
				
				int chunkDumpNameFieldIdx = monitoringDatastore.getSchema().getStore(DatastoreConstants.CHUNK_STORE).getFieldIndex(DatastoreConstants.CHUNK__DUMP_NAME);

				for (int i=0;i<chunkIds.length;i++)
				{
				keys[0] =  chunkIds[i];
				keys[1] =  monitoringDatastore.getSchema().getStore(DatastoreConstants.CHUNK_STORE).getDictionaryProvider().getDictionary(chunkDumpNameFieldIdx).read(1);
				
				Object[] top_obj = query.runInTransaction(keys, true).toTuple();
				monitoredChunkSizes[i] = Long.valueOf(top_obj[3].toString());
				}
			
			});
			// Now we verify the monitored chunks and the chunk in the Datastore of the Monitoring Cube are identical
			assertArrayEquals(chunkSizes, monitoredChunkSizes);
		});

	}

	@Test
	public void testChunkStructureFieldsWithSingleRecord( ) throws IOException, AgentException {
	final Pair<IDatastore,IActivePivotManager> monitoredApp = createMicroApplication(); 
	
	// Add a single record
	monitoredApp.getLeft().edit(tm -> {
		IntStream.range(0, 1).forEach(i -> {
			tm.add("A",i*i*100000000);
		});
	});

	// Force to discard all versions
	monitoredApp.getLeft().getEpochManager().forceDiscardEpochs(__ -> true);

	final IMemoryAnalysisService analysisService = createService(monitoredApp.getLeft(), monitoredApp.getRight());
	final Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
	final Collection<IMemoryStatistic> storeStats = loadDatastoreMemoryStatFromFolder(exportPath);
	
	//Make sure there is only one loaded store
	Assert.assertEquals(1, storeStats.size());
	
	// Start a monitoring datastore with the exported data 
	final IDatastore monitoringDatastore = createAnalysisDatastore();
monitoringDatastore.edit(tm -> {
		storeStats.forEach((stats) -> {			
				stats.accept(new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "storeA"));
			});
		});

	// Query record chunks data :
	final IDictionaryCursor cursor = monitoringDatastore.getHead().getQueryRunner()
			.forStore(CHUNK_STORE)
			.withCondition(
					BaseConditions.Equal(DatastoreConstants.CHUNK__PARENT_TYPE,MemoryAnalysisDatastoreDescription.ParentType.RECORDS)
					)
			.selecting(
					DatastoreConstants.CHUNK__FREE_ROWS,
					DatastoreConstants.CHUNK__NON_WRITTEN_ROWS,
					DatastoreConstants.CHUNK__SIZE,
					DatastoreConstants.CHUNK__OFF_HEAP_SIZE)
			.onCurrentThread().run();
	final List<Object[]> list = new ArrayList<>();
	cursor.forEach((record)->{
		Object[] data = record.toTuple();
		list.add(data);
		});
	// Expect 3 Chunks : 
	// - 1 Chunk for versions : directChunkLong
	// - 2 Chunks for the actual records
	//         * 1 Wrapper chunk (DirectChunkPositiveInteger) -> Verify that the offHeap size equal to Zero
	//         * 1 Content Chunk (ChunkSingleInteger) -> Verify that no data is stored offheap
	Assertions.assertThat(list.size()).isEqualTo(3);
	Assertions.assertThat(list).containsExactly(new Object[] {0,255,256,0L},new Object[] {0,255,256,0L},new Object[] {0,255,256,2048L});
	}
	
	@Test
	public void testChunkStructureFieldsWithFullChunk( ) throws IOException, AgentException {
		final Pair<IDatastore,IActivePivotManager> monitoredApp = createMicroApplication(); 
		
		// Add a full chunk
		monitoredApp.getLeft().edit(tm -> {
			IntStream.range(0, 256).forEach(i -> {
				tm.add("A", i*i*i);
			});
		});
		
		// Force to discard all versions
		monitoredApp.getLeft().getEpochManager().forceDiscardEpochs(__ -> true);
		final int storeAIdx = monitoredApp.getLeft().getSchemaMetadata().getStoreId("A");
		

		final IMemoryAnalysisService analysisService = createService(monitoredApp.getLeft(), monitoredApp.getRight());
		final Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
		final Collection<IMemoryStatistic> storeStats = loadDatastoreMemoryStatFromFolder(exportPath);
		
		//Make sure there is only one loaded store
		Assert.assertEquals(1, storeStats.size());
		
		// Start a monitoring datastore with the exported data 
		final IDatastore monitoringDatastore = createAnalysisDatastore();

			storeStats.forEach((stats) -> {
				monitoringDatastore.edit(tm -> {
					stats.accept(new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "storeA"));
				});
			});

		// Query record chunks data :
		final IDictionaryCursor cursor = monitoringDatastore.getHead().getQueryRunner()
				.forStore(CHUNK_STORE)
				.withCondition(
						BaseConditions.Equal(DatastoreConstants.CHUNK__PARENT_TYPE,MemoryAnalysisDatastoreDescription.ParentType.RECORDS)
						)
				.selecting(
						DatastoreConstants.CHUNK__FREE_ROWS,
						DatastoreConstants.CHUNK__NON_WRITTEN_ROWS,
						DatastoreConstants.CHUNK__SIZE,
						DatastoreConstants.CHUNK__OFF_HEAP_SIZE)
				.onCurrentThread().run();
		final List<Object[]> list = new ArrayList<>();
		cursor.forEach((record)->{
			Object[] data = record.toTuple();
			list.add(data);
			});
		// Expect 2 Chunks : 
		// - 1 Chunk for versions : directChunkLong
		// - 1 Chunks for the actual records (ChunkShorts) -> Verify that 256 bytes of data is stored offheap
		Assertions.assertThat(list.size()).isEqualTo(2);
		Assertions.assertThat(list).containsExactly(new Object[] {0,0,256,256L},new Object[] {0,0,256,2048L});
	}
	
	
	@Test
	public void testChunkStructureFieldsWithTwoChunks( ) throws IOException, AgentException {
		final Pair<IDatastore,IActivePivotManager> monitoredApp = createMicroApplication(); 
		
		// Add a full chunk + 10 records on the second chunk
		monitoredApp.getLeft().edit(tm -> {
			IntStream.range(0, 266).forEach(i -> {
				tm.add("A", i*i*i);
			});
		});
		
		// Force to discard all versions
		monitoredApp.getLeft().getEpochManager().forceDiscardEpochs(__ -> true);
		final int storeAIdx = monitoredApp.getLeft().getSchemaMetadata().getStoreId("A");
		

		final IMemoryAnalysisService analysisService = createService(monitoredApp.getLeft(), monitoredApp.getRight());
		final Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
		final Collection<IMemoryStatistic> storeStats = loadDatastoreMemoryStatFromFolder(exportPath);
		
		//Make sure there is only one loaded store
		Assert.assertEquals(1, storeStats.size());
		
		// Start a monitoring datastore with the exported data 
		final IDatastore monitoringDatastore = createAnalysisDatastore();

			storeStats.forEach((stats) -> {
				monitoringDatastore.edit(tm -> {
					stats.accept(new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "storeA"));
				});
			});

		// Query record chunks data :
		final IDictionaryCursor cursor = monitoringDatastore.getHead().getQueryRunner()
				.forStore(CHUNK_STORE)
				.withCondition(
						BaseConditions.Equal(DatastoreConstants.CHUNK__PARENT_TYPE,MemoryAnalysisDatastoreDescription.ParentType.RECORDS)
						)
				.selecting(
						DatastoreConstants.CHUNK__FREE_ROWS,
						DatastoreConstants.CHUNK__NON_WRITTEN_ROWS,
						DatastoreConstants.CHUNK__SIZE,
						DatastoreConstants.CHUNK__OFF_HEAP_SIZE)
				.onCurrentThread().run();
		final List<Object[]> list = new ArrayList<>();
		cursor.forEach((record)->{
			Object[] data = record.toTuple();
			list.add(data);
			});
		// Expect 5 Chunks : 
		// - 2 Chunk for versions : directChunkLong
		// - 3 Chunks for the actual records
		//		   * 1 Wrapping chunk 
		//         * 2 Content Chunk
		Assertions.assertThat(list.size()).isEqualTo(5);
		Assertions.assertThat(list).containsExactlyInAnyOrder(
				//Version chunks
				new Object[] {0,0,256,2048L},
				new Object[] {0,246,256,2048L},
				//Content chunks
				//Chunk full (not wrapped)
				new Object[] {0,0,256,256L},
				// Partially full chunk (wrapper+data)
				new Object[] {0,246,256,0L},
				new Object[] {0,246,256,512L}
				);
	}
	
	@Test
	public void testChunkStructureFieldsWithFreedRows( ) throws IOException, AgentException {
//		final Pair<IDatastore,IActivePivotManager> monitoredApp = createMicroApplication(); 
//		
//		// Add a full chunk + 10 records on the second chunk
//		monitoredApp.getLeft().edit(tm -> {
//			IntStream.range(0, 128).forEach(i -> {
//				tm.add("A", i*i*i);
//			});
//		});
//		
//		//Remove 20 records
//		monitoredApp.getLeft().edit(tm -> {
//		IntStream.range(100, 120).boxed().forEach(((Integer i) -> {
//			try {
//				tm.remove("A", i*i*i);
//			} catch (NoTransactionException | DatastoreTransactionException | IllegalArgumentException
//					| NullPointerException e) {
//				throw new RuntimeException(e);
//			}
//		}));}
//		);
//		//Re-add 10 records over the removed records
//		monitoredApp.getLeft().edit(tm -> {
//			IntStream.range(105, 115).boxed().forEach(((Integer i) -> {
//				tm.add("A", i * i * i);
//			}));
//		});
//		
//		// Force to discard all versions
//		monitoredApp.getLeft().getEpochManager().forceDiscardEpochs(__ -> true);
//		final int storeAIdx = monitoredApp.getLeft().getSchemaMetadata().getStoreId("A");
//		
//
//		final IMemoryAnalysisService analysisService = createService(monitoredApp.getLeft(), monitoredApp.getRight());
//		final Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
//		final Collection<IMemoryStatistic> storeStats = loadDatastoreMemoryStatFromFolder(exportPath);
//		
		
		final Pair<IDatastore,IActivePivotManager> monitoredApp = createMicroApplication();
		// Add 100 records
		monitoredApp.getLeft().edit(tm -> {
			IntStream.range(0, 100).forEach(i -> {
				tm.add("A", i * i);
			});
		});
		// Delete 10 records
		monitoredApp.getLeft().edit(tm -> {
			IntStream.range(50, 50+10).forEach(i -> {
				try {
					tm.remove("A", i * i);
				} catch (NoTransactionException | DatastoreTransactionException | IllegalArgumentException
						| NullPointerException e) {
					throw new QuartetRuntimeException(e);
				}
			});
		});
				
		// Force to discard all versions
		monitoredApp.getLeft().getEpochManager().forceDiscardEpochs(__ -> true);
		final int storeAIdx = monitoredApp.getLeft().getSchemaMetadata().getStoreId("A");

		final IMemoryAnalysisService analysisService = createService(monitoredApp.getLeft(), monitoredApp.getRight());
		final Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
		Tools.extractSnappyFile(exportPath.toString()+"\\pivot_Cube.json.sz");
		Tools.extractSnappyFile(exportPath.toString()+"\\store_A.json.sz");
		//Make sure there is only one loaded store
	//	Assert.assertEquals(1, storeStats.size());
		final Collection<IMemoryStatistic> storeStats = loadDatastoreMemoryStatFromFolder(exportPath);
		// Start a monitoring datastore with the exported data 
		final IDatastore monitoringDatastore = createAnalysisDatastore();

			storeStats.forEach((stats) -> {
				monitoringDatastore.edit(tm -> {
					stats.accept(new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "storeA"));
				});
			});

		// Query record chunks data :
		final IDictionaryCursor cursor = monitoringDatastore.getHead().getQueryRunner()
				.forStore(CHUNK_STORE)
				.withCondition(
						BaseConditions.Equal(DatastoreConstants.CHUNK__PARENT_TYPE,MemoryAnalysisDatastoreDescription.ParentType.RECORDS)
						)
				.selecting(
						DatastoreConstants.CHUNK__FREE_ROWS,
						DatastoreConstants.CHUNK__NON_WRITTEN_ROWS,
						DatastoreConstants.CHUNK__SIZE,
						DatastoreConstants.CHUNK__OFF_HEAP_SIZE)
				.onCurrentThread().run();
		final List<Object[]> list = new ArrayList<>();
		cursor.forEach((record)->{
			Object[] data = record.toTuple();
			list.add(data);
			});
		// Expect 3 Chunks : 
		// - 1 Chunk for versions : directChunkLong
		// - 2 Chunks for the actual records
		//		   * 1 Wrapping chunk 
		//         * 1 Content Chunk
		Assertions.assertThat(list.size()).isEqualTo(3);
		Assertions.assertThat(list).containsExactlyInAnyOrder(
				//Version chunks
				new Object[] {0,118,256,2048L},
				//Content chunks
				// Partially full chunk (wrapper+data)
				new Object[] {20,118,256,0L},
				new Object[] {20,118,256,256L}
				);
	}
	
	@Test void testSharedChunks() {
		
		
	}
	
	@Test
	public void testLevelStoreContent() throws AgentException, IOException, UnsupportedQueryException, QueryException {
		
		final Pair<IDatastore,IActivePivotManager> monitoredApp = createMicroApplication(); 
		
		// Add 10 records
		monitoredApp.getLeft().edit(tm -> {
			IntStream.range(0, 10).forEach(i -> {
				tm.add("A", i*i*i);
			});
		});
		
		// Force to discard all versions
		monitoredApp.getLeft().getEpochManager().forceDiscardEpochs(__ -> true);
		final int storeAIdx = monitoredApp.getLeft().getSchemaMetadata().getStoreId("A");
		

		final IMemoryAnalysisService analysisService = createService(monitoredApp.getLeft(), monitoredApp.getRight());
		final Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
		final Collection<IMemoryStatistic> pivotStats = loadPivotMemoryStatFromFolder(exportPath);
		
		//Make sure there is only one loaded store
		Assert.assertEquals(1, pivotStats.size());
		
		// Start a monitoring datastore with the exported data 
		final IDatastore monitoringDatastore = createAnalysisDatastore();
		monitoringDatastore.edit(tm -> {
			pivotStats.forEach((stats) -> {
					stats.accept(new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "Cube"));
				});
			});

		// Query record of level data :
		final IDictionaryCursor cursor = monitoringDatastore.getHead().getQueryRunner()
				.forStore(DatastoreConstants.CHUNK_TO_LEVEL_STORE)
				.withCondition(
						BaseConditions.TRUE
						)
				.selecting(
						DatastoreConstants.CHUNK_TO_INDEX__PARENT_ID)
				.onCurrentThread().run();
		final List<Object[]> list = new ArrayList<>();
		cursor.forEach((record)->{
			Object[] data = record.toTuple();
			list.add(data);
			});
		Assertions.assertThat(list.size()).isEqualTo(1);
	}
}