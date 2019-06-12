package com.activeviam.mac.statistic.memory;

import static com.activeviam.mac.memory.DatastoreConstants.CHUNK_STORE;

import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.activeviam.mac.Tools;
import com.activeviam.mac.cfg.impl.DatastoreDescriptionConfig;
import com.activeviam.mac.cfg.impl.LocalContentServiceConfig;
import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.cfg.impl.SourceConfig;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription;
import com.activeviam.mac.statistic.memory.visitor.impl.FeedVisitor;
import com.activeviam.pivot.builders.StartBuilding;
import com.activeviam.properties.IActiveViamProperty;
import com.activeviam.properties.cfg.impl.ActiveViamPropertyFromSpringConfig;
import com.activeviam.properties.impl.ActiveViamProperty;
import com.activeviam.properties.impl.ActiveViamPropertyRule;
import com.activeviam.properties.impl.ActiveViamPropertyRule.ActiveViamPropertyRuleBuilder;
import com.qfs.chunk.impl.Chunks;
import com.qfs.condition.impl.BaseConditions;
import com.qfs.memory.impl.PlatformOperations;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils.StatisticsSummary;
import com.qfs.monitoring.offheap.SlabDirectChunkAllocatorWithCounter;
import com.qfs.monitoring.offheap.SlabDirectChunkAllocatorWithCounter.SlabMemoryAllocatorWithCounter;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;
import com.qfs.server.cfg.IDatastoreDescriptionConfig;
import com.qfs.server.cfg.impl.ActivePivotConfig;
import com.qfs.server.cfg.impl.DatastoreConfig;
import com.qfs.server.cfg.impl.FullAccessBranchPermissionsManagerConfig;
import com.qfs.service.monitoring.IMemoryAnalysisService;
import com.qfs.store.IDatastore;
import com.qfs.store.NoTransactionException;
import com.qfs.store.query.IDictionaryCursor;
import com.qfs.store.query.impl.DatastoreQueryHelper;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.qfs.util.impl.QfsArrays;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.IMultiVersionActivePivot;
import com.quartetfs.biz.pivot.cellset.ICellSet;
import com.quartetfs.biz.pivot.cube.hierarchy.measures.IMeasureHierarchy;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.dto.CellSetDTO;
import com.quartetfs.biz.pivot.query.impl.ActivePivotQueryRunner;
import com.quartetfs.biz.pivot.query.impl.MDXQuery;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.QuartetRuntimeException;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.contributions.impl.ClasspathContributionProvider;
import com.quartetfs.fwk.impl.Pair;
import com.quartetfs.fwk.query.QueryException;
import com.quartetfs.fwk.query.UnsupportedQueryException;

import test.util.impl.DatastoreTestUtils;

/**
 * Test class verifying the results obtained by the Measures provided
 * in the MemoryAnalysisCube 
 * 
 * @author ActiveViam
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(properties = { "contentServer.security.calculatedMemberRole=zob", "contentServer.security.kpiRole=zob"})
@ContextConfiguration(classes = {
		DatastoreDescriptionConfig.class, 
		ManagerDescriptionConfig.class,
})
public class TestMACMeasures extends ATestMemoryStatistic {

	@Autowired
	IActivePivotManagerDescription managerDescription;
	
	@Autowired
	IDatastoreDescriptionConfig datastoreDescriptionConfig;
	
	Pair<IDatastore, IActivePivotManager> monitoredApp;
	
	Pair<IDatastore,IActivePivotManager> monitoringApp;
	
	StatisticsSummary statsSumm;
	
	public static final int ADDED_DATA_SIZE = 100;
	public static final int REMOVED_DATA_SIZE = 10;	
	public static final int MAX_GC_STEPS = 10;
	@ClassRule
	public static ActiveViamPropertyRule propertyRule = new ActiveViamPropertyRuleBuilder().withProperty(ActiveViamProperty.ACTIVEVIAM_TEST_PROPERTY, true).build();
	@BeforeClass
	public static void init() {		
	Registry.setContributionProvider(new ClasspathContributionProvider());
	}
	
	@Before
	public void setup() throws AgentException, IOException {
		SlabDirectChunkAllocatorWithCounter.configureThis();
		monitoredApp = createMicroApplication();
		// Add 100 records
		monitoredApp.getLeft().edit(tm -> {
			IntStream.range(0, ADDED_DATA_SIZE).forEach(i -> {
				tm.add("A", i * i);
			});
		});
		// Delete 10 records
		monitoredApp.getLeft().edit(tm -> {
			IntStream.range(50, 50+REMOVED_DATA_SIZE).forEach(i -> {
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
		// perform  GCs before exporting the store data
		performGC();
		final HackedMemoryAnalysisService analysisService = (HackedMemoryAnalysisService) createService(monitoredApp.getLeft(), monitoredApp.getRight());
		final Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
		Tools.extractSnappyFile(exportPath.toString()+"\\pivot_Cube.json.sz");
		Tools.extractSnappyFile(exportPath.toString()+"\\store_A.json.sz");
		
		
		Map<String, Long> a1 = analysisService.collectGlobalMemoryStatus();

		final IMemoryStatistic stats = loadMemoryStatFromFolder(exportPath);
		statsSumm =MemoryStatisticsTestUtils.getStatisticsSummary(stats);

		Map<String, Long> a2 = analysisService.collectGlobalMemoryStatus();

		// Start a monitoring datastore with the exported data
		final IDatastore monitoringDatastore = createAnalysisDatastore();
		//Start a monitoring cube
		IActivePivotManager manager = StartBuilding.manager()
		.setDescription(managerDescription)
		.setDatastoreAndDescription(monitoringDatastore, datastoreDescriptionConfig.schemaDescription())
		.buildAndStart();
		monitoringApp = new Pair<>(monitoringDatastore,manager);
		
		//Fill the monitoring datastore
		monitoringDatastore.edit(tm -> {			
			stats.accept(new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "storeA"));
		});

		IMultiVersionActivePivot pivot = monitoringApp.getRight().getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);
		Assertions.assertThat(pivot).isNotNull();
	}
	
	static Long extractValueFromSingleCellDTO(CellSetDTO data) {
		Assertions.assertThat(data.getCells().size()).isEqualTo(1);

		String sum_s = data.getCells().iterator().next().toString();
		String[] cell =sum_s.split(",");
		Long value = null;
		for (String attr : cell)
		{
			if (attr.contains(" value=")){
				value =Long.parseLong(attr.replace(" value=", ""));
			}
		}
		return value;
	}
	
	static Double[] extractValuesFromCellSetDTO(CellSetDTO data) {
		final AtomicInteger cursor = new AtomicInteger();
		Double[] res = new Double[data.getCells().size()];
		data.getCells().forEach(cell -> {
			int i = cursor.getAndIncrement();
			String[] cell_s = cell.toString().split(",");
			for (String attr : cell_s) {
				
				if (attr.contains(" value=")) {
					res[i]=Double.parseDouble(attr.replace(" value=", ""));
				}
			}
		});
		return res;
	}
	
	
	static Long sumValuesFromCellSetDTO(CellSetDTO data) {
		final AtomicLong value = new AtomicLong();
		data.getCells().forEach(cell -> {
			String[] cell_s = cell.toString().split(",");

			for (String attr : cell_s) {
				if (attr.contains(" value=")) {
					value.addAndGet(Long.parseLong(attr.replace(" value=", "")));
				}
			}
		});
		return value.get();
	}
	
	@Test
	public void testDirectMemorySum() throws AgentException, IOException, UnsupportedQueryException, QueryException {
				
		final IMultiVersionActivePivot pivot = monitoringApp.getRight().getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);
		final MDXQuery query = new MDXQuery("SELECT" + 
				"  NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS" + 
				"  FROM [MemoryCube]");
		CellSetDTO res = pivot.execute(query);

		Long value = extractValueFromSingleCellDTO(res);

		System.out.println(value);
		
		final MDXQuery query2 = new MDXQuery("SELECT" + 
				"  NON EMPTY [Measures].[contributors.COUNT] ON COLUMNS" + 
				"  FROM [MemoryCube]");
		CellSetDTO res2 = pivot.execute(query2);		
		Long nbC = extractValueFromSingleCellDTO(res2);


		final MDXQuery query3 = new MDXQuery(
		"SELECT" + 
		  " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS," + 
		  "  NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS" + 
		  "  FROM [MemoryCube]");
		CellSetDTO res3 = pivot.execute(query3);
		
		// Check that the cell size is the expected one (the amount of chunks)
		Assertions.assertThat(res3.getCells().size()).isEqualTo(nbC.intValue());
		// Check that the summed value corresponds to the sum on each chunk of the Chunk Level
		Assertions.assertThat(sumValuesFromCellSetDTO(res3)).isEqualTo(value);		
		// Check that the summed value corresponds to the Exported sum
		Assertions.assertThat(statsSumm.offHeapMemory).isEqualTo(value);
	}

	@Test
	public void testOnHeapMemorySum() throws AgentException, IOException, UnsupportedQueryException, QueryException {

		final IMultiVersionActivePivot pivot = monitoringApp.getRight().getActivePivots()
				.get(ManagerDescriptionConfig.MONITORING_CUBE);
		final MDXQuery query = new MDXQuery(
				"SELECT" + "  NON EMPTY [Measures].[HeapMemory.SUM] ON COLUMNS" + "  FROM [MemoryCube]");
		CellSetDTO res = pivot.execute(query);

		Long value = extractValueFromSingleCellDTO(res);

		System.out.println(value);

		final MDXQuery query2 = new MDXQuery(
				"SELECT" + "  NON EMPTY [Measures].[contributors.COUNT] ON COLUMNS" + "  FROM [MemoryCube]");
		CellSetDTO res2 = pivot.execute(query2);
		Long nbC = extractValueFromSingleCellDTO(res2);

		final MDXQuery query3 = new MDXQuery("SELECT" + " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
				+ "  NON EMPTY [Measures].[HeapMemory.SUM] ON COLUMNS" + "  FROM [MemoryCube]");
		CellSetDTO res3 = pivot.execute(query3);

		// Check that the cell size is the expected one (the amount of chunks)
		Assertions.assertThat(res3.getCells().size()).isEqualTo(nbC.intValue());
		// Check that the summed value corresponds to the sum on each chunk of the Chunk
		// Level
		Assertions.assertThat(sumValuesFromCellSetDTO(res3)).isEqualTo(value);
		
		/*
		 * On-heap memory usage by chunks is not consistent with application
		 *  on-heap usage since on-heap data is not necessarily held by chunks
		 */
		//Assertions.assertThat(statsSumm.onHeapMemory).isEqualTo(value);
	}
	
	@Test
	public void testChunkSize() throws UnsupportedQueryException, QueryException {
		
		final IMultiVersionActivePivot pivot = monitoringApp.getRight().getActivePivots()
				.get(ManagerDescriptionConfig.MONITORING_CUBE);
		
		final MDXQuery query = new MDXQuery("SELECT" 
		+ " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
		+ "  NON EMPTY [Measures].[ChunkSize.SUM] ON COLUMNS" 
		+ "  FROM [MemoryCube]");
		CellSetDTO res = pivot.execute(query);
		
		Assertions.assertThat(extractValuesFromCellSetDTO(res)).contains((double)ATestMemoryStatistic.MICROAPP_CHUNK_SIZE);
	}
	
	@Test
	public void testNonWrittenCount() throws UnsupportedQueryException, QueryException {
		
		final IMultiVersionActivePivot pivot = monitoringApp.getRight().getActivePivots()
				.get(ManagerDescriptionConfig.MONITORING_CUBE);
		
		final MDXQuery query = new MDXQuery("SELECT" 
		+ " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
		+ "  NON EMPTY [Measures].[NonWrittenRows.COUNT] ON COLUMNS" 
		+ "  FROM [MemoryCube]");
		CellSetDTO res = pivot.execute(query);
		
		Assertions.assertThat(extractValuesFromCellSetDTO(res)).contains((double)ATestMemoryStatistic.MICROAPP_CHUNK_SIZE-ADDED_DATA_SIZE);
	}
	
	
	@Test
	public void testApplicationMeasures() throws UnsupportedQueryException, QueryException
	{
		final IMultiVersionActivePivot pivot = monitoringApp.getRight().getActivePivots()
				.get(ManagerDescriptionConfig.MONITORING_CUBE);

		final MDXQuery query2 = new MDXQuery(
				"SELECT" + "  NON EMPTY [Measures].[UsedDirectMemory] ON COLUMNS" + "  FROM [MemoryCube]");
		CellSetDTO res2 = pivot.execute(query2);
		Long value2 = extractValueFromSingleCellDTO(res2);
		Assertions.assertThat(value2).isEqualTo(statsSumm.offHeapMemory);
		}
	
	@Test
	public void testFreedCount() throws UnsupportedQueryException, QueryException {
		
		final IMultiVersionActivePivot pivot = monitoringApp.getRight().getActivePivots()
				.get(ManagerDescriptionConfig.MONITORING_CUBE);

		final MDXQuery query = new MDXQuery("SELECT" 
		+ " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
		+ "  [Measures].[DeletedRows.COUNT] ON COLUMNS" 
		+ "  FROM [MemoryCube]");
		CellSetDTO res = pivot.execute(query);
		
		Assertions.assertThat(extractValuesFromCellSetDTO(res)).contains((double) REMOVED_DATA_SIZE);
	}
			
	@Test
	public void testNonWrittenRatio() throws AgentException, IOException, UnsupportedQueryException, QueryException {

		final IMultiVersionActivePivot pivot = monitoringApp.getRight().getActivePivots()
				.get(ManagerDescriptionConfig.MONITORING_CUBE);
		final MDXQuery query = new MDXQuery("SELECT" 
		+ " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
		+ "  NON EMPTY [Measures].[ChunkSize.SUM] ON COLUMNS" 
		+ "  FROM [MemoryCube]");
		CellSetDTO res = pivot.execute(query);

		final MDXQuery query2 = new MDXQuery("SELECT" 
		+ " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
		+ "  NON EMPTY [Measures].[NonWrittenRows.COUNT] ON COLUMNS" 
		+ "  FROM [MemoryCube]");
		CellSetDTO res2 = pivot.execute(query2);

		final MDXQuery query3 = new MDXQuery("SELECT" 
		+ " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
		+ "  NON EMPTY [Measures].[NonWrittenRows.Ratio] ON COLUMNS"
		+ "  FROM [MemoryCube]");
		CellSetDTO res3 = pivot.execute(query3);
		
		final Double[] chunkSizes = extractValuesFromCellSetDTO(res);
		final Double[] nonWrittenRows = extractValuesFromCellSetDTO(res2);
		final Double[] nonWrittenRatio = extractValuesFromCellSetDTO(res3);

		for (int i =0; i<chunkSizes.length;i++)
		{
			Assertions.assertThat(nonWrittenRatio[i]).isEqualTo(nonWrittenRows[i]/chunkSizes[i]);
		}
	}
	
	@Test
	public void testDeletedRatio() throws AgentException, IOException, UnsupportedQueryException, QueryException {

		
		final IMultiVersionActivePivot pivot = monitoringApp.getRight().getActivePivots()
				.get(ManagerDescriptionConfig.MONITORING_CUBE);
		final MDXQuery query = new MDXQuery("SELECT" 
		+ " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
		+ "  NON EMPTY [Measures].[ChunkSize.SUM] ON COLUMNS" 
		+ "  FROM [MemoryCube]");
		CellSetDTO res = pivot.execute(query);

		final MDXQuery query2 = new MDXQuery("SELECT" 
		+ " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
		+ "  NON EMPTY [Measures].[DeletedRows.COUNT] ON COLUMNS" 
		+ "  FROM [MemoryCube]");
		CellSetDTO res2 = pivot.execute(query2);

		final MDXQuery query3 = new MDXQuery("SELECT" 
		+ " NON EMPTY [Chunks].[ChunkId].[ChunkId].Members ON ROWS,"
		+ "  NON EMPTY [Measures].[DeletedRows.Ratio] ON COLUMNS"
		+ "  FROM [MemoryCube]");
		CellSetDTO res3 = pivot.execute(query3);
		
		final Double[] chunkSizes = extractValuesFromCellSetDTO(res);
		final Double[] DeletedRows = extractValuesFromCellSetDTO(res2);
		final Double[] DeletedRatio = extractValuesFromCellSetDTO(res3);

		for (int i =0; i<chunkSizes.length;i++)
		{
			Assertions.assertThat(DeletedRatio[i]).isEqualTo(DeletedRows[i]/chunkSizes[i]);
		}
	}
}
