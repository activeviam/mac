/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.statistic.memory;

import static com.activeviam.mac.memory.DatastoreConstants.CHUNK_ID;
import static com.activeviam.mac.memory.DatastoreConstants.CHUNK_STORE;
import static com.activeviam.mac.memory.DatastoreConstants.CHUNK__CLASS;
import static com.activeviam.mac.memory.DatastoreConstants.CHUNK__OFF_HEAP_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import com.activeviam.health.monitor.IHealthCheckAgent;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription;
import com.activeviam.mac.statistic.memory.visitor.impl.DatastoreFeederVisitor;
import com.activeviam.mac.statistic.memory.visitor.impl.FeedVisitor;
import com.qfs.condition.impl.BaseConditions;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.desc.IStoreDescription;
import com.qfs.desc.impl.DatastoreSchemaDescription;
import com.qfs.desc.impl.StoreDescriptionBuilder;
import com.qfs.junit.EnvTestRule;
import com.qfs.junit.ResourceRule;
import com.qfs.literal.ILiteralType;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils.StatisticsSummary;
import com.qfs.monitoring.statistic.impl.LongStatisticAttribute;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;
import com.qfs.monitoring.statistic.memory.impl.MemoryStatisticBuilder;
import com.qfs.pivot.servlet.impl.ContextValueFilter;
import com.qfs.rest.client.impl.ClientPool;
import com.qfs.rest.services.impl.JsonRestService;
import com.qfs.server.cfg.IActivePivotConfig;
import com.qfs.server.cfg.IDatastoreConfig;
import com.qfs.server.cfg.impl.MonitoringRestServicesConfig;
import com.qfs.service.store.impl.NoSecurityDatastoreServiceConfig;
import com.qfs.store.IDatastore;
import com.qfs.store.build.impl.DatastoreBuilder;
import com.qfs.store.build.impl.UnitTestDatastoreBuilder;
import com.qfs.store.query.IDictionaryCursor;
import com.qfs.store.query.impl.DatastoreQueryHelper;
import com.qfs.store.record.IRecordReader;
import com.qfs.store.service.impl.DatastoreService;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.qfs.util.impl.QfsArrays;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.security.IContextValueManager;
import com.quartetfs.biz.pivot.security.IContextValuePropagator;
import com.quartetfs.fwk.security.ISecurityFacade;
import com.quartetfs.fwk.security.impl.SecurityDetails;
import org.assertj.core.api.SoftAssertions;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import test.scenario.MultipleStores;
import test.scenario.MultipleStoresData;

public class TestMemoryStatisticLoading {

	@ClassRule
	public static final EnvTestRule envRule = EnvTestRule.getInstance();
	@ClassRule
	public static final ResourceRule resources = new ResourceRule();

	protected static final String datastoreBeanName = "datastore";
	protected static final String jsonDatastoreServiceBeanName = "jsonDatastoreService";
//	private static JettyServer jettyServer;
	private static ClientPool clientPool;
	private static JsonRestService service;
	private static IDatastore datastore;

	@Rule
	public TemporaryFolder temp = new TemporaryFolder();

	@Configuration
	@Import(value = {
			MonitoringRestServicesConfig.class,
			TestOffHeapMonitoringServiceConfig.DatastoreConfigForTest.class,
			TestOffHeapMonitoringServiceConfig.APConfigForTest.class
	})
	protected static class TestOffHeapMonitoringServiceConfig {

		@Configuration
		protected static class DatastoreConfigForTest implements IDatastoreConfig {

			@Bean(name = datastoreBeanName)
			@Override
			public IDatastore datastore() {
				return new UnitTestDatastoreBuilder()
						.setSchemaDescription(MultipleStores.schemaDescription())
						.build();
			}

			@Bean(name = jsonDatastoreServiceBeanName)
			public DatastoreService jsonDatastoreService() {
				final ISecurityFacade sf = Mockito.mock(ISecurityFacade.class);
				Mockito.when(sf.snapshotSecurityDetails()).thenReturn(
						new SecurityDetails("user1", new HashSet<>(Arrays.asList("ROLE_USER", "role1"))));

				return new DatastoreService(
						datastore(),
						new NoSecurityDatastoreServiceConfig(),
						sf);
			}

		}

		@Configuration
		protected static class APConfigForTest implements IActivePivotConfig {

			@Override
			public IActivePivotManagerDescription activePivotManagerDescription() {
				return null;
			}

			@Bean
			@Override
			public IActivePivotManager activePivotManager() {
				// As of Spring 5.0, Beans cannot be null anymore;
				// see: https://stackoverflow.com/questions/49044770/change-in-how-spring-5-handles-null-beans
				final IActivePivotManager manager = Mockito.mock(IActivePivotManager.class);
				Mockito.when(manager.getMemoryStatistic()).thenReturn(
						new MemoryStatisticBuilder()
								.withName("mock")
								.withCreatorClasses(APConfigForTest.class)
								.withMemoryFootPrint(0, 0)
								.build());
				return manager;
			}

			@Override
			public IHealthCheckAgent healthCheckAgent() {
				return null;
			}

			@Override
			public ContextValueFilter contextValueFilter() {
				return null;
			}

			@Override
			public IContextValuePropagator contextValuePropagator() {
				return null;
			}

			@Override
			public IContextValueManager contextValueManager() {
				return null;
			}
		}

	}

	/**
	 * The {@link DatastoreService} that will be used
	 * (as a local service but it could be remote)
	 */
	protected static DatastoreService jsonDatastoreService;

//	@BeforeClass
//	// Override super to only add one rest service with a different datastore configuration
//	public static void setUp() throws Exception {
//		MemUtils.runGC();
//
////		jettyServer = new JettyServer(TestOffHeapMonitoringServiceConfig.class);
////
////		jettyServer.createServer();
////		jettyServer.start();
//
//		clientPool = new ClientPool(1);
//		service = new JsonRestService(QfsWebUtils.url("http://localhost:" + jettyServer.getPort(),
//		                                              MonitoringRestServicesConfig.REST_API_URL_PREFIX,
//		                                              "/"), clientPool);
//
//		datastore = (IDatastore) jettyServer.getApplicationContext().getBean(datastoreBeanName);
//		MultipleStoresData.addDataSingle(MultipleStoresData.generateData(2, 5, 0), datastore.getTransactionManager());
//
//		jsonDatastoreService = (DatastoreService) jettyServer.getApplicationContext().getBean(jsonDatastoreServiceBeanName);
//	}
//
//	@AfterClass
//	public static void tearDownTestOffHeap() {
//		datastore = null;
//		if (clientPool != null) {
//			clientPool.close();
//			clientPool = null;
//		}
//		jsonDatastoreService = null;
//		if (jettyServer != null) {
//			jettyServer.stop();
//			jettyServer = null;
//		}
//	}

	// TODO(ope) restore tests with remote loading
//	@Test
//	public void testFillMonitoringDatastoreFromRemoteServer() throws Exception {
//		// MONITORING DATASTORE
//		final IDatastoreSchemaDescription desc = new MemoryAnalysisDatastoreDescription();
//		final IDatastore monitoringDatastore = new DatastoreBuilder()
//				.setSchemaDescription(desc)
//				.build();
//
//		final String data = service.path("memory_allocations").get().as(String.class);
//		final IMemoryStatistic stats = MonitoringStatisticSerializerUtil.deserialize(
//				new StringReader(data),
//				IMemoryStatistic.class);
//
//		final IMemoryStatistic datastoreStats = stats.getChildren().stream()
//				.filter(s -> MemoryStatisticConstants.STAT_NAME_DATASTORE.equals(s.getName()))
//				.findFirst()
//				.orElseThrow(() -> new AssertionError("No stats for the datastore in " + data));
//
//		monitoringDatastore.getTransactionManager().startTransaction();
//		datastoreStats.accept(new DatastoreFeederVisitor(monitoringDatastore, "test"));
//		monitoringDatastore.getTransactionManager().commitTransaction();
//
//		for (final String storeName : monitoringDatastore.getSchemaMetadata().getStoreNames()) {
//			final int cursorSize = DatastoreQueryHelper.getCursorSize(
//					monitoringDatastore.getHead().getQueryRunner()
//							.forStore(storeName)
//							.withoutCondition()
//							.selectingAllStoreFields()
//							.run());
//			Assertions.assertThat(cursorSize).isGreaterThanOrEqualTo(1); // check store is non empty
//		}
//	}

	/**
	 * Assert the number of offheap chunks by filling the datastore used
	 * for monitoring AND doing a query on it for counting. Comparing
	 * the value from counting from {@link IMemoryStatistic}.
	 *
	 * @throws Exception
	 */
	@Test
	public void testLoadMonitoringDatastore() throws Exception {
		IDatastore monitoredDatastore = resources.create(() -> new UnitTestDatastoreBuilder()
				.setSchemaDescription(MultipleStores.schemaDescription())
				.build());

		MultipleStoresData.addDataSingle(MultipleStoresData.generateData(2, 5, 0), monitoredDatastore.getTransactionManager());

		final IMemoryStatistic datastoreStats = monitoredDatastore.getMemoryStatistic();
		completeWithMemoryInfo(datastoreStats);
		assertDatastoreLoadsCorrectly(datastoreStats);
	}

	@Test
	public void testLoadMonitoringDatastoreWithVectorsWODuplicate() throws Exception {
		doTestLoadMonitoringDatastoreWithVectors(true);
	}

	@Test
	public void testLoadMonitoringDatastoreWithDuplicate() throws Exception {
		doTestLoadMonitoringDatastoreWithVectors(false);
	}

	@Test
	public void testPerStoreDatastoreLoad() throws DatastoreTransactionException {
		IDatastore monitoredDatastore = new UnitTestDatastoreBuilder()
				.setSchemaDescription(MultipleStores.schemaDescription())
				.build();

		MultipleStoresData.addDataSingle(MultipleStoresData.generateData(6, 36, 3), monitoredDatastore.getTransactionManager());

		IDatastoreSchemaDescription desc = new MemoryAnalysisDatastoreDescription();
		IDatastore monitoringDatastore = new DatastoreBuilder()
				.setSchemaDescription(desc)
				.build();

		final int storeCount = monitoredDatastore.getSchemaMetadata().getStoreCount();
		for (int i = 0; i < storeCount; i++) {
			final IMemoryStatistic memoryStatisticForStore = monitoredDatastore.getMemoryStatisticForStore(i);
			completeWithMemoryInfo(memoryStatisticForStore);
			memoryStatisticForStore.getAttributes().put(
					MemoryStatisticConstants.ATTR_NAME_DATE,
					new LongStatisticAttribute(Instant.now().getEpochSecond()));

			final int iteration = i;
			monitoringDatastore.edit(tm -> {
				memoryStatisticForStore.accept(new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "store_" + iteration));
			});
		}

		IMemoryStatistic datastoreStats = monitoredDatastore.getMemoryStatistic();
		final StatisticsSummary fullDatastoreSummary = MemoryStatisticsTestUtils.getStatisticsSummary(datastoreStats);

		assertDatastoreConsistentWithSummary(monitoringDatastore, fullDatastoreSummary);
	}

	public void doTestLoadMonitoringDatastoreWithVectors(boolean duplicateVectors) throws Exception {
		final IDatastore monitoredDatastore = resources.create(TestMemoryStatisticLoading::buildDatastoreWithVectors);
		commitDataInDatastoreWithVectors(monitoredDatastore, duplicateVectors);
		final IMemoryStatistic datastoreStats = monitoredDatastore.getMemoryStatistic();
		completeWithMemoryInfo(datastoreStats);

		assertDatastoreLoadsCorrectly(datastoreStats);
	}

	protected static final String VECTOR_STORE_NAME = "vectorStore";
	protected static IDatastore buildDatastoreWithVectors() {
		DatastoreSchemaDescription desc = MultipleStores.schemaDescription();
		List<IStoreDescription> storeDescCopy = new ArrayList<>(desc.getStoreDescriptions());
		storeDescCopy.add(new StoreDescriptionBuilder()
				                  .withStoreName(VECTOR_STORE_NAME)
				                  .withField("vectorId", ILiteralType.INT).asKeyField()
				                  .withVectorField("vectorInt1", ILiteralType.INT).withVectorBlockSize(35)
				                  .withVectorField("vectorInt2", ILiteralType.INT).withVectorBlockSize(20)
				                  .withVectorField("vectorLong", ILiteralType.LONG).withVectorBlockSize(30)
				                  .build());
		DatastoreSchemaDescription datastoreSchemaDescription = new DatastoreSchemaDescription(storeDescCopy, desc.getReferenceDescriptions());

		final IDatastore monitoredDatastore = new UnitTestDatastoreBuilder()
				.setSchemaDescription(datastoreSchemaDescription)
				.build();

		return monitoredDatastore;
	}

	protected static void commitDataInDatastoreWithVectors(
			final IDatastore monitoredDatastore,
			final boolean commitDuplicatedVectors) throws DatastoreTransactionException {
		final int nbOfVectors = 10;
		final int vectorSize = 10;

		// 3 vectors of same size with same values (but not copied one from another), v1, v3 of ints and v2 of long
		final int[] v1 = new int[vectorSize];
		final int[] v3 = new int[vectorSize];
		final long[] v2 = new long[vectorSize];
		for (int j = 0; j < vectorSize; j++) {
			v1[j] = j;
			v2[j] = j;
			v3[j] = j;
		}

		// add the same vectors over and over
		monitoredDatastore.edit(tm -> {
			for (int i = 0; i < nbOfVectors; i++) {
				tm.add(VECTOR_STORE_NAME, i, v1, v3, v2);
			}
		});

		// If commitDuplicatedVectors, take already registered vector and re-commit it in a different field
		if (commitDuplicatedVectors) {
			IDictionaryCursor cursor = monitoredDatastore.getHead().getQueryManager().forStore(VECTOR_STORE_NAME)
					.withoutCondition().selecting("vectorInt1").run();

			final Object vec = cursor.next()
					? cursor.getRecord().read("vectorInt1")
					: null;

			monitoredDatastore.edit(tm -> {
				tm.add(VECTOR_STORE_NAME, 0, v1, vec, v2);
			});
		}

		MultipleStoresData.addDataSingle(MultipleStoresData.generateData(2, 5, 0), monitoredDatastore.getTransactionManager());
	}

	/**
	 * Asserts the chunks number and off-heap memory as computed from the loaded datastore are consistent
	 * with the ones computed by visiting the statistic.
	 * @param datastoreStats
	 */
	protected void assertDatastoreLoadsCorrectly(IMemoryStatistic datastoreStats) {
		IDatastoreSchemaDescription desc = new MemoryAnalysisDatastoreDescription();
		IDatastore monitoringDatastore = new DatastoreBuilder()
				.setSchemaDescription(desc)
				.build();

		monitoringDatastore.edit(tm -> {
			datastoreStats.accept(new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "test"));
		});

		final StatisticsSummary statisticsSummary = MemoryStatisticsTestUtils.getStatisticsSummary(datastoreStats);

		assertDatastoreConsistentWithSummary(monitoringDatastore, statisticsSummary);
	}

	/**
	 * Asserts the monitoring datastore contains chunks consistent with what the statistics summary says.
	 * @param monitoringDatastore The monitoring datastore in which the statistics was loaded.
	 * @param statisticsSummary The statistics summary we want to compare the datastore with.
	 */
	protected void assertDatastoreConsistentWithSummary(IDatastore monitoringDatastore, StatisticsSummary statisticsSummary) {
		IDictionaryCursor cursor = monitoringDatastore.getHead().getQueryRunner()
				.forStore(CHUNK_STORE)
				.withoutCondition()
				.selecting(CHUNK__OFF_HEAP_SIZE, CHUNK_ID, CHUNK__CLASS)
				.onCurrentThread().run();

		// Count all chunks plus the total allocated amount of bytes
		final LongAdder sum = new LongAdder();
		final LongAdder countFromStore = new LongAdder();
		Map<String, Collection<Long>> chunkIdsByClass = new HashMap<>();
		while (cursor.hasNext()) {
			cursor.next();
			IRecordReader reader = cursor.getRecord();
			sum.add((long) reader.read(CHUNK__OFF_HEAP_SIZE));
			countFromStore.increment();
			final String chunkClass = (String) reader.read(CHUNK__CLASS);
			final long chunkId = (long) reader.read(CHUNK_ID);
			final Collection<Long> v = chunkIdsByClass.computeIfAbsent(chunkClass, __ -> new HashSet<>());
			if (!v.add(chunkId)) {
				System.out.println("Chunk ID " + chunkId + " already existed.");
				final IDictionaryCursor chunkIdCursor = monitoringDatastore.getHead().getQueryRunner()
						.forStore(CHUNK_STORE)
						.withCondition(BaseConditions.Equal(CHUNK_ID, chunkId))
						.selectingAllStoreFields()
						.onCurrentThread().run();
				while (chunkIdCursor.hasNext()) {
					chunkIdCursor.next();
					System.out.println(Arrays.toString(chunkIdCursor.getRecord().toTuple()));
				}
			}
		}

		SoftAssertions.assertSoftly(assertions -> {
			assertions.assertThat(sum.longValue())
					.as("off-heap memory computed on monitoring datastore")
					.isEqualTo(statisticsSummary.offHeapMemory);
			assertions.assertThat(countFromStore.longValue())
					.as("total number of chunks loaded in monitoring store")
					.isEqualTo(statisticsSummary.numberDistinctChunks);
			assertions.assertThat(chunkIdsByClass)
					.as("Classes of the loaded chunks")
					.containsAllEntriesOf(statisticsSummary.chunkIdsByClass);
		});
	}

	private static void completeWithMemoryInfo(final IMemoryStatistic statistic) {
		final Map<String, Long> fakeMemoryInfo = QfsArrays.mutableMap(
				MemoryStatisticConstants.STAT_NAME_GLOBAL_USED_HEAP_MEMORY, 10L,
				MemoryStatisticConstants.ST$AT_NAME_GLOBAL_MAX_HEAP_MEMORY, 20L,
				MemoryStatisticConstants.STAT_NAME_GLOBAL_USED_DIRECT_MEMORY, 30L,
				MemoryStatisticConstants.STAT_NAME_GLOBAL_MAX_DIRECT_MEMORY, 40L);
		fakeMemoryInfo.forEach((attribute, value) -> {
			statistic.getAttributes().put(
					attribute,
					new LongStatisticAttribute(value));
		});
	}

}
