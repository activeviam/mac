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

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import com.activeviam.builders.FactFilterConditions;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription;
import com.activeviam.mac.statistic.memory.visitor.impl.FeedVisitor;
import com.activeviam.pivot.builders.StartBuilding;
import com.qfs.condition.impl.BaseConditions;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.desc.IStoreDescription;
import com.qfs.desc.impl.DatastoreSchemaDescription;
import com.qfs.desc.impl.StoreDescriptionBuilder;
import com.qfs.junit.ResourceRule;
import com.qfs.literal.ILiteralType;
import com.qfs.monitoring.memory.impl.OnHeapPivotMemoryQuantifierPlugin;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils.StatisticsSummary;
import com.qfs.monitoring.statistic.impl.LongStatisticAttribute;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;
import com.qfs.monitoring.statistic.memory.impl.MemoryStatisticBuilder;
import com.qfs.pivot.monitoring.impl.MemoryMonitoringService;
import com.qfs.service.monitoring.IMemoryMonitoringService;
import com.qfs.store.IDatastore;
import com.qfs.store.build.impl.UnitTestDatastoreBuilder;
import com.qfs.store.query.IDictionaryCursor;
import com.qfs.store.record.IRecordReader;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.qfs.util.impl.QfsArrays;
import com.qfs.util.impl.ThrowingLambda;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.IMultiVersionActivePivot;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.definitions.impl.ActivePivotDatastorePostProcessor;
import com.quartetfs.biz.pivot.test.util.PivotTestUtils;
import com.quartetfs.fwk.AgentException;
import org.assertj.core.api.SoftAssertions;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import test.scenario.MultipleStores;
import test.scenario.MultipleStoresData;

public class TestMemoryStatisticLoading {

//	@ClassRule
//	public static final EnvTestRule envRule = EnvTestRule.getInstance();
	@ClassRule
	public static final ResourceRule resources = new ResourceRule();

	@BeforeClass
	public static void setUpRegistry() {
		PivotTestUtils.setUpRegistry(OnHeapPivotMemoryQuantifierPlugin.class);
	}

	/**
	 * Assert the number of offheap chunks by filling the datastore used
	 * for monitoring AND doing a query on it for counting. Comparing
	 * the value from counting from {@link IMemoryStatistic}.
	 */
	@Test
	public void testLoadDatastoreStats() {
		createApplication((monitoredDatastore, monitoredManager) -> {
			fillApplication(monitoredDatastore);

			final IMemoryStatistic datastoreStats = monitoredDatastore.getMemoryStatistic();
			completeWithMemoryInfo(datastoreStats);
			assertLoadsCorrectly(datastoreStats);
		});
	}

	@Test
	public void testLoadPivotStats() {
		createApplication((monitoredDatastore, monitoredManager) -> {
			fillApplication(monitoredDatastore);

			final IMemoryStatistic pivotStats = monitoredManager.getMemoryStatistic();
			completeWithMemoryInfo(pivotStats);

			assertLoadsCorrectly(pivotStats);
		});
	}

	@Test
	public void testLoadFullStats() {
		createApplication((monitoredDatastore, monitoredManager) -> {
			fillApplication(monitoredDatastore);

			final IMemoryStatistic stats = new MemoryStatisticBuilder()
					.withName("application")
					.withCreatorClasses(getClass())
					.withChildren(
							monitoredDatastore.getMemoryStatistic(),
							monitoredManager.getMemoryStatistic())
					.build();
			completeWithMemoryInfo(stats);
			assertLoadsCorrectly(stats);
		});
	}

	@Test
	public void testPerStoreDatastoreLoad() {
		createApplication((monitoredDatastore, monitoredManager) -> {
			fillApplication(monitoredDatastore);
			final IDatastore monitoringDatastore = createAnalysisDatastore();

			final int storeCount = monitoredDatastore.getSchemaMetadata().getStoreCount();
			for (int i = 0; i < storeCount; i++) {
				final IMemoryStatistic memoryStatisticForStore = monitoredDatastore.getMemoryStatisticForStore(i);
				completeWithMemoryInfo(memoryStatisticForStore);
				memoryStatisticForStore.getAttributes().put(
						MemoryStatisticConstants.ATTR_NAME_DATE,
						new LongStatisticAttribute(Instant.now().getEpochSecond()));

				final int iteration = i;
				monitoringDatastore.edit(tm -> {
					final FeedVisitor feeder = new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "store_" + iteration);
					memoryStatisticForStore.accept(feeder);
				});
			}

			IMemoryStatistic datastoreStats = monitoredDatastore.getMemoryStatistic();
			final StatisticsSummary fullDatastoreSummary = MemoryStatisticsTestUtils.getStatisticsSummary(datastoreStats);

			assertDatastoreConsistentWithSummary(monitoringDatastore, fullDatastoreSummary);
		});
	}

	@Test
	public void testLoadPivotPerPivot() {
		createApplication((monitoredDatastore, monitoredManager) -> {
			fillApplication(monitoredDatastore);
			final IDatastore monitoringDatastore = createAnalysisDatastore();

			int i = 0;
			for (final IMultiVersionActivePivot pivot: monitoredManager.getActivePivots().values()) {
				final IMemoryStatistic pivotStat = pivot.getMemoryStatistic();
				completeWithMemoryInfo(pivotStat);

				pivotStat.getAttributes().put(
						MemoryStatisticConstants.ATTR_NAME_DATE,
						new LongStatisticAttribute(Instant.now().getEpochSecond()));

				final int iteration = ++i;
				monitoringDatastore.edit(tm -> {
					final FeedVisitor feeder = new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "pivot_" + iteration);
					pivotStat.accept(feeder);
				});
			}

			IMemoryStatistic datastoreStats = monitoredDatastore.getMemoryStatistic();
			final StatisticsSummary fullDatastoreSummary = MemoryStatisticsTestUtils.getStatisticsSummary(datastoreStats);

			assertDatastoreConsistentWithSummary(monitoringDatastore, fullDatastoreSummary);
		});
	}

	@Test
	public void testLoadMonitoringDatastoreWithVectorsWODuplicate() throws Exception {
		doTestLoadMonitoringDatastoreWithVectors(true);
	}

	@Test
	public void testLoadMonitoringDatastoreWithDuplicate() throws Exception {
		doTestLoadMonitoringDatastoreWithVectors(false);
	}

	private static void createApplication(
			final ThrowingLambda.ThrowingBiConsumer<IDatastore, IActivePivotManager> actions) {
		final IDatastoreSchemaDescription datastoreSchema = StartBuilding.datastoreSchema()
				.withStore(
						StartBuilding.store().withStoreName("Sales")
								.withField("id", ILiteralType.INT).asKeyField()
								.withField("seller")
								.withField("buyer")
								.withField("date", ILiteralType.LOCAL_DATE)
								.withField("productId", ILiteralType.LONG)
								.withModuloPartitioning(4, "id")
								.build())
				.withStore(
						StartBuilding.store().withStoreName("People")
								.withField("id").asKeyField()
								.withField("firstName")
								.withField("lastName")
								.withField("company")
								.build())
				.withStore(
						StartBuilding.store().withStoreName("Products")
								.withField("id", ILiteralType.LONG).asKeyField()
								.withField("name")
								.build())
				.withReference(
						StartBuilding.reference()
								.fromStore("Sales").toStore("People")
								.withName("Sales->Buyer")
								.withMapping("buyer", "id")
								.build())
				.withReference(
						StartBuilding.reference()
								.fromStore("Sales").toStore("People")
								.withName("Sales->Seller")
								.withMapping("seller", "id")
								.build())
				.withReference(
						StartBuilding.reference()
								.fromStore("Sales").toStore("Products")
								.withName("Sales->Products")
								.withMapping("productId", "id")
								.build())
				.build();
		final IActivePivotManagerDescription managerDescription = StartBuilding.managerDescription()
				.withSchema().withSelection(
						StartBuilding.selection(datastoreSchema).fromBaseStore("Sales")
								.withAllFields()
								.usingReference("Sales->Products")
								.withField("product", "name")
								.usingReference("Sales->Buyer")
								.withField("buyer_firstName", "firstName")
								.withField("buyer_lastName", "lastName")
								.withField("buyer_company", "company")
								.usingReference("Sales->Seller")
								.withField("seller_firstName", "firstName")
								.withField("seller_lastName", "lastName")
								.withField("seller_company", "company")
								.build())
				.withCube(
						StartBuilding.cube("HistoryCube")
								.withContributorsCount()
								.withSingleLevelDimension("Product").withPropertyName("product")
								.withSingleLevelDimension("Date").withPropertyName("date")
								.withDimension("Operations")
								.withHierarchy("Sellers")
								.withLevel("Company").withPropertyName("seller_company")
								.withLevel("LastName").withPropertyName("seller_lastName")
								.withLevel("FirstName").withPropertyName("seller_firstName")
								.withHierarchy("Buyers")
								.withLevel("Company").withPropertyName("buyer_company")
								.withLevel("LastName").withPropertyName("buyer_lastName")
								.withLevel("FirstName").withPropertyName("buyer_firstName")
								.withFilter(
										FactFilterConditions.not(
												FactFilterConditions.eq("date", LocalDate.now())))
								.build())
				.withCube(
						StartBuilding.cube("DailyCube")
								.withContributorsCount()
								.withSingleLevelDimension("Product").withPropertyName("product")
								.withDimension("Operations")
								.withHierarchy("Sellers")
								.withLevel("Company").withPropertyName("seller_company")
								.withLevel("LastName").withPropertyName("seller_lastName")
								.withLevel("FirstName").withPropertyName("seller_firstName")
								.withHierarchy("Buyers")
								.withLevel("Company").withPropertyName("buyer_company")
								.withLevel("LastName").withPropertyName("buyer_lastName")
								.withLevel("FirstName").withPropertyName("buyer_firstName")
								.withFilter(FactFilterConditions.eq("date", LocalDate.now()))
								.build())
				.withCube(
						StartBuilding.cube("OverviewCube")
								.withContributorsCount()
								.withSingleLevelDimension("Product").withPropertyName("product")
								.withSingleLevelDimension("Date").withPropertyName("date")
								.withDimension("Operations")
								.withHierarchy("Sales")
								.withLevel("Seller").withPropertyName("seller_company")
								.withLevel("Buyer").withPropertyName("buyer_company")
								.withHierarchy("Purchases")
								.withLevel("Buyer").withPropertyName("buyer_company")
								.withLevel("Seller").withPropertyName("seller_company")
								.build())
				.build();

		final IDatastore datastore = resources.create(() -> StartBuilding.datastore()
				.setSchemaDescription(datastoreSchema)
				.addSchemaDescriptionPostProcessors(ActivePivotDatastorePostProcessor.createFrom(managerDescription))
				.build());
		final IActivePivotManager manager;
		try {
			manager = StartBuilding.manager()
					.setDescription(managerDescription)
					.setDatastoreAndDescription(datastore, datastoreSchema)
					.buildAndStart();
		} catch (AgentException e) {
			throw new RuntimeException("Cannot create manager", e);
		}
		resources.register(manager::stop);

		actions.accept(datastore, manager);
	}

	/**
	 * Fills the datastore created by {@link #createApplication()}.
	 * @param datastore datastore to fill
	 */
	private static void fillApplication(final IDatastore datastore) {
		datastore.edit(tm -> {
			final int peopleCount = 10;
			IntStream.range(0, peopleCount).forEach(i -> {
				tm.add("People", String.valueOf(i), "FN" + (i % 4), "LN" + i, "Corp" + (i + 1 % 3));
			});
			final int productCount = 20;
			LongStream.range(0, productCount).forEach(i -> {
				tm.add("Products", i, "p" + i);
			});

			final Random r = new Random(47605);
			IntStream.range(0, 1000).forEach(i -> {
				final int seller = r.nextInt(peopleCount);
				int buyer;
				do { buyer = r.nextInt(peopleCount); } while (buyer == seller);
				tm.add(
						"Sales",
				       i,
						String.valueOf(seller),
						String.valueOf(buyer),
						LocalDate.now().plusDays(-r.nextInt(7)),
						(long) r.nextInt(productCount));
			});
		});
	}

	// FIXME(ope) it would be good to be able to actually use the service to export the stats
	private static IMemoryMonitoringService createService(final IDatastore datastore, final IActivePivotManager manager) {
		return new MemoryMonitoringService(
				datastore,
				manager,
				datastore.getEpochManager());
	}

	private static IDatastore createAnalysisDatastore() {
		final IDatastoreSchemaDescription desc = new MemoryAnalysisDatastoreDescription();
		return resources.create(() -> {
			return StartBuilding.datastore()
					.setSchemaDescription(desc)
					.build();
		});
	}

	public void doTestLoadMonitoringDatastoreWithVectors(boolean duplicateVectors) throws Exception {
		final IDatastore monitoredDatastore = resources.create(TestMemoryStatisticLoading::buildDatastoreWithVectors);
		commitDataInDatastoreWithVectors(monitoredDatastore, duplicateVectors);
		final IMemoryStatistic datastoreStats = monitoredDatastore.getMemoryStatistic();
		completeWithMemoryInfo(datastoreStats);

		assertLoadsCorrectly(datastoreStats);
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
	 * @param statistics
	 */
	protected void assertLoadsCorrectly(IMemoryStatistic statistics) {
		final IDatastore monitoringDatastore = createAnalysisDatastore();

		monitoringDatastore.edit(tm -> {
			statistics.accept(new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "test"));
		});

		final StatisticsSummary statisticsSummary = MemoryStatisticsTestUtils.getStatisticsSummary(statistics);

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
