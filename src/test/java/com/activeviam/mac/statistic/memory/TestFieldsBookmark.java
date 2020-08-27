package com.activeviam.mac.statistic.memory;

import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.statistic.memory.visitor.impl.FeedVisitor;
import com.activeviam.pivot.builders.StartBuilding;
import com.google.common.collect.Table.Cell;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils.StatisticsSummary;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.pivot.monitoring.impl.MemoryAnalysisService;
import com.qfs.store.IDatastore;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.IMultiVersionActivePivot;
import com.quartetfs.biz.pivot.dto.CellSetDTO;
import com.quartetfs.biz.pivot.query.impl.MDXQuery;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.contributions.impl.ClasspathContributionProvider;
import com.quartetfs.fwk.impl.Pair;
import com.quartetfs.fwk.query.QueryException;
import java.nio.file.Path;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFieldsBookmark extends ATestMemoryStatistic {

	Pair<IDatastore, IActivePivotManager> monitoredApp;
	Pair<IDatastore, IActivePivotManager> monitoringApp;

	StatisticsSummary summary;

	public static final int ADDED_DATA_SIZE = 20;

	@BeforeClass
	public static void setupRegistry() {
		Registry.setContributionProvider(new ClasspathContributionProvider());
	}

	@Before
	public void setup() throws AgentException {
		monitoredApp = createMicroApplicationWithSharedVectorField();

		monitoredApp.getLeft()
				.edit(tm -> IntStream.range(0, ADDED_DATA_SIZE)
						.forEach(i -> tm.add("A", i * i, new double[] {i}, new double[] {-i, -i * i})));

		// Force to discard all versions
		monitoredApp.getLeft().getEpochManager().forceDiscardEpochs(__ -> true);

		// perform GCs before exporting the store data
		performGC();
		final MemoryAnalysisService analysisService =
				(MemoryAnalysisService) createService(monitoredApp.getLeft(), monitoredApp.getRight());
		final Path exportPath = analysisService.exportMostRecentVersion("testOverview");

		final IMemoryStatistic stats = loadMemoryStatFromFolder(exportPath);
		summary = MemoryStatisticsTestUtils.getStatisticsSummary(stats);

		// Start a monitoring datastore with the exported data
		ManagerDescriptionConfig config = new ManagerDescriptionConfig();
		final IDatastore monitoringDatastore = StartBuilding.datastore()
				.setSchemaDescription(config.schemaDescription())
				.build();

		// Start a monitoring cube
		IActivePivotManager manager = StartBuilding.manager()
				.setDescription(config.managerDescription())
				.setDatastoreAndPermissions(monitoringDatastore)
				.buildAndStart();
		monitoringApp = new Pair<>(monitoringDatastore, manager);

		// Fill the monitoring datastore
		monitoringDatastore.edit(
				tm -> stats.accept(new FeedVisitor(monitoringDatastore.getSchemaMetadata(), tm, "storeA")));

		IMultiVersionActivePivot pivot =
				monitoringApp.getRight().getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);
		Assertions.assertThat(pivot).isNotNull();
	}

	@After
	public void tearDown() throws AgentException {
		monitoringApp.getLeft().close();
		monitoringApp.getRight().stop();
	}

	@Test
	public void testStoreTotal() throws QueryException {
		final IMultiVersionActivePivot pivot =
				monitoringApp.getRight().getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);

		final MDXQuery storeTotal = new MDXQuery(
				"SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS "
						+ "FROM [MemoryCube]"
						+ "WHERE [Owners].[Owner].[ALL].[AllMember].[Store A]");

		final MDXQuery perFieldQuery = new MDXQuery(
				"SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS, "
						+ "NON EMPTY [Fields].[Field].[ALL].[AllMember].Children ON ROWS "
						+ "FROM [MemoryCube]"
						+ "WHERE [Owners].[Owner].[ALL].[AllMember].[Store A]");

		final MDXQuery excessMemoryQuery = new MDXQuery(
				"WITH MEMBER [Measures].[ExcessDirectMemory] AS"
						+ " Sum("
						+ "   [Chunks].[ChunkId].[ALL].[AllMember].Children,"
						// this considers chunks with no owning field with a negative contribution for the sum
						+ "   ([Measures].[Field.COUNT] - 1) * [Measures].[DirectMemory.SUM]"
						+ " )"
						+ " SELECT [Measures].[ExcessDirectMemory] ON COLUMNS"
						+ " FROM [MemoryCube]"
						+ " WHERE [Owners].[Owner].[ALL].[AllMember].[Store A]");

		final CellSetDTO totalResult = pivot.execute(storeTotal);
		final CellSetDTO fieldResult = pivot.execute(perFieldQuery);
		final CellSetDTO excessResult = pivot.execute(excessMemoryQuery);

		Assertions.assertThat(CellSetUtils.extractValueFromSingleCellDTO(totalResult))
				.isEqualTo(CellSetUtils.sumValuesFromCellSetDTO(fieldResult)
						- CellSetUtils.extractDoubleValueFromSingleCellDTO(excessResult).longValue());
	}

	@Test
	public void testVectorBlockRecordConsumptionIsZero() throws QueryException {
		final IMultiVersionActivePivot pivot =
				monitoringApp.getRight().getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);

		final MDXQuery recordQuery = new MDXQuery(
				"SELECT [Chunks].[Type].[Type].[RECORDS] ON ROWS,"
						+ " [Measures].[DirectMemory.SUM] ON COLUMNS"
						+ " FROM [MemoryCube]"
						+ " WHERE ("
						+ "   [Owners].[Owner].[Owner].[Store A],"
						+ "   [Fields].[Field].[Field].[vector1]"
						+ " )");

		final CellSetDTO result = pivot.execute(recordQuery);

		Assertions.assertThat((long) result.getCells().get(0).getValue())
				.isZero();
	}

	@Test
	public void testVectorBlockConsumption() throws QueryException {
		final IMultiVersionActivePivot pivot =
				monitoringApp.getRight().getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);

		final MDXQuery vectorBlockQuery = new MDXQuery(
				"SELECT {"
						+ "   [Chunks].[Type].[ALL].[AllMember],"
						+ "   [Chunks].[Type].[Type].[VECTOR_BLOCK]"
						+ " } ON ROWS,"
						+ " [Measures].[DirectMemory.SUM] ON COLUMNS"
						+ " FROM [MemoryCube]"
						+ " WHERE ("
						+ "   [Owners].[Owner].[Owner].[Store A],"
						+ "   [Fields].[Field].[Field].[vector1]"
						+ " )");

		final CellSetDTO result = pivot.execute(vectorBlockQuery);

		Assertions.assertThat((long) result.getCells().get(1).getValue())
				.isEqualTo((long) result.getCells().get(0).getValue());
	}

	@Test
	public void testVectorBlockLength() throws QueryException {
		final IMultiVersionActivePivot pivot =
				monitoringApp.getRight().getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);

		final MDXQuery lengthQuery = new MDXQuery(
				"SELECT  [Measures].[VectorBlock.Length] ON COLUMNS"
						+ " FROM [MemoryCube]"
						+ " WHERE ("
						+ "   [Owners].[Owner].[Owner].[Store A],"
						+ "   [Fields].[Field].[Field].[vector1]"
						+ " )");

		final CellSetDTO result = pivot.execute(lengthQuery);

		Assertions.assertThat(CellSetUtils.extractValueFromSingleCellDTO(result))
				.isEqualTo((long) MICROAPP_VECTOR_BLOCK_SIZE);
	}

	@Test
	public void testVectorBlockRefCount() throws QueryException {
		final IMultiVersionActivePivot pivot =
				monitoringApp.getRight().getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);

		final MDXQuery refCountQuery = new MDXQuery(
				"SELECT [Measures].[VectorBlock.RefCount] ON COLUMNS"
						+ " FROM [MemoryCube]"
						+ " WHERE ("
						+ "   [Owners].[Owner].[Owner].[Store A],"
						+ "   [Fields].[Field].[Field].[vector1]"
						+ " )");

		final MDXQuery sharedCountQuery = new MDXQuery(
				"SELECT [Measures].[Field.COUNT] ON COLUMNS"
						+ " FROM [MemoryCube]"
						+ " WHERE ("
						+ "   [Owners].[Owner].[Owner].[Store A],"
						+ "   [Fields].[Field].[Field].[vector1]"
						+ " )");

		final CellSetDTO refCountResult = pivot.execute(refCountQuery);
		final CellSetDTO sharedCountResult = pivot.execute(sharedCountQuery);

		Assertions.assertThat((long) CellSetUtils.extractValueFromSingleCellDTO(refCountResult))
				.isEqualTo(CellSetUtils.extractValueFromSingleCellDTO(sharedCountResult)
						* ADDED_DATA_SIZE);
	}
}