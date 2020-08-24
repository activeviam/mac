package com.activeviam.mac.statistic.memory;

import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.statistic.memory.visitor.impl.FeedVisitor;
import com.activeviam.pivot.builders.StartBuilding;
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
import org.assertj.core.api.SoftAssertions;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestOverviewBookmark extends ATestMemoryStatistic {

	Pair<IDatastore, IActivePivotManager> monitoredApp;
	Pair<IDatastore, IActivePivotManager> monitoringApp;

	StatisticsSummary summary;

	private static final String OVERVIEW_QUERY =
			"SELECT [Measures].[DirectMemory.SUM] ON COLUMNS,"
					+ " Crossjoin("
					+ "   Hierarchize("
					+ "     DrilldownLevel("
					+ "       [Owners].[Owner].[ALL].[AllMember]"
					+ "     )"
					+ "   ),"
					+ "   Hierarchize("
					+ "     DrilldownLevel("
					+ "       [Components].[Component].[ALL].[AllMember]"
					+ "     )"
					+ "   )"
					+ " ) ON ROWS "
					+ "FROM [MemoryCube]";

	public static final int ADDED_DATA_SIZE = 20;

	@BeforeClass
	public static void setupRegistry() {
		Registry.setContributionProvider(new ClasspathContributionProvider());
	}

	@Before
	public void setup() throws AgentException {
		monitoredApp = createMicroApplication();

		// Add 100 records
		monitoredApp.getLeft()
				.edit(tm -> IntStream.range(0, ADDED_DATA_SIZE)
								.forEach(i -> tm.add("A", i * i)));

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
	public void testOverviewGrandTotal() throws QueryException {
		final IMultiVersionActivePivot pivot =
				monitoringApp.getRight().getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);

		final MDXQuery totalQuery = new MDXQuery(
				"SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS FROM ("
						+ OVERVIEW_QUERY
						+ ")");

		final CellSetDTO totalResult = pivot.execute(totalQuery);

		Assertions.assertThat(CellSetUtils.extractValueFromSingleCellDTO(totalResult))
				.isEqualTo(summary.offHeapMemory);
	}

	@Test
	public void testOverviewOwnerTotal() throws QueryException {
		final IMultiVersionActivePivot pivot =
				monitoringApp.getRight().getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);

		final MDXQuery totalQuery = new MDXQuery(
				"SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS FROM ("
						+ OVERVIEW_QUERY
						+ ")");

		final MDXQuery perOwnerQuery = new MDXQuery(
				"SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS, "
						+ "[Owners].[Owner].[ALL].[AllMember].Children ON ROWS "
						+ "FROM (" + OVERVIEW_QUERY + ")");

		final MDXQuery excessMemoryQuery = new MDXQuery(
				"WITH"
						+ " MEMBER [Measures].[ExcessDirectMemory] AS"
						+ " Sum("
						+ "   [Chunks].[ChunkId].[ALL].[AllMember].Children,"
						+ "   ([Measures].[Owner.COUNT] - 1) * [Measures].[DirectMemory.SUM]"
						+ " )"
						+ " SELECT [Measures].[ExcessDirectMemory] ON COLUMNS"
						+ " FROM [MemoryCube]");

		final CellSetDTO totalResult = pivot.execute(totalQuery);
		final CellSetDTO perOwnerResult = pivot.execute(perOwnerQuery);
		final CellSetDTO excessMemoryResult = pivot.execute(excessMemoryQuery);

		Assertions.assertThat(CellSetUtils.sumValuesFromCellSetDTO(perOwnerResult)
				- CellSetUtils.extractDoubleValueFromSingleCellDTO(excessMemoryResult).longValue())
				.isEqualTo(CellSetUtils
						.extractValueFromSingleCellDTO(totalResult));
	}

	// todo vlg: junit5 parameterized tests
	@Test
	public void testOverviewStoreTotal() throws QueryException {
		final IMultiVersionActivePivot pivot =
				monitoringApp.getRight().getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);

		final MDXQuery storeTotalQuery = new MDXQuery(
				"SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS "
						+ "FROM (" + OVERVIEW_QUERY + ") "
						+ "WHERE [Owners].[Owner].[ALL].[AllMember].[Store A]");

		final MDXQuery perComponentsStoreQuery = new MDXQuery(
				"SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS, "
						+ "[Components].[Component].[ALL].[AllMember].Children ON ROWS "
						+ "FROM (" + OVERVIEW_QUERY + ") "
						+ "WHERE [Owners].[Owner].[ALL].[AllMember].[Store A]");

		final MDXQuery excessMemoryQuery = new MDXQuery(
				"WITH"
						+ " MEMBER [Measures].[ExcessDirectMemory] AS"
						+ " Sum("
						+ "   [Chunks].[ChunkId].[ALL].[AllMember].Children,"
						+ "   ([Measures].[Component.COUNT] - 1) * [Measures].[DirectMemory.SUM]"
						+ " )"
						+ " SELECT [Measures].[ExcessDirectMemory] ON COLUMNS"
						+ " FROM [MemoryCube]"
						+ " WHERE [Owners].[Owner].[ALL].[AllMember].[Store A]");

		final CellSetDTO storeTotalResult = pivot.execute(storeTotalQuery);
		final CellSetDTO perComponentStoreResult = pivot.execute(perComponentsStoreQuery);
		final CellSetDTO excessMemoryResult = pivot.execute(excessMemoryQuery);

		Assertions.assertThat(CellSetUtils.sumValuesFromCellSetDTO(perComponentStoreResult)
				- CellSetUtils.extractDoubleValueFromSingleCellDTO(excessMemoryResult).longValue())
				.isEqualTo(CellSetUtils.extractValueFromSingleCellDTO(storeTotalResult));
	}

	@Test
	public void testOverviewCubeTotal() throws QueryException {
		final IMultiVersionActivePivot pivot =
				monitoringApp.getRight().getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);

		final MDXQuery cubeTotalQuery = new MDXQuery(
				"SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS "
						+ "FROM (" + OVERVIEW_QUERY + ") "
						+ "WHERE [Owners].[Owner].[ALL].[AllMember].[Cube Cube]");

		final MDXQuery perComponentCubeQuery = new MDXQuery(
				"SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS,"
						+ "[Components].[Component].[ALL].[AllMember].Children ON ROWS "
						+ "FROM (" + OVERVIEW_QUERY + ") "
						+ "WHERE [Owners].[Owner].[ALL].[AllMember].[Cube Cube]");

		final MDXQuery excessMemoryQuery = new MDXQuery(
				"WITH"
						+ " MEMBER [Measures].[ExcessDirectMemory] AS"
						+ " Sum("
						+ "   [Chunks].[ChunkId].[ALL].[AllMember].Children,"
						+ "   ([Measures].[Component.COUNT] - 1) * [Measures].[DirectMemory.SUM]"
						+ " )"
						+ " SELECT [Measures].[ExcessDirectMemory] ON COLUMNS"
						+ " FROM [MemoryCube]"
						+ " WHERE [Owners].[Owner].[ALL].[AllMember].[Cube Cube]");

		final CellSetDTO cubeTotalResult = pivot.execute(cubeTotalQuery);
		final CellSetDTO perComponentCubeResult = pivot.execute(perComponentCubeQuery);
		final CellSetDTO excessMemoryResult = pivot.execute(excessMemoryQuery);

		Assertions.assertThat(CellSetUtils.sumValuesFromCellSetDTO(perComponentCubeResult)
				- CellSetUtils.extractDoubleValueFromSingleCellDTO(excessMemoryResult).longValue())
				.isEqualTo(CellSetUtils.extractValueFromSingleCellDTO(cubeTotalResult));
	}
}