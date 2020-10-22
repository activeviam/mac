package com.activeviam.mac.statistic.memory;

import com.activeviam.copper.testing.CubeTester;
import com.activeviam.mac.statistic.memory.descriptions.MicroApplicationDescription;
import com.activeviam.mac.statistic.memory.junit.RegistrySetupExtension;
import com.activeviam.properties.impl.ActiveViamProperty;
import com.activeviam.properties.impl.ActiveViamPropertyExtension;
import com.activeviam.properties.impl.ActiveViamPropertyExtension.ActiveViamPropertyExtensionBuilder;
import com.qfs.junit.LocalResourcesExtension;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils.StatisticsSummary;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.util.impl.QfsFileTestUtils;
import com.quartetfs.biz.pivot.IMultiVersionActivePivot;
import com.quartetfs.biz.pivot.dto.CellSetDTO;
import com.quartetfs.biz.pivot.query.impl.MDXQuery;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.query.QueryException;
import java.nio.file.Path;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(RegistrySetupExtension.class)
public class TestOverviewBookmark {

	@RegisterExtension
	protected static ActiveViamPropertyExtension propertyExtension =
			new ActiveViamPropertyExtensionBuilder()
					.withProperty(ActiveViamProperty.ACTIVEVIAM_TEST_PROPERTY, true)
					.build();

	@RegisterExtension
	protected final LocalResourcesExtension resources = new LocalResourcesExtension();

	protected static Path tempDir = QfsFileTestUtils.createTempDirectory(TestOverviewBookmark.class);

	protected Application monitoredApplication;
	protected Application monitoringApplication;
	protected StatisticsSummary statisticsSummary;
	protected CubeTester tester;

	@BeforeEach
	public void setup() throws AgentException {
		monitoredApplication = MonitoringTestUtils.setupApplication(
				new MicroApplicationDescription(),
				resources,
				MicroApplicationDescription::fillWithGenericData);

		final Path exportPath = MonitoringTestUtils.exportMostRecentVersion(
				monitoredApplication.getDatastore(),
				monitoredApplication.getManager(),
				tempDir,
				this.getClass().getSimpleName());

		final IMemoryStatistic stats = MonitoringTestUtils.loadMemoryStatFromFolder(exportPath);
		statisticsSummary = MemoryStatisticsTestUtils.getStatisticsSummary(stats);

		monitoringApplication = MonitoringTestUtils.setupMonitoringApplication(stats, resources);

		tester = MonitoringTestUtils.createMonitoringCubeTester(monitoringApplication.getManager());
	}

	@Test
	public void testOverviewGrandTotal() {
		tester.mdxQuery("SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS"
				+ " FROM [MemoryCube]")
				.getTester()
				.hasOnlyOneCell()
				.containing(statisticsSummary.offHeapMemory);
	}

  @Test
  public void testOwnerTotal() throws QueryException {
		final IMultiVersionActivePivot pivot = tester.pivot();

		final MDXQuery totalQuery =
				new MDXQuery("SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS "
						+ "FROM [MemoryCube]");

		final MDXQuery perOwnerQuery =
				new MDXQuery("SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS, "
						+ "NON EMPTY [Owners].[Owner].[ALL].[AllMember].Children ON ROWS "
						+ "FROM [MemoryCube]");

		final MDXQuery excessMemoryQuery =
				new MDXQuery("WITH MEMBER [Measures].[Owner.COUNT] AS "
						+ ownershipCountMdxExpression("[Owners].[Owner]")
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

		Assertions.assertThat(CellSetUtils.sumValuesFromCellSetDTO(perOwnerResult, 0L, Long::sum)
				- CellSetUtils.<Double>extractValueFromSingleCellDTO(excessMemoryResult).longValue())
				.isEqualTo(CellSetUtils.extractValueFromSingleCellDTO(totalResult));
	}

  @Test
  public void testStoreTotal() throws QueryException {
		final IMultiVersionActivePivot pivot = tester.pivot();

		final MDXQuery storeTotalQuery =
				new MDXQuery("SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS "
						+ "FROM [MemoryCube] "
						+ "WHERE [Owners].[Owner].[ALL].[AllMember].[Store A]");

		final MDXQuery perComponentsStoreQuery =
				new MDXQuery("SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS, "
						+ "[Components].[Component].[ALL].[AllMember].Children ON ROWS "
						+ "FROM [MemoryCube] "
						+ "WHERE [Owners].[Owner].[ALL].[AllMember].[Store A]");

		final MDXQuery excessMemoryQuery =
				new MDXQuery("WITH MEMBER [Measures].[Component.COUNT] AS "
						+ ownershipCountMdxExpression("[Components].[Component]")
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

		Assertions.assertThat(
				CellSetUtils.sumValuesFromCellSetDTO(perComponentStoreResult, 0L, Long::sum)
						- CellSetUtils.<Double>extractValueFromSingleCellDTO(excessMemoryResult).longValue())
				.isEqualTo(CellSetUtils.extractValueFromSingleCellDTO(storeTotalResult));
	}

  @Test
  public void testCubeTotal() throws QueryException {
		final IMultiVersionActivePivot pivot = tester.pivot();

		final MDXQuery cubeTotalQuery =
				new MDXQuery("SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS "
						+ "FROM [MemoryCube] "
						+ "WHERE [Owners].[Owner].[ALL].[AllMember].[Cube Cube]");

		final MDXQuery perComponentCubeQuery =
				new MDXQuery("SELECT NON EMPTY [Measures].[DirectMemory.SUM] ON COLUMNS,"
						+ "[Components].[Component].[ALL].[AllMember].Children ON ROWS "
						+ "FROM [MemoryCube] "
						+ "WHERE [Owners].[Owner].[ALL].[AllMember].[Cube Cube]");

		final MDXQuery excessMemoryQuery =
				new MDXQuery("WITH MEMBER [Measures].[Component.COUNT] AS "
						+ ownershipCountMdxExpression("[Components].[Component]")
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

		Assertions.assertThat(
				CellSetUtils.sumValuesFromCellSetDTO(perComponentCubeResult, 0L, Long::sum)
						- CellSetUtils.<Double>extractValueFromSingleCellDTO(excessMemoryResult).longValue())
				.isEqualTo(CellSetUtils.extractValueFromSingleCellDTO(cubeTotalResult));
	}

  protected String ownershipCountMdxExpression(final String hierarchyUniqueName) {
    return "DistinctCount("
        + "  Generate("
        + "    NonEmpty("
        + "      [Chunks].[ChunkId].[ALL].[AllMember].Children,"
        + "      {[Measures].[contributors.COUNT]}"
        + "    ),"
        + "    NonEmpty("
        + "      " + hierarchyUniqueName + ".[ALL].[AllMember].Children,"
        + "      {[Measures].[contributors.COUNT]}"
        + "    )"
        + "  )"
        + ")";
  }
}
