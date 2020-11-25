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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(RegistrySetupExtension.class)
public class TestFieldsBookmark {

	@RegisterExtension
	protected static ActiveViamPropertyExtension propertyExtension =
			new ActiveViamPropertyExtensionBuilder()
					.withProperty(ActiveViamProperty.ACTIVEVIAM_TEST_PROPERTY, true)
					.build();

	@RegisterExtension
	protected final LocalResourcesExtension resources = new LocalResourcesExtension();

	protected static Path tempDir = QfsFileTestUtils.createTempDirectory(TestFieldsBookmark.class);

	protected Application monitoredApplication;
	protected Application monitoringApplication;

	protected StatisticsSummary statisticsSummary;
	protected CubeTester tester;

	@BeforeEach
	public void setup() throws AgentException {
		monitoredApplication = MonitoringTestUtils
				.setupApplication(new MicroApplicationDescription(), resources,
						MicroApplicationDescription::fillWithGenericData);

		final Path exportPath =
				MonitoringTestUtils.exportMostRecentVersion(monitoredApplication.getDatastore(),
						monitoredApplication.getManager(),
						tempDir,
						this.getClass().getSimpleName());

		final IMemoryStatistic stats = MonitoringTestUtils.loadMemoryStatFromFolder(exportPath);
		statisticsSummary = MemoryStatisticsTestUtils.getStatisticsSummary(stats);

		monitoringApplication = MonitoringTestUtils.setupMonitoringApplication(stats, resources);

		tester = MonitoringTestUtils.createMonitoringCubeTester(monitoringApplication.getManager());
	}

	@Disabled("wrong field counts, should be updated after Field.COUNT is removed")
	@Test
	public void testStoreTotal() throws QueryException {
		final IMultiVersionActivePivot pivot = tester.pivot();

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
				"WITH MEMBER [Measures].[Field.COUNT] AS "
						+ ownershipCountMdxExpression("[Fields].[Field]")
						+ " MEMBER [Measures].[ExcessDirectMemory] AS"
						+ " Sum("
						+ "   [Chunks].[ChunkId].[ALL].[AllMember].Children,"
						+ "   IIF([Measures].[Field.COUNT] > 1,"
						+ "     ([Measures].[Field.COUNT] - 1) * [Measures].[DirectMemory.SUM],"
						+ "     0)"
						+ " )"
						+ " SELECT [Measures].[ExcessDirectMemory] ON COLUMNS"
						+ " FROM [MemoryCube]"
						+ " WHERE [Owners].[Owner].[ALL].[AllMember].[Store A]");

		final CellSetDTO totalResult = pivot.execute(storeTotal);
		final CellSetDTO fieldResult = pivot.execute(perFieldQuery);
		final CellSetDTO excessResult = pivot.execute(excessMemoryQuery);

		Assertions.assertThat(CellSetUtils.<Long>extractValueFromSingleCellDTO(totalResult))
				.isEqualTo(CellSetUtils.sumValuesFromCellSetDTO(fieldResult, 0L, Long::sum)
						- CellSetUtils.<Long>extractValueFromSingleCellDTO(excessResult));
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
