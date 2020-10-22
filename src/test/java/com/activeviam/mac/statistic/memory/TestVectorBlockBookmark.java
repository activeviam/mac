/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.activeviam.copper.testing.CubeTester;
import com.activeviam.mac.statistic.memory.descriptions.MicroApplicationDescriptionWithSharedVectorField;
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
public class TestVectorBlockBookmark extends ATestMemoryStatistic {

	@RegisterExtension
	protected static ActiveViamPropertyExtension propertyExtension =
			new ActiveViamPropertyExtensionBuilder()
					.withProperty(ActiveViamProperty.ACTIVEVIAM_TEST_PROPERTY, true)
					.build();

	@RegisterExtension
	protected final LocalResourcesExtension resources = new LocalResourcesExtension();

	protected static Path tempDir = QfsFileTestUtils.createTempDirectory(TestMACMeasures.class);

	protected Application monitoredApplication;
	protected Application monitoringApplication;
	protected StatisticsSummary statisticsSummary;
	protected CubeTester tester;

	@BeforeEach
	public void setup() throws AgentException {
		monitoredApplication = MonitoringTestUtils.setupApplication(
				new MicroApplicationDescriptionWithSharedVectorField(),
				resources,
				MicroApplicationDescriptionWithSharedVectorField::fillWithGenericData);

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
	public void testVectorBlockRecordConsumptionIsZero() throws QueryException {
		tester.mdxQuery("SELECT [Components].[Component].[Component].[RECORDS] ON ROWS,"
				+ " [Measures].[DirectMemory.SUM] ON COLUMNS"
				+ " FROM [MemoryCube]"
				+ " WHERE ("
				+ "   [Owners].[Owner].[Owner].[Store A],"
				+ "   [Fields].[Field].[Field].[vector1]"
				+ " )").getTester()
				.hasOnlyOneCell()
				.containing(0L);
	}

	@Test
	public void testVectorBlockConsumption() throws QueryException {
		final IMultiVersionActivePivot pivot = tester.pivot();

		final MDXQuery vectorBlockQuery = new MDXQuery(
				"SELECT {"
						+ "   [Components].[Component].[ALL].[AllMember],"
						+ "   [Components].[Component].[Component].[VECTOR_BLOCK]"
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
	public void testVectorBlockLength() {
		tester.mdxQuery("SELECT  [Measures].[VectorBlock.Length] ON COLUMNS"
				+ " FROM [MemoryCube]"
				+ " WHERE ("
				+ "   [Owners].[Owner].[Owner].[Store A],"
				+ "   [Fields].[Field].[Field].[vector1]"
				+ " )")
				.getTester()
				.hasOnlyOneCell()
				.containing((long) MicroApplicationDescriptionWithSharedVectorField.VECTOR_BLOCK_SIZE);
	}

	@Test
	public void testVectorBlockRefCount() {
		tester.mdxQuery("SELECT [Measures].[VectorBlock.RefCount] ON COLUMNS"
				+ " FROM [MemoryCube]"
				+ " WHERE ("
				+ "   [Owners].[Owner].[Owner].[Store A],"
				+ "   [Fields].[Field].[Field].[vector1]"
				+ " )")
				.getTester()
				.hasOnlyOneCell()
				.containing((long) MicroApplicationDescriptionWithSharedVectorField.FIELD_SHARING_COUNT
						* MicroApplicationDescriptionWithSharedVectorField.ADDED_DATA_SIZE);
	}
}
