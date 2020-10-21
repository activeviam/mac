/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.activeviam.copper.testing.CubeTester;
import com.activeviam.mac.statistic.memory.descriptions.MicroApplicationDescriptionWithIndexedFields;
import com.activeviam.mac.statistic.memory.descriptions.MonitoringApplicationDescription;
import com.activeviam.mac.statistic.memory.junit.RegistrySetupExtension;
import com.activeviam.properties.impl.ActiveViamProperty;
import com.activeviam.properties.impl.ActiveViamPropertyExtension;
import com.activeviam.properties.impl.ActiveViamPropertyExtension.ActiveViamPropertyExtensionBuilder;
import com.qfs.junit.LocalResourcesExtension;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils;
import com.qfs.monitoring.offheap.MemoryStatisticsTestUtils.StatisticsSummary;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.store.record.impl.Records.IDictionaryProvider;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.qfs.util.impl.QfsFileTestUtils;
import com.quartetfs.biz.pivot.IMultiVersionActivePivot;
import com.quartetfs.biz.pivot.dto.AxisPositionDTO;
import com.quartetfs.biz.pivot.dto.CellDTO;
import com.quartetfs.biz.pivot.dto.CellSetDTO;
import com.quartetfs.biz.pivot.query.impl.MDXQuery;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.query.QueryException;
import java.nio.file.Path;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(RegistrySetupExtension.class)
public class TestIndexAndDictionaryBookmarks {

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
	public void setup() throws AgentException, DatastoreTransactionException {
		monitoredApplication = MonitoringTestUtils.setupApplication(
				new MicroApplicationDescriptionWithIndexedFields(),
				resources);

		final Path exportPath = MonitoringTestUtils.exportMostRecentVersion(
				monitoredApplication.getDatastore(),
				monitoredApplication.getManager(),
				tempDir,
				this.getClass().getSimpleName());

		final IMemoryStatistic stats = MonitoringTestUtils.loadMemoryStatFromFolder(exportPath);
		statisticsSummary = MemoryStatisticsTestUtils.getStatisticsSummary(stats);

		monitoringApplication = MonitoringTestUtils
				.setupApplication(new MonitoringApplicationDescription(stats), resources);

		tester = MonitoringTestUtils.createMonitoringCubeTester(monitoringApplication.getManager());
	}

	@Test
	public void testIndexedFieldsForStoreA() throws QueryException {
		final IMultiVersionActivePivot pivot = tester.pivot();

		final MDXQuery totalQuery =
				new MDXQuery(
						"SELECT NON EMPTY [Indices].[Indexed Fields].[Indexed Fields].Members ON COLUMNS"
								+ " FROM [MemoryCube]"
								+ " WHERE [Owners].[Owner].[ALL].[AllMember].[Store A]");

		final CellSetDTO result = pivot.execute(totalQuery);

		final List<AxisPositionDTO> indexedFieldsPosition = result.getAxes().get(0).getPositions();

		Assertions.assertThat(indexedFieldsPosition)
				.extracting(position -> position.getMembers().get(0).getPath().getPath())
				.containsExactlyInAnyOrder(
						new String[] {"AllMember", "id0, id1, id2"},
						new String[] {"AllMember", "id0"});
	}

	@Test
	public void testDictionarizedFieldsForStoreB() throws QueryException {
		final IMultiVersionActivePivot pivot = tester.pivot();

		final MDXQuery totalQuery =
				new MDXQuery(
						"SELECT NonEmpty("
								+ "   Except("
								+ "     [Fields].[Field].[Field].Members,"
								+ "     [Fields].[Field].[ALL].[AllMember].[N/A]"
								+ "   ),"
								+ "   [Measures].[Dictionary Size]"
								+ " ) ON COLUMNS"
								+ " FROM [MemoryCube]"
								+ " WHERE ("
								+ "   [Owners].[Owner].[ALL].[AllMember].[Store B]"
								+ " )");

		final CellSetDTO result = pivot.execute(totalQuery);

		final List<AxisPositionDTO> dictionarizedFieldsPositions =
				result.getAxes().get(0).getPositions();

		Assertions.assertThat(dictionarizedFieldsPositions)
				.extracting(position -> position.getMembers().get(0).getPath().getPath())
				.containsExactlyInAnyOrder(
						new String[] {"AllMember", "id0"});
	}

	@Test
	public void testDictionarySizeTotal() throws QueryException {
		final IMultiVersionActivePivot pivot = tester.pivot();

		final MDXQuery totalQuery = new MDXQuery(
				"SELECT NON EMPTY [Fields].[Field].[ALL].[AllMember] ON COLUMNS,"
						+ " [Measures].[Dictionary Size] ON ROWS"
						+ " FROM [MemoryCube]"
						+ " WHERE [Owners].[Owner].[ALL].[AllMember].[Store A]");

		final MDXQuery perFieldQuery = new MDXQuery(
				"SELECT NonEmpty("
						+ "   Except("
						+ "     [Fields].[Field].[Field].Members,"
						+ "     [Fields].[Field].[ALL].[AllMember].[N/A]"
						+ "   ),"
						+ "   [Measures].[Dictionary Size]"
						+ " ) ON COLUMNS,"
						+ " [Measures].[Dictionary Size] ON ROWS"
						+ " FROM [MemoryCube]"
						+ " WHERE [Owners].[Owner].[ALL].[AllMember].[Store A]");

		final CellSetDTO total = pivot.execute(totalQuery);
		final CellSetDTO perField = pivot.execute(perFieldQuery);

		Assertions.assertThat(perField.getCells().stream()
				.mapToLong(x -> (long) x.getValue())
				.sum())
				.isEqualTo((long) total.getCells().get(0).getValue());
	}

	@Test
	public void testDictionarySizesPerField() throws QueryException {
		final IMultiVersionActivePivot pivot = tester.pivot();

		final MDXQuery totalQuery =
				new MDXQuery(
						"SELECT NON EMPTY"
								+ "   Except("
								+ "     [Fields].[Field].[Field].Members,"
								+ "     [Fields].[Field].[ALL].[AllMember].[N/A]"
								+ "   ) ON COLUMNS,"
								+ " [Measures].[Dictionary Size] ON ROWS"
								+ " FROM [MemoryCube]"
								+ " WHERE ("
								+ "   [Owners].[Owner].[ALL].[AllMember].[Store A],"
								+ "   [Components].[Component].[ALL].[AllMember].[DICTIONARY]"
								+ " )");

		final CellSetDTO result = pivot.execute(totalQuery);

		final IDictionaryProvider dictionaryProvider = monitoredApplication.getDatastore()
				.getDictionaries().getStoreDictionaries("A");

		SoftAssertions.assertSoftly(assertions -> {
			final List<AxisPositionDTO> positions = result.getAxes().get(0).getPositions();
			for (final CellDTO cell : result.getCells()) {
				final String fieldName =
						positions.get(cell.getOrdinal()).getMembers().get(0).getPath().getPath()[1];

				final long expectedDictionarySize = dictionaryProvider
						.getDictionary(monitoredApplication.getDatastore()
								.getSchemaMetadata().getFieldIndex("A", fieldName))
						.size();

				assertions.assertThat((long) cell.getValue())
						.isEqualTo(expectedDictionarySize);
			}
		});
	}
}
