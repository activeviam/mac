/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.memory.AnalysisDatastoreFeeder;
import com.activeviam.pivot.builders.StartBuilding;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.pivot.monitoring.impl.MemoryAnalysisService;
import com.qfs.store.IDatastore;
import com.qfs.store.record.impl.Records.IDictionaryProvider;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.IMultiVersionActivePivot;
import com.quartetfs.biz.pivot.dto.AxisDTO;
import com.quartetfs.biz.pivot.dto.AxisPositionDTO;
import com.quartetfs.biz.pivot.dto.CellDTO;
import com.quartetfs.biz.pivot.dto.CellSetDTO;
import com.quartetfs.biz.pivot.query.impl.MDXQuery;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.contributions.impl.ClasspathContributionProvider;
import com.quartetfs.fwk.impl.Pair;
import com.quartetfs.fwk.query.QueryException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestIndexAndDictionaryBookmarks extends ATestMemoryStatistic {

	private Pair<IDatastore, IActivePivotManager> monitoredApp;
	private Pair<IDatastore, IActivePivotManager> monitoringApp;

	public static final int ADDED_DATA_SIZE = 20;

	@BeforeClass
	public static void setupRegistry() {
		Registry.setContributionProvider(new ClasspathContributionProvider());
	}

	@Before
	public void setup() throws AgentException {
		initializeApplication();

		final Path exportPath = generateMemoryStatistics();

		final IMemoryStatistic stats = loadMemoryStatFromFolder(exportPath);

		initializeMonitoringApplication(stats);

		IMultiVersionActivePivot pivot =
				monitoringApp.getRight().getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);
		Assertions.assertThat(pivot).isNotNull();
	}

	private void initializeApplication() {
		monitoredApp = createMicroApplicationWithIndexedFields();

		monitoredApp.getLeft()
				.edit(tm -> IntStream.range(0, ADDED_DATA_SIZE)
						.forEach(i -> tm.add("A", i, i % 11, i % 7, i % 5)));
	}

	private Path generateMemoryStatistics() {
		monitoredApp.getLeft().getEpochManager().forceDiscardEpochs(__ -> true);

		performGC();

		final MemoryAnalysisService analysisService =
				(MemoryAnalysisService) createService(monitoredApp.getLeft(), monitoredApp.getRight());
		return analysisService.exportMostRecentVersion("testOverview");
	}

	private void initializeMonitoringApplication(final IMemoryStatistic data) throws AgentException {
		ManagerDescriptionConfig config = new ManagerDescriptionConfig();
		final IDatastore monitoringDatastore =
				StartBuilding.datastore().setSchemaDescription(config.schemaDescription()).build();

		IActivePivotManager manager =
				StartBuilding.manager()
						.setDescription(config.managerDescription())
						.setDatastoreAndPermissions(monitoringDatastore)
						.buildAndStart();
		monitoringApp = new Pair<>(monitoringDatastore, manager);

		final AnalysisDatastoreFeeder feeder =
				new AnalysisDatastoreFeeder(data, "storeA");
		monitoringDatastore.edit(feeder::feedDatastore);
	}

	@After
	public void tearDown() throws AgentException {
		monitoringApp.getLeft().close();
		monitoringApp.getRight().stop();
	}

	@Test
	public void testIndexedFieldsForStoreA() throws QueryException {
		final IMultiVersionActivePivot pivot =
				monitoringApp.getRight().getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);

		final MDXQuery totalQuery =
				new MDXQuery(
						"SELECT NON EMPTY [Indices].[Indexed Fields].[Indexed Fields].Members ON COLUMNS"
								+ " FROM [MemoryCube]"
								+ " WHERE [Owners].[Owner].[ALL].[AllMember].[Store A]");

		final CellSetDTO result = pivot.execute(totalQuery);

		final List<AxisDTO> axes = result.getAxes();
		Assertions.assertThat(axes)
				.hasSize(1);

		final List<AxisPositionDTO> indexedFieldsPosition = axes.get(0).getPositions();

		Assertions.assertThat(indexedFieldsPosition)
				.extracting(position -> position.getMembers().get(0).getPath().getPath())
				.containsExactlyInAnyOrder(
						new String[] {"AllMember", "id0, id1, id2"},
						new String[] {"AllMember", "id0"});
	}

	@Test
	public void testDictionarizedFieldsForStoreB() throws QueryException {
		final IMultiVersionActivePivot pivot =
				monitoringApp.getRight().getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);

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

		final List<AxisDTO> axes = result.getAxes();
		Assertions.assertThat(axes)
				.hasSize(1);

		final List<AxisPositionDTO> dictionarizedFieldsPositions = axes.get(0).getPositions();

		Assertions.assertThat(dictionarizedFieldsPositions)
				.extracting(position -> position.getMembers().get(0).getPath().getPath())
				.containsExactlyInAnyOrder(
						new String[] {"AllMember", "id0"});
	}

	@Test
	public void testDictionarySizeTotal() throws QueryException {
		final IMultiVersionActivePivot pivot =
				monitoringApp.getRight().getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);

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
		final IMultiVersionActivePivot pivot =
				monitoringApp.getRight().getActivePivots().get(ManagerDescriptionConfig.MONITORING_CUBE);

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

		final IDictionaryProvider dictionaryProvider =
				monitoredApp.getLeft().getDictionaries().getStoreDictionaries("A");

		SoftAssertions.assertSoftly(assertions -> {
			final List<AxisPositionDTO> positions = result.getAxes().get(0).getPositions();
			for (final CellDTO cell : result.getCells()) {
				final String fieldName =
						positions.get(cell.getOrdinal()).getMembers().get(0).getPath().getPath()[1];

				final long expectedDictionarySize = dictionaryProvider
						.getDictionary(monitoredApp.getLeft().getSchemaMetadata().getFieldIndex("A", fieldName))
						.size();

				assertions.assertThat((long) cell.getValue())
						.isEqualTo(expectedDictionarySize);
			}
		});
	}
}
