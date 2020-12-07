/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.activeviam.mac.TestMemoryStatisticBuilder;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription.ParentType;
import com.activeviam.mac.statistic.memory.descriptions.FullApplicationDescription;
import com.activeviam.mac.statistic.memory.descriptions.FullApplicationDescriptionWithVectors;
import com.activeviam.mac.statistic.memory.junit.RegistrySetupExtension;
import com.activeviam.properties.impl.ActiveViamProperty;
import com.activeviam.properties.impl.ActiveViamPropertyExtension;
import com.activeviam.properties.impl.ActiveViamPropertyExtension.ActiveViamPropertyExtensionBuilder;
import com.qfs.condition.impl.BaseConditions;
import com.qfs.junit.LocalResourcesExtension;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.store.IDatastore;
import com.qfs.store.query.ICursor;
import com.qfs.store.transaction.DatastoreTransactionException;
import com.qfs.util.impl.QfsFileTestUtils;
import com.quartetfs.fwk.AgentException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(RegistrySetupExtension.class)
public class TestMemoryStatisticLoadingWithReverseOrder {

	@RegisterExtension
	protected static ActiveViamPropertyExtension propertyExtension =
			new ActiveViamPropertyExtensionBuilder()
					.withProperty(ActiveViamProperty.ACTIVEVIAM_TEST_PROPERTY, true)
					.build();

	@RegisterExtension
	protected final LocalResourcesExtension resources = new LocalResourcesExtension();

	protected static Path tempDir =
			QfsFileTestUtils.createTempDirectory(TestMemoryStatisticLoadingWithReverseOrder.class);

	@Test
	public void testLoadDatastoreStats() throws AgentException, DatastoreTransactionException {
		final Application monitoredApplication = MonitoringTestUtils
				.setupApplication(new FullApplicationDescription(), resources,
						FullApplicationDescription::fillWithGenericData);

		final Path exportPath = MonitoringTestUtils.exportMostRecentVersion(
				monitoredApplication.getDatastore(),
				monitoredApplication.getManager(), tempDir, "testLoadDatastoreStatsWithReverseOrder");

		final List<? extends IMemoryStatistic> storeStats = new ArrayList<>(
				MonitoringTestUtils.loadMemoryStatFromFolder(exportPath).getChildren());

		Collections.reverse(storeStats);

		Assertions.assertThat(storeStats).isNotEmpty();
		MonitoringTestUtils.assertLoadsCorrectly(new TestMemoryStatisticBuilder()
						.withCreatorClasses(TestMemoryStatisticLoadingWithReverseOrder.class)
						.withChildren(storeStats)
						.build(),
				resources);
	}

	@Test
	public void testLoadDatastoreStatsWithVectors()
			throws AgentException, DatastoreTransactionException {
		final Application monitoredApplication = MonitoringTestUtils
				.setupApplication(new FullApplicationDescriptionWithVectors(),
						resources,
						FullApplicationDescriptionWithVectors::fillWithGenericData);

		final Path exportPath = MonitoringTestUtils.exportMostRecentVersion(
				monitoredApplication.getDatastore(),
				monitoredApplication.getManager(), tempDir, "testLoadDatastoreStatsWithReverseOrder");

		final List<? extends IMemoryStatistic> storeStats = new ArrayList<>(
				MonitoringTestUtils.loadMemoryStatFromFolder(exportPath).getChildren());

		Collections.reverse(storeStats);

		Assertions.assertThat(storeStats).isNotEmpty();
		final IDatastore monitoringDatastore = MonitoringTestUtils.assertLoadsCorrectly(
				new TestMemoryStatisticBuilder()
						.withCreatorClasses(TestMemoryStatisticLoadingWithReverseOrder.class)
						.withChildren(storeStats)
						.build(),
				resources);

		final Set<Long> vectorBlocks = extractVectorBlocks(monitoringDatastore);
		assertVectorBlockAttributesArePresent(monitoringDatastore, vectorBlocks);
	}

	protected void assertVectorBlockAttributesArePresent(
			final IDatastore monitoringDatastore, final Collection<Long> chunkIdSubset) {
		final ICursor cursor = monitoringDatastore.getHead().getQueryRunner()
				.forStore(DatastoreConstants.CHUNK_STORE)
				.withCondition(BaseConditions.And(
						BaseConditions.In(DatastoreConstants.CHUNK_ID, chunkIdSubset.toArray()),
						BaseConditions.Equal(DatastoreConstants.CHUNK__VECTOR_BLOCK_LENGTH, null)))
				.selecting(DatastoreConstants.CHUNK_ID)
				.onCurrentThread()
				.run();

		Assertions.assertThat(StreamSupport.stream(cursor.spliterator(), false).count())
				.isZero();
	}

	protected Set<Long> extractVectorBlocks(final IDatastore monitoringDatastore) {
		final ICursor cursor = monitoringDatastore.getHead().getQueryRunner()
				.forStore(DatastoreConstants.CHUNK_STORE)
				.withCondition(
						BaseConditions.Equal(DatastoreConstants.OWNER__COMPONENT, ParentType.VECTOR_BLOCK))
				.selecting(DatastoreConstants.CHUNK_ID)
				.onCurrentThread()
				.run();

		return StreamSupport.stream(cursor.spliterator(), false)
				.map(reader -> reader.readLong(0))
				.collect(Collectors.toSet());
	}
}
